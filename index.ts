/**
 * AgentBus extension for omo (senpi) — the WAKE.
 *
 * Port of the Claude Code plugin (agentbus@rodmena) and the opencode plugin
 * (@rodmena/agentbus-opencode) onto the senpi extension API. Protocol, guard
 * rules and catch-up wording stay in the shared client (`agentbus`,
 * `agentbus-hook`); only the harness-specific part lives here: holding the
 * agent's SSE stream and turning each arrival into a turn.
 *
 *   session_start       resolve identity, catch up (`agentbus-hook session-start`),
 *                       open GET /v1/stream and keep it open for the session
 *   arrival             pi.session.admitExternalMessage -> starts a turn when idle,
 *                       queues as a follow-up when busy, dedupes by delivery id
 *   before_agent_start  per-prompt catch-up (`agentbus-hook pending`) as context
 *   tool_call           the AgentBus tool-guard (`agentbus-hook pre-tool-use`)
 *   session_shutdown    close the stream, release the watcher lock, `session-end`
 *
 * Opt-in exactly like the other harnesses: no declared identity
 * ($AGENTBUS_AGENT or `.agentbus/agent`), no AgentBus — nothing runs, nothing
 * is printed, no tool is gated.
 */
import type { ExtensionAPI } from "@code-yeongyu/senpi";
import { spawn } from "node:child_process";
import { appendFile, mkdir, readFile, unlink, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";

const VERSION = "0.1.0";
const HOOK_BIN = process.env.AGENTBUS_HOOK_BIN || "agentbus-hook";
const BASE_URL = (process.env.AGENTBUS_BASE_URL || "https://agentbus.rodmena.co.uk").replace(/\/+$/, "");
const CONFIG_DIR = process.env.AGENTBUS_CONFIG_DIR || join(homedir(), ".config", "agentbus");
const LOG_PATH = process.env.AGENTBUS_OMO_LOG || join(CONFIG_DIR, "omo-plugin.log");

// Server keepalive is 20s; three missed beats means the link is dead.
const STREAM_KEEPALIVE_MS = 20_000;
const STREAM_IDLE_DEADLINE_MS = 3 * STREAM_KEEPALIVE_MS;
// Keepalives keep the idle deadline from ever firing on a healthy server, so a
// cheap inbox check catches a push the stream missed.
const STREAM_RECONCILE_MS = 60_000;
// Bulkhead: at most this many wakes per rolling window.
const MAX_INJECTIONS = 12;
const WINDOW_MS = 60_000;
const CONTEXT_LIMIT = 1200;

// Process-level: one stream per identity per omo process, however many
// sessions the process hosts (a second one would double every wake).
const streamsInProcess = new Set<string>();

async function log(line: string): Promise<void> {
	try {
		await mkdir(dirname(LOG_PATH), { recursive: true });
		await appendFile(LOG_PATH, `${new Date().toISOString()} pid=${process.pid} ${line}\n`);
	} catch {
		/* diagnostics only; never break a session for it */
	}
}

type RunResult = { code: number; stdout: string; stderr: string };

function run(args: string[], env: Record<string, string>, cwd: string, stdin?: string, timeoutMs = 10_000): Promise<RunResult> {
	return new Promise((resolve) => {
		let stdout = "";
		let stderr = "";
		let child: ReturnType<typeof spawn>;
		try {
			child = spawn(HOOK_BIN, args, { cwd, env: { ...process.env, ...env }, stdio: ["pipe", "pipe", "pipe"] });
		} catch (err) {
			resolve({ code: 127, stdout: "", stderr: String(err) });
			return;
		}
		const timer = setTimeout(() => child.kill("SIGTERM"), timeoutMs);
		child.stdout?.on("data", (d) => (stdout += d));
		child.stderr?.on("data", (d) => (stderr += d));
		child.on("error", (err) => {
			clearTimeout(timer);
			resolve({ code: 127, stdout, stderr: stderr + String(err) });
		});
		child.on("close", (code) => {
			clearTimeout(timer);
			resolve({ code: code ?? 1, stdout, stderr });
		});
		child.stdin?.end(stdin ?? "");
	});
}

/** The identity is DECLARED, never inferred: $AGENTBUS_AGENT, else the nearest `.agentbus/agent`. */
async function resolveAgent(cwd: string): Promise<string> {
	const fromEnv = (process.env.AGENTBUS_AGENT || "").trim();
	if (fromEnv) return fromEnv;
	let dir = cwd;
	for (;;) {
		try {
			const name = (await readFile(join(dir, ".agentbus", "agent"), "utf8")).replace(/\s+/g, "");
			if (name) return name;
		} catch {
			/* not here; keep walking up */
		}
		const parent = dirname(dir);
		if (parent === dir) return "";
		dir = parent;
	}
}

/** The agent's own bound key file wins over an inherited env key (same order as the Claude hooks). */
async function resolveKey(agent: string): Promise<string> {
	try {
		const text = await readFile(join(CONFIG_DIR, "keys", `${agent}.env`), "utf8");
		const m = text.match(/^\s*(?:export\s+)?AGENTBUS_API_KEY\s*=\s*["']?([^"'\s]+)/m);
		if (m) return m[1];
	} catch {
		/* no key file for this agent */
	}
	return process.env.AGENTBUS_API_KEY || "";
}

function pidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === "EPERM";
	}
}

async function acquireLock(lockPath: string): Promise<number | undefined> {
	try {
		const holder = Number.parseInt((await readFile(lockPath, "utf8")).trim(), 10);
		if (holder && holder !== process.pid && pidAlive(holder)) return holder;
	} catch {
		/* no lock yet */
	}
	await mkdir(dirname(lockPath), { recursive: true });
	await writeFile(lockPath, `${process.pid}\n`, { mode: 0o600 });
	return undefined;
}

function withIdleDeadline<T>(p: Promise<T>, onExpire: () => void): Promise<T> {
	return new Promise<T>((resolve, reject) => {
		const timer = setTimeout(() => {
			onExpire();
			reject(new Error(`no data for ${STREAM_IDLE_DEADLINE_MS / 1000}s though the server keepalives every ${STREAM_KEEPALIVE_MS / 1000}s; treating the link as dead`));
		}, STREAM_IDLE_DEADLINE_MS);
		p.then(
			(v) => {
				clearTimeout(timer);
				resolve(v);
			},
			(e) => {
				clearTimeout(timer);
				reject(e);
			},
		);
	});
}

function clip(text: string): string {
	return text.length > CONTEXT_LIMIT ? `${text.slice(0, CONTEXT_LIMIT)}\n... truncated; run \`agentbus inbox --unread\` for the rest.` : text;
}

export default function agentbus(pi: ExtensionAPI) {
	// Subagent/worker sessions are not the agent; only the session a human opened listens.
	if (pi.sessionKind === "worker") return;

	let agent = "";
	let key = "";
	let cwd = pi.cwd;
	let state: "off" | "passive" | "connecting" | "live" | "down" = "off";
	let stream: AbortController | undefined;
	let lockPath = "";
	let ownsLock = false;
	let catchUp = "";
	const announced = new Set<string>();
	let injections: number[] = [];
	let mutedUntil = 0;
	const retryTimers = new Set<ReturnType<typeof setTimeout>>();

	const hookEnv = (): Record<string, string> => ({ AGENTBUS_AGENT: agent, AGENTBUS_API_KEY: key });

	function budgetAllows(now: number): boolean {
		if (now < mutedUntil) return false;
		injections = injections.filter((t) => now - t < WINDOW_MS);
		if (injections.length >= MAX_INJECTIONS) {
			mutedUntil = now + WINDOW_MS;
			return false;
		}
		injections.push(now);
		return true;
	}

	function admit(deliveryId: string, text: string, attempt = 0): void {
		const session = (pi as unknown as { session?: { admitExternalMessage?: (m: object) => { kind: string } } }).session;
		if (!session?.admitExternalMessage) {
			pi.sendUserMessage(text, { deliverAs: "followUp" });
			void log(`wake via sendUserMessage: ${deliveryId}`);
			return;
		}
		const { kind } = session.admitExternalMessage({ delivery_id: `agentbus:${deliveryId}`, text, deliverAs: "followUp" });
		void log(`wake ${deliveryId}: ${kind}`);
		// held_draft: the human is typing. Nothing was enqueued; try again shortly.
		if (kind === "held_draft" && attempt < 120) {
			const t = setTimeout(() => {
				retryTimers.delete(t);
				admit(deliveryId, text, attempt + 1);
			}, 5_000);
			retryTimers.add(t);
		}
	}

	function deliver(deliveryId: string, from: string, subject: string): void {
		if (announced.has(deliveryId)) return;
		announced.add(deliveryId);
		if (!budgetAllows(Date.now())) {
			void log(`SUPPRESSED (wake budget): ${deliveryId} ${subject}`);
			return;
		}
		admit(
			deliveryId,
			`AgentBus: new message${from ? ` from ${from}` : ""} — "${subject}". ` +
				`Read it in full with \`agentbus show ${deliveryId}\`, print the complete body, then act on it. ` +
				`Reply in-thread with \`agentbus reply ${deliveryId} -b '...'\`.`,
		);
	}

	async function inboxHasMailAfter(cursor: string): Promise<boolean> {
		try {
			const r = await fetch(`${BASE_URL}/v1/inbox?cursor=${encodeURIComponent(cursor || "0")}&limit=1`, {
				headers: { Authorization: `Bearer ${key}`, "X-AgentBus-Agent": agent },
			});
			if (!r.ok) return false;
			const body = (await r.json()) as { messages?: unknown[] };
			return Array.isArray(body.messages) && body.messages.length > 0;
		} catch {
			return false;
		}
	}

	async function streamLoop(ac: AbortController): Promise<void> {
		let lastId = "";
		let backoff = 1_000;
		while (!ac.signal.aborted) {
			// The first connection replays the backlog and then sends `: connected`;
			// only what follows is live. A resume sends Last-Event-ID, so all of it is live.
			let live = Boolean(lastId);
			try {
				const headers: Record<string, string> = { Authorization: `Bearer ${key}`, "X-AgentBus-Agent": agent, Accept: "text/event-stream" };
				if (lastId) headers["Last-Event-ID"] = lastId;
				state = "connecting";
				const res = await fetch(`${BASE_URL}/v1/stream`, { headers, signal: ac.signal });
				if (res.status === 401 || res.status === 403) {
					state = "down";
					await log(`stream REJECTED (HTTP ${res.status}): the key for ${agent} is revoked or invalid; not retrying`);
					return;
				}
				if (!res.ok || !res.body) throw new Error(`HTTP ${res.status}`);
				await log(`stream connected as ${agent}${lastId ? ` (resume ${lastId})` : " (live only)"}`);
				if (live) state = "live";
				backoff = 1_000;
				const reader = res.body.getReader();
				const decoder = new TextDecoder();
				let buf = "";
				let lastReconcile = Date.now();
				for (;;) {
					const { done, value } = await withIdleDeadline(reader.read(), () => void reader.cancel().catch(() => {}));
					if (done) break;
					if (Date.now() - lastReconcile >= STREAM_RECONCILE_MS) {
						lastReconcile = Date.now();
						if (await inboxHasMailAfter(lastId)) {
							await log("reconcile found mail the stream never pushed; reconnecting to replay it");
							void reader.cancel().catch(() => {});
							break;
						}
					}
					buf += decoder.decode(value, { stream: true });
					let cut: number;
					while ((cut = buf.indexOf("\n\n")) !== -1) {
						const frame = buf.slice(0, cut);
						buf = buf.slice(cut + 2);
						if (frame.startsWith(":")) {
							if (frame.includes("connected") && !live) {
								live = true;
								state = "live";
								await log("backlog ended, now live");
							}
							continue;
						}
						let id = "";
						let ev = "";
						let data = "";
						for (const line of frame.split("\n")) {
							if (line.startsWith("id:")) id = line.slice(3).trim();
							else if (line.startsWith("event:")) ev = line.slice(6).trim();
							else if (line.startsWith("data:")) data += line.slice(5).trim();
						}
						if (id) lastId = id;
						if (ev === "unauthorized") {
							state = "down";
							await log("stream UNAUTHORIZED: credential revoked; wake path is down");
							return;
						}
						// Only deliveries wake; acks, presence and lifecycle events do not.
						if (!live || !data || ev !== "message.delivered") continue;
						let m: { subject?: string; sender_display?: string; sender_address?: string; delivery_id?: string };
						try {
							m = JSON.parse(data);
						} catch {
							continue;
						}
						const from = m.sender_display || m.sender_address || "";
						const subject = m.subject || "(no subject)";
						await log(`arrival: ${from || "(sender not in event)"} — ${subject}`);
						deliver(m.delivery_id || `${from}:${subject}:${id}`, from, subject);
					}
				}
				await log("stream ended; reconnecting");
			} catch (err) {
				if (ac.signal.aborted) return;
				await log(`stream error: ${String(err).slice(0, 200)}`);
			}
			state = "connecting";
			await new Promise((r) => setTimeout(r, backoff));
			backoff = Math.min(backoff * 2, 30_000);
		}
	}

	async function stop(): Promise<void> {
		stream?.abort();
		stream = undefined;
		for (const t of retryTimers) clearTimeout(t);
		retryTimers.clear();
		if (agent) streamsInProcess.delete(agent);
		if (ownsLock) {
			ownsLock = false;
			try {
				const holder = Number.parseInt((await readFile(lockPath, "utf8")).trim(), 10);
				if (holder === process.pid) await unlink(lockPath);
			} catch {
				/* already gone */
			}
		}
	}

	pi.on("session_start", async (_event, ctx) => {
		cwd = ctx.cwd;
		agent = await resolveAgent(cwd);
		if (!agent) return;
		key = await resolveKey(agent);
		if (!key) {
			state = "down";
			await log(`no credential for ${agent}: no ${join(CONFIG_DIR, "keys", `${agent}.env`)} and no AGENTBUS_API_KEY`);
			return;
		}
		await log(`loaded v${VERSION} as ${agent} in ${cwd}`);

		// Catch-up: surfaced as context on the next turn, never as a turn of its own.
		const start = await run(["session-start"], hookEnv(), cwd);
		catchUp = start.stdout.trim();

		lockPath = join(CONFIG_DIR, `omo-watch-${agent}.lock`);
		const holder = streamsInProcess.has(agent) ? process.pid : await acquireLock(lockPath);
		if (holder !== undefined) {
			state = "passive";
			await log(`another omo session (pid ${holder}) already watches ${agent}; this one is PASSIVE`);
			if (ctx.hasUI) ctx.ui.notify(`AgentBus: ${agent} is already watched by another omo session; this one will not be woken.`, "warning");
			return;
		}
		ownsLock = true;
		streamsInProcess.add(agent);
		stream = new AbortController();
		void streamLoop(stream);
		if (ctx.hasUI) ctx.ui.setStatus("agentbus", `bus: ${agent}`);
	});

	pi.on("before_agent_start", async (event) => {
		if (!agent || !key) return;
		let text = catchUp;
		catchUp = "";
		// Per-prompt catch-up only for prompts a person typed; a wake already names its message.
		if ((event as { trigger?: string }).trigger === "prompt") {
			const pending = (await run(["pending"], hookEnv(), cwd, "{}")).stdout.trim();
			text = [text, pending].filter(Boolean).join("\n\n");
		}
		if (!text) return;
		return { message: { customType: "agentbus", content: clip(text), display: true } };
	});

	pi.on("tool_call", async (event) => {
		if (!agent || !key) return;
		const payload = JSON.stringify({ tool_name: event.toolName, tool_input: event.input });
		const res = await run(["pre-tool-use"], hookEnv(), cwd, payload, 60_000);
		let decision = "";
		let reason = "";
		try {
			const parsed = JSON.parse(res.stdout) as { hookSpecificOutput?: { permissionDecision?: string; permissionDecisionReason?: string } };
			decision = parsed.hookSpecificOutput?.permissionDecision ?? "";
			reason = parsed.hookSpecificOutput?.permissionDecisionReason ?? "";
		} catch {
			// Unparseable is undecidable and denies, unless the client exited 0 ("nothing to enforce").
			if (res.code !== 0) {
				return { block: true, reason: `AgentBus could not confirm this action is permitted (hook exit ${res.code}). Blocked because approval could not be checked, not because it was refused.` };
			}
			return;
		}
		if (decision === "deny") return { block: true, reason: `AgentBus blocked this action: ${reason}` };
	});

	pi.on("session_shutdown", async () => {
		if (!agent) return;
		await stop();
		if (key) await run(["session-end"], hookEnv(), cwd, undefined, 5_000);
		await log(`session ended for ${agent}`);
	});

	pi.registerCommand("agentbus", {
		description: "AgentBus wake status for this session",
		handler: async (_args, ctx) => {
			const msg = !agent
				? "AgentBus: off here (no $AGENTBUS_AGENT and no .agentbus/agent above this directory)."
				: `AgentBus: ${agent} — wake ${state}. Log: ${LOG_PATH}`;
			ctx.ui.notify(msg, state === "live" || state === "off" ? "info" : "warning");
		},
	});
}
