import { spawn } from "node:child_process";
import type {
  ExecutionOutputChunk,
  ExecutionOutputStream,
  ExecutionReadSnapshot,
  ExecutionSessionRuntime,
  ExecutionSessionSummary,
  ExecutionSnapshot,
  ReadExecutionInput,
  StartExecutionInput,
  WriteExecutionInput,
} from "./execution-sessions.js";
import { resolveShellCommand, terminateProcessTree } from "./process-platform.js";

export type ProcessSnapshot = ExecutionSnapshot;
export type StartCommandInput = StartExecutionInput;
export type WriteStdinInput = WriteExecutionInput;

const DEFAULT_EXEC_YIELD_MS = 10_000;
const DEFAULT_INTERACTIVE_YIELD_MS = 250;
const DEFAULT_POLL_YIELD_MS = 5_000;
const MAX_COMMAND_YIELD_MS = 30_000;
const MAX_POLL_YIELD_MS = 110_000;
const DEFAULT_MAX_OUTPUT_TOKENS = 10_000;
const DEFAULT_BUFFER_CHARACTERS = 1_000_000;
const DEFAULT_READ_BYTES = 64 * 1_024;
const MIN_READ_BYTES = 4 * 1_024;
const MAX_READ_BYTES = 512 * 1_024;
const MAX_OUTPUT_CHUNK_BYTES = 2 * 1_024;
const COMPLETED_SESSION_TTL_MS = 5 * 60 * 1_000;
const DEFAULT_COLUMNS = 80;
const DEFAULT_ROWS = 24;

interface ManagedProcess {
  pid?: number;
  write(data: string): void;
  kill(signal?: NodeJS.Signals): void;
  resize?(columns: number, rows: number): void;
}

interface ProcessSession {
  id: number;
  workspaceId: string;
  command: string;
  workingDirectory: string;
  tty: boolean;
  process?: ManagedProcess;
  startedAt: number;
  finishedAt?: number;
  columns: number;
  rows: number;
  buffer: SequencedOutputBuffer;
  legacyCursor: number;
  running: boolean;
  exitCode?: number;
  signal?: string;
  exitPromise: Promise<void>;
  resolveExit: () => void;
  activityWaiters: Set<() => void>;
  cleanupTimer?: NodeJS.Timeout;
}

interface ProcessSessionManagerOptions {
  maxBufferCharacters?: number;
  completedSessionTtlMs?: number;
}

function boundedInteger(value: number | undefined, fallback: number, maximum: number): number {
  if (value === undefined) return fallback;
  if (!Number.isFinite(value) || value < 0) {
    throw new Error("Duration and output limits must be non-negative.");
  }
  return Math.min(Math.floor(value), maximum);
}

function terminalSize(value: number | undefined, fallback: number): number {
  if (value === undefined) return fallback;
  if (!Number.isInteger(value) || value < 1 || value > 1_000) {
    throw new Error("Terminal dimensions must be integers between 1 and 1000.");
  }
  return value;
}

function processEnvironment(input?: {
  workspaceId?: string;
  workspaceRoot?: string;
}): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
    ),
    NO_COLOR: "1",
    TERM: "dumb",
    PAGER: "cat",
    GIT_PAGER: "cat",
    GH_PAGER: "cat",
    CODEX_CI: "1",
    LANG: process.env.LANG ?? "C.UTF-8",
    LC_ALL: process.env.LC_ALL ?? "C.UTF-8",
    ...(input?.workspaceId ? { DEVSPACE_WORKSPACE_ID: input.workspaceId } : {}),
    ...(input?.workspaceRoot ? { DEVSPACE_WORKSPACE_ROOT: input.workspaceRoot } : {}),
  };
}

function codePointLength(value: string): number {
  return Array.from(value).length;
}

function sliceCodePoints(value: string, start: number, end?: number): string {
  return Array.from(value).slice(start, end).join("");
}

function takeHead(value: string, count: number): string {
  if (count <= 0) return "";
  return sliceCodePoints(value, 0, count);
}

function takeTail(value: string, count: number): string {
  if (count <= 0) return "";
  const characters = Array.from(value);
  return characters.slice(Math.max(0, characters.length - count)).join("");
}

function splitBudget(maxCharacters: number): { head: number; tail: number } {
  return {
    head: Math.ceil(maxCharacters / 2),
    tail: Math.floor(maxCharacters / 2),
  };
}

function formatHeadTail(head: string, tail: string, omittedCharacters: number): string {
  if (omittedCharacters <= 0) return head + tail;
  return `${head}\n... output truncated (${omittedCharacters} characters omitted) ...\n${tail}`;
}

export class HeadTailBuffer {
  private head = "";
  private tail = "";
  private totalCharacters = 0;

  constructor(private readonly maxCharacters: number) {
    if (!Number.isInteger(maxCharacters) || maxCharacters < 1) {
      throw new Error("Head/tail buffer limit must be a positive integer.");
    }
  }

  append(output: string): void {
    if (!output) return;

    const previousTotal = this.totalCharacters;
    this.totalCharacters += codePointLength(output);

    if (this.totalCharacters <= this.maxCharacters) {
      this.head += output;
      return;
    }

    const budget = splitBudget(this.maxCharacters);
    if (previousTotal <= this.maxCharacters) {
      const fullOutput = this.head + output;
      this.head = takeHead(fullOutput, budget.head);
      this.tail = takeTail(fullOutput, budget.tail);
      return;
    }

    this.tail = takeTail(this.tail + output, budget.tail);
  }

  hasOutput(): boolean {
    return this.totalCharacters > 0;
  }

  drain(maxCharacters: number): { output: string; truncated: boolean } {
    if (!Number.isInteger(maxCharacters) || maxCharacters < 1) {
      throw new Error("Output limit must be a positive integer.");
    }

    const omittedByBuffer = Math.max(
      0,
      this.totalCharacters - codePointLength(this.head) - codePointLength(this.tail),
    );
    const retained = formatHeadTail(this.head, this.tail, omittedByBuffer);
    const output = truncateOutput(retained, maxCharacters);
    const truncated = omittedByBuffer > 0 || output.truncated;

    this.head = "";
    this.tail = "";
    this.totalCharacters = 0;

    return { output: output.output, truncated };
  }
}

interface SequencedReadResult {
  chunks: ExecutionOutputChunk[];
  output: string;
  afterSeq: number;
  nextSeq: number;
  firstRetainedSeq: number;
  lastSeq: number;
  gap: boolean;
  hasMore: boolean;
}

function splitOutputChunks(value: string, maxBytes: number, maxCharacters: number): string[] {
  const chunks: string[] = [];
  let current = "";
  let currentBytes = 0;
  let currentCharacters = 0;

  for (const character of value) {
    const characterBytes = Buffer.byteLength(character, "utf8");
    if (
      current.length > 0 &&
      (currentBytes + characterBytes > maxBytes || currentCharacters + 1 > maxCharacters)
    ) {
      chunks.push(current);
      current = "";
      currentBytes = 0;
      currentCharacters = 0;
    }
    current += character;
    currentBytes += characterBytes;
    currentCharacters++;
  }

  if (current) chunks.push(current);
  return chunks;
}

export class SequencedOutputBuffer {
  private readonly chunks: ExecutionOutputChunk[] = [];
  private totalCharacters = 0;
  private nextSequence = 1;
  private evictedThroughSeq = 0;

  constructor(private readonly maxCharacters: number) {
    if (!Number.isInteger(maxCharacters) || maxCharacters < 1) {
      throw new Error("Sequenced output buffer limit must be a positive integer.");
    }
  }

  append(data: string, stream: ExecutionOutputStream): number {
    if (!data) return this.lastSeq();

    const pieces = splitOutputChunks(data, MAX_OUTPUT_CHUNK_BYTES, this.maxCharacters);
    for (const piece of pieces) {
      const chunk: ExecutionOutputChunk = {
        seq: this.nextSequence++,
        stream,
        data: piece,
        timestamp: Date.now(),
      };
      this.chunks.push(chunk);
      this.totalCharacters += codePointLength(piece);
      this.trim();
    }
    return this.lastSeq();
  }

  hasAfter(afterSeq: number): boolean {
    return this.lastSeq() > afterSeq;
  }

  firstRetainedSeq(): number {
    return this.chunks[0]?.seq ?? this.nextSequence;
  }

  lastSeq(): number {
    return this.nextSequence - 1;
  }

  readAfter(afterSeq: number, maxBytes?: number): SequencedReadResult {
    if (!Number.isInteger(afterSeq) || afterSeq < 0) {
      throw new Error("Process output cursor must be a non-negative integer.");
    }
    if (maxBytes !== undefined && (!Number.isInteger(maxBytes) || maxBytes < MIN_READ_BYTES)) {
      throw new Error(`Process output maxBytes must be at least ${MIN_READ_BYTES}.`);
    }

    const gap = afterSeq < this.evictedThroughSeq;
    const effectiveAfterSeq = Math.max(afterSeq, this.evictedThroughSeq);
    const available = this.chunks.filter((chunk) => chunk.seq > effectiveAfterSeq);
    const selected: ExecutionOutputChunk[] = [];
    let selectedBytes = 0;

    for (const chunk of available) {
      const chunkBytes = Buffer.byteLength(chunk.data, "utf8");
      if (maxBytes !== undefined && selectedBytes + chunkBytes > maxBytes) break;
      selected.push(chunk);
      selectedBytes += chunkBytes;
    }

    const nextSeq = selected.at(-1)?.seq ?? effectiveAfterSeq;
    const lastSeq = this.lastSeq();
    return {
      chunks: selected,
      output: selected.map((chunk) => chunk.data).join(""),
      afterSeq,
      nextSeq,
      firstRetainedSeq: this.firstRetainedSeq(),
      lastSeq,
      gap,
      hasMore: available.length > selected.length,
    };
  }

  private trim(): void {
    while (this.totalCharacters > this.maxCharacters && this.chunks.length > 0) {
      const removed = this.chunks.shift();
      if (!removed) break;
      this.totalCharacters -= codePointLength(removed.data);
      this.evictedThroughSeq = removed.seq;
    }
  }
}

function truncateOutput(output: string, maxCharacters: number): { output: string; truncated: boolean } {
  const outputCharacters = codePointLength(output);
  if (outputCharacters <= maxCharacters) return { output, truncated: false };

  const marker = "\n... output truncated ...\n";
  const markerCharacters = codePointLength(marker);
  const available = Math.max(0, maxCharacters - markerCharacters);
  const budget = splitBudget(available);
  return {
    output: takeHead(output, budget.head) + marker + takeTail(output, budget.tail),
    truncated: true,
  };
}

function readByteLimit(value: number | undefined): number {
  if (value === undefined) return DEFAULT_READ_BYTES;
  if (!Number.isInteger(value) || value < MIN_READ_BYTES || value > MAX_READ_BYTES) {
    throw new Error(`Process output maxBytes must be between ${MIN_READ_BYTES} and ${MAX_READ_BYTES}.`);
  }
  return value;
}

function outputCursor(value: number | undefined): number {
  if (value === undefined) return 0;
  if (!Number.isInteger(value) || value < 0) {
    throw new Error("Process output cursor must be a non-negative integer.");
  }
  return value;
}

export class ProcessSessionManager implements ExecutionSessionRuntime {
  private readonly sessions = new Map<number, ProcessSession>();
  private readonly maxBufferCharacters: number;
  private readonly completedSessionTtlMs: number;
  private nextSessionId = 1;

  constructor(options: ProcessSessionManagerOptions = {}) {
    this.maxBufferCharacters = options.maxBufferCharacters ?? DEFAULT_BUFFER_CHARACTERS;
    this.completedSessionTtlMs = options.completedSessionTtlMs ?? COMPLETED_SESSION_TTL_MS;
  }

  async start(input: StartExecutionInput): Promise<ExecutionSnapshot> {
    const session = this.createSession(input);
    this.sessions.set(session.id, session);

    try {
      if (input.tty && process.platform !== "win32") await this.startPty(session, input);
      else this.startPipe(session, input);
    } catch (error) {
      this.sessions.delete(session.id);
      throw error;
    }

    const yieldTimeMs = boundedInteger(input.yieldTimeMs, DEFAULT_EXEC_YIELD_MS, MAX_COMMAND_YIELD_MS);
    await this.waitForExit(session, yieldTimeMs);

    return this.consumeLegacy(session, input.maxOutputTokens);
  }

  async write(input: WriteExecutionInput): Promise<ExecutionSnapshot> {
    const session = this.getOwnedSession(input.workspaceId, input.sessionId);
    const chars = input.chars ?? "";
    const interactionRequested =
      chars.length > 0 || input.columns !== undefined || input.rows !== undefined;

    if (input.columns !== undefined || input.rows !== undefined) {
      session.columns = terminalSize(input.columns, session.columns);
      session.rows = terminalSize(input.rows, session.rows);
      if (!session.process?.resize) {
        throw new Error(`Process session ${session.id} is not a PTY and cannot be resized.`);
      }
      session.process.resize(session.columns, session.rows);
    }

    const interruptRequested = chars.includes("\u0003") && session.running;
    if (interruptRequested) {
      session.process?.kill("SIGINT");
    }
    const writableChars = chars.replaceAll("\u0003", "");
    if (writableChars && session.running) session.process?.write(writableChars);

    if ((interactionRequested || !session.buffer.hasAfter(session.legacyCursor)) && session.running) {
      const fallback = interactionRequested ? DEFAULT_INTERACTIVE_YIELD_MS : DEFAULT_POLL_YIELD_MS;
      const maximum = interactionRequested ? MAX_COMMAND_YIELD_MS : MAX_POLL_YIELD_MS;
      const yieldTimeMs = boundedInteger(input.yieldTimeMs, fallback, maximum);
      await this.waitForExit(session, yieldTimeMs);
    }

    return this.consumeLegacy(session, input.maxOutputTokens);
  }

  async read(input: ReadExecutionInput): Promise<ExecutionReadSnapshot> {
    const session = this.getOwnedSession(input.workspaceId, input.sessionId);
    const afterSeq = outputCursor(input.afterSeq);
    const maxBytes = readByteLimit(input.maxBytes);
    const waitMs = boundedInteger(input.waitMs, DEFAULT_POLL_YIELD_MS, MAX_POLL_YIELD_MS);

    if (session.running && !session.buffer.hasAfter(afterSeq)) {
      await this.waitForActivity(session, afterSeq, waitMs);
    }

    const buffered = session.buffer.readAfter(afterSeq, maxBytes);
    return {
      sessionId: session.id,
      output: buffered.output,
      outputTruncated: buffered.gap || buffered.hasMore,
      running: session.running,
      exitCode: session.exitCode,
      signal: session.signal,
      wallTimeMs: (session.finishedAt ?? Date.now()) - session.startedAt,
      afterSeq,
      nextSeq: buffered.nextSeq,
      firstRetainedSeq: buffered.firstRetainedSeq,
      lastSeq: buffered.lastSeq,
      gap: buffered.gap,
      hasMore: buffered.hasMore,
      chunks: buffered.chunks,
    };
  }

  list(workspaceId: string): ExecutionSessionSummary[] {
    return [...this.sessions.values()]
      .filter((session) => session.workspaceId === workspaceId)
      .sort((left, right) => {
        if (left.running !== right.running) return left.running ? -1 : 1;
        return right.startedAt - left.startedAt;
      })
      .map((session) => this.summary(session));
  }

  terminate(workspaceId: string, sessionId: number): void {
    const session = this.getOwnedSession(workspaceId, sessionId);
    if (session.running) session.process?.kill("SIGTERM");
  }

  async terminateAndWait(
    workspaceId: string,
    sessionId: number,
    waitMs = 2_000,
  ): Promise<ExecutionSessionSummary> {
    const session = this.getOwnedSession(workspaceId, sessionId);
    const afterSeq = session.buffer.lastSeq();
    if (session.running) {
      this.terminate(workspaceId, sessionId);
      await this.waitForActivity(
        session,
        afterSeq,
        boundedInteger(waitMs, 2_000, MAX_COMMAND_YIELD_MS),
      );
    }
    return this.summary(session);
  }

  shutdown(): void {
    for (const session of this.sessions.values()) {
      if (session.cleanupTimer) clearTimeout(session.cleanupTimer);
      if (session.running) session.process?.kill("SIGTERM");
    }
    this.sessions.clear();
  }

  private async waitForExit(session: ProcessSession, yieldTimeMs: number): Promise<void> {
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        session.exitPromise,
        new Promise<void>((resolve) => {
          timer = setTimeout(resolve, yieldTimeMs);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async waitForActivity(
    session: ProcessSession,
    afterSeq: number,
    yieldTimeMs: number,
  ): Promise<void> {
    if (!session.running || session.buffer.hasAfter(afterSeq) || yieldTimeMs === 0) return;

    await new Promise<void>((resolve) => {
      let timer: NodeJS.Timeout | undefined;
      const finish = () => {
        if (timer) clearTimeout(timer);
        session.activityWaiters.delete(finish);
        resolve();
      };
      session.activityWaiters.add(finish);
      timer = setTimeout(finish, yieldTimeMs);
    });
  }

  private notifyActivity(session: ProcessSession): void {
    for (const waiter of [...session.activityWaiters]) waiter();
  }

  private createSession(input: StartExecutionInput): ProcessSession {
    let resolveExit = (): void => undefined;
    const exitPromise = new Promise<void>((resolve) => {
      resolveExit = resolve;
    });

    return {
      id: this.nextSessionId++,
      workspaceId: input.workspaceId,
      command: input.command,
      workingDirectory: input.workingDirectory ?? ".",
      tty: Boolean(input.tty),
      startedAt: Date.now(),
      columns: terminalSize(input.columns, DEFAULT_COLUMNS),
      rows: terminalSize(input.rows, DEFAULT_ROWS),
      buffer: new SequencedOutputBuffer(this.maxBufferCharacters),
      legacyCursor: 0,
      running: true,
      exitPromise,
      resolveExit,
      activityWaiters: new Set(),
    };
  }

  private startPipe(session: ProcessSession, input: StartExecutionInput): void {
    const shell = resolveShellCommand(input.command);
    const detached = process.platform !== "win32";
    const child = spawn(input.command, {
      cwd: input.cwd,
      env: processEnvironment({
        workspaceId: input.workspaceId,
        workspaceRoot: input.workspaceRoot,
      }),
      stdio: "pipe",
      windowsHide: true,
      detached,
      shell: shell.executable,
    });

    session.process = {
      pid: child.pid,
      write: (data) => child.stdin.write(data),
      kill: (signal = "SIGTERM") => terminateProcessTree(child, signal, detached),
      resize: input.tty ? () => undefined : undefined,
    };
    child.stdout.on("data", (data: Buffer) => this.append(session, data.toString("utf8"), "stdout"));
    child.stderr.on("data", (data: Buffer) => this.append(session, data.toString("utf8"), "stderr"));
    child.on("error", (error) => this.append(session, `${error.message}\n`, "system"));
    child.on("close", (code, signal) => this.finish(session, code ?? undefined, signal ?? undefined));
  }

  private async startPty(session: ProcessSession, input: StartExecutionInput): Promise<void> {
    let nodePty: typeof import("node-pty");
    try {
      nodePty = await import("node-pty");
    } catch {
      throw new Error("PTY support requires the optional node-pty dependency.");
    }

    const shell = resolveShellCommand(input.command);
    let pty: import("node-pty").IPty;
    try {
      pty = nodePty.spawn(shell.executable, shell.args, {
        cwd: input.cwd,
        env: processEnvironment({
          workspaceId: input.workspaceId,
          workspaceRoot: input.workspaceRoot,
        }),
        name: "xterm-256color",
        cols: session.columns,
        rows: session.rows,
      });
    } catch (error) {
      throw error;
    }

    session.process = {
      pid: pty.pid,
      write: (data) => pty.write(data),
      kill: (signal) => pty.kill(signal),
      resize: (columns, rows) => pty.resize(columns, rows),
    };
    pty.onData((data) => this.append(session, data, "pty"));
    pty.onExit(({ exitCode, signal }) => {
      this.finish(session, exitCode, signal === 0 ? undefined : String(signal));
    });
  }

  private finish(session: ProcessSession, exitCode?: number, signal?: string): void {
    if (!session.running) return;
    session.finishedAt = Date.now();
    session.running = false;
    session.exitCode = exitCode;
    session.signal = signal;
    session.resolveExit();
    this.notifyActivity(session);
    session.cleanupTimer = setTimeout(
      () => this.sessions.delete(session.id),
      this.completedSessionTtlMs,
    );
    session.cleanupTimer.unref();
  }

  private append(session: ProcessSession, output: string, stream: ExecutionOutputStream): void {
    session.buffer.append(output, stream);
    this.notifyActivity(session);
  }

  private consumeLegacy(session: ProcessSession, maxOutputTokens?: number): ExecutionSnapshot {
    const limit = boundedInteger(maxOutputTokens, DEFAULT_MAX_OUTPUT_TOKENS, 100_000);
    const maxCharacters = Math.max(256, limit * 4);
    const buffered = session.buffer.readAfter(session.legacyCursor);
    session.legacyCursor = buffered.nextSeq;
    const output = truncateOutput(buffered.output, maxCharacters);

    return {
      sessionId: session.running ? session.id : undefined,
      output: output.output,
      outputTruncated: buffered.gap || output.truncated,
      running: session.running,
      exitCode: session.exitCode,
      signal: session.signal,
      wallTimeMs: (session.finishedAt ?? Date.now()) - session.startedAt,
      nextSeq: buffered.nextSeq,
      firstRetainedSeq: buffered.firstRetainedSeq,
      lastSeq: buffered.lastSeq,
    };
  }

  private summary(session: ProcessSession): ExecutionSessionSummary {
    return {
      sessionId: session.id,
      command: session.command,
      workingDirectory: session.workingDirectory,
      tty: session.tty,
      osPid: session.process?.pid,
      startedAt: session.startedAt,
      finishedAt: session.finishedAt,
      running: session.running,
      exitCode: session.exitCode,
      signal: session.signal,
      firstRetainedSeq: session.buffer.firstRetainedSeq(),
      lastSeq: session.buffer.lastSeq(),
    };
  }

  private getOwnedSession(workspaceId: string, sessionId: number): ProcessSession {
    const session = this.sessions.get(sessionId);
    if (!session) throw new Error(`Unknown process session: ${sessionId}`);
    if (session.workspaceId !== workspaceId) {
      throw new Error(`Process session ${sessionId} does not belong to workspace ${workspaceId}.`);
    }
    return session;
  }
}
