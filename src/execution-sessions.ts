export type ExecutionOutputStream = "stdout" | "stderr" | "pty" | "system";

export interface ExecutionOutputChunk {
  seq: number;
  stream: ExecutionOutputStream;
  data: string;
  timestamp: number;
}

export interface StartExecutionInput {
  workspaceId: string;
  command: string;
  cwd: string;
  workspaceRoot?: string;
  workingDirectory?: string;
  tty?: boolean;
  columns?: number;
  rows?: number;
  yieldTimeMs?: number;
  maxOutputTokens?: number;
}

export interface WriteExecutionInput {
  workspaceId: string;
  sessionId: number;
  chars?: string;
  columns?: number;
  rows?: number;
  yieldTimeMs?: number;
  maxOutputTokens?: number;
}

export interface ReadExecutionInput {
  workspaceId: string;
  sessionId: number;
  afterSeq?: number;
  waitMs?: number;
  maxBytes?: number;
}

export interface ExecutionSnapshot {
  sessionId?: number;
  output: string;
  outputTruncated: boolean;
  running: boolean;
  exitCode?: number;
  signal?: string;
  wallTimeMs: number;
  nextSeq: number;
  firstRetainedSeq: number;
  lastSeq: number;
}

export interface ExecutionReadSnapshot extends ExecutionSnapshot {
  sessionId: number;
  afterSeq: number;
  gap: boolean;
  hasMore: boolean;
  chunks: ExecutionOutputChunk[];
}

export interface ExecutionSessionSummary {
  sessionId: number;
  command: string;
  workingDirectory: string;
  tty: boolean;
  osPid?: number;
  startedAt: number;
  finishedAt?: number;
  running: boolean;
  exitCode?: number;
  signal?: string;
  firstRetainedSeq: number;
  lastSeq: number;
}

export interface ExecutionSessionRuntime {
  start(input: StartExecutionInput): Promise<ExecutionSnapshot>;
  write(input: WriteExecutionInput): Promise<ExecutionSnapshot>;
  read(input: ReadExecutionInput): Promise<ExecutionReadSnapshot>;
  list(workspaceId: string): ExecutionSessionSummary[];
  terminate(workspaceId: string, sessionId: number): void;
  terminateAndWait(
    workspaceId: string,
    sessionId: number,
    waitMs?: number,
  ): Promise<ExecutionSessionSummary>;
  shutdown(): void;
}
