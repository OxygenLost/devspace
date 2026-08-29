import assert from "node:assert/strict";
import {
  HeadTailBuffer,
  ProcessSessionManager,
  SequencedOutputBuffer,
} from "./process-sessions.js";

const smallBuffer = new HeadTailBuffer(100);
smallBuffer.append("hello\n");
assert.deepEqual(smallBuffer.drain(100), { output: "hello\n", truncated: false });
assert.deepEqual(smallBuffer.drain(100), { output: "", truncated: false });

const headTail = new HeadTailBuffer(10);
headTail.append("start-middle-end");
const headTailResult = headTail.drain(1_000);
assert.equal(headTailResult.truncated, true);
assert.match(headTailResult.output, /^start/);
assert.match(headTailResult.output, /e-end$/);
assert.match(headTailResult.output, /characters omitted/);

const responseLimited = new HeadTailBuffer(100);
responseLimited.append("abcdef".repeat(20));
const responseLimitedResult = responseLimited.drain(40);
assert.equal(responseLimitedResult.truncated, true);
assert.match(responseLimitedResult.output, /^abc/);
assert.match(responseLimitedResult.output, /def$/);

const unicodeBuffer = new HeadTailBuffer(4);
unicodeBuffer.append("a🙂b🙂c");
const unicodeResult = unicodeBuffer.drain(1_000);
assert.equal(unicodeResult.truncated, true);
assert.match(unicodeResult.output, /^a🙂/);
assert.match(unicodeResult.output, /🙂c$/);

const replayBuffer = new SequencedOutputBuffer(100);
replayBuffer.append("one", "stdout");
replayBuffer.append("two", "stderr");
const replayFirst = replayBuffer.readAfter(0, 4_096);
const replayAgain = replayBuffer.readAfter(0, 4_096);
assert.equal(replayFirst.output, "onetwo");
assert.deepEqual(replayAgain, replayFirst);
assert.equal(replayFirst.nextSeq, 2);
assert.equal(replayFirst.gap, false);
assert.equal(replayBuffer.readAfter(replayFirst.nextSeq, 4_096).output, "");

const evictedBuffer = new SequencedOutputBuffer(5);
evictedBuffer.append("abc", "stdout");
evictedBuffer.append("def", "stderr");
const evictedRead = evictedBuffer.readAfter(0, 4_096);
assert.equal(evictedRead.gap, true);
assert.equal(evictedRead.firstRetainedSeq, 2);
assert.equal(evictedRead.output, "def");

const manager = new ProcessSessionManager({
  maxBufferCharacters: 1_024,
  completedSessionTtlMs: 1_000,
});

const node = process.platform === "win32"
  ? `"${process.execPath}"`
  : JSON.stringify(process.execPath);

const foreground = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "console.log('foreground')"`,
  yieldTimeMs: 2_000,
});
assert.equal(foreground.running, false);
assert.equal(foreground.exitCode, 0);
assert.match(foreground.output, /foreground/);
assert.equal(foreground.sessionId, undefined);
const retainedForeground = manager.list("workspace-a").find((session) =>
  session.command.includes("console.log('foreground')"),
);
assert.ok(retainedForeground);
const retainedForegroundRead = await manager.read({
  workspaceId: "workspace-a",
  sessionId: retainedForeground.sessionId,
  afterSeq: 0,
  waitMs: 0,
  maxBytes: 4_096,
});
assert.match(retainedForegroundRead.output, /foreground/);

const environment = await manager.start({
  workspaceId: "workspace-a",
  workspaceRoot: "/tmp/devspace-workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "console.log([process.env.NO_COLOR, process.env.TERM, process.env.PAGER, process.env.GIT_PAGER, process.env.GH_PAGER, process.env.CODEX_CI, process.env.DEVSPACE_WORKSPACE_ID, process.env.DEVSPACE_WORKSPACE_ROOT].join(','))"`,
  yieldTimeMs: 2_000,
});
assert.equal(environment.running, false);
assert.match(environment.output, /1,dumb,cat,cat,cat,1,workspace-a,\/tmp\/devspace-workspace-a/);

const background = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "setTimeout(() => console.log('finished'), 100)"`,
  yieldTimeMs: 5,
});
assert.equal(background.running, true);
assert.ok(background.sessionId);
assert.equal(typeof background.sessionId, "number");

await assert.rejects(
  manager.write({
    workspaceId: "workspace-b",
    sessionId: background.sessionId,
    yieldTimeMs: 1,
  }),
  /does not belong to workspace/,
);

const completed = await manager.write({
  workspaceId: "workspace-a",
  sessionId: background.sessionId,
  yieldTimeMs: 2_000,
});
assert.equal(completed.running, false);
assert.equal(completed.exitCode, 0);
assert.match(completed.output, /finished/);

const completedSessions = manager.list("workspace-a");
assert.ok(completedSessions.some((session) => session.sessionId === background.sessionId));
assert.equal(manager.list("workspace-b").length, 0);
const completedReplay = await manager.read({
  workspaceId: "workspace-a",
  sessionId: background.sessionId,
  afterSeq: 0,
  waitMs: 0,
  maxBytes: 4_096,
});
const completedReplayAgain = await manager.read({
  workspaceId: "workspace-a",
  sessionId: background.sessionId,
  afterSeq: 0,
  waitMs: 0,
  maxBytes: 4_096,
});
assert.match(completedReplay.output, /finished/);
assert.deepEqual(completedReplayAgain.chunks, completedReplay.chunks);
assert.equal(completedReplayAgain.output, completedReplay.output);
assert.equal(
  (await manager.read({
    workspaceId: "workspace-a",
    sessionId: background.sessionId,
    afterSeq: completedReplay.nextSeq,
    waitMs: 0,
    maxBytes: 4_096,
  })).output,
  "",
);

const incremental = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  workingDirectory: ".",
  command: `${node} -e "console.log('first'); setTimeout(() => console.log('second'), 500); setTimeout(() => process.exit(0), 900)"`,
  yieldTimeMs: 20,
});
assert.equal(incremental.running, true);
assert.ok(incremental.sessionId);
const incrementalFirst = await manager.read({
  workspaceId: "workspace-a",
  sessionId: incremental.sessionId,
  afterSeq: 0,
  waitMs: 2_000,
  maxBytes: 4_096,
});
assert.match(incrementalFirst.output, /first/);
const incrementalSecond = await manager.read({
  workspaceId: "workspace-a",
  sessionId: incremental.sessionId,
  afterSeq: incrementalFirst.nextSeq,
  waitMs: 2_000,
  maxBytes: 4_096,
});
assert.match(incrementalSecond.output, /second/);
assert.ok(incrementalSecond.nextSeq > incrementalFirst.nextSeq);
const incrementalExit = await manager.read({
  workspaceId: "workspace-a",
  sessionId: incremental.sessionId,
  afterSeq: incrementalSecond.nextSeq,
  waitMs: 2_000,
  maxBytes: 4_096,
});
assert.equal(incrementalExit.running, false);

const frozenDuration = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "setTimeout(() => process.exit(0), 60)"`,
  yieldTimeMs: 1,
});
assert.equal(frozenDuration.running, true);
assert.ok(frozenDuration.sessionId);
await new Promise((resolve) => setTimeout(resolve, 220));
const frozenDurationCompleted = await manager.write({
  workspaceId: "workspace-a",
  sessionId: frozenDuration.sessionId,
  yieldTimeMs: 1,
});
assert.equal(frozenDurationCompleted.running, false);
assert.equal(frozenDurationCompleted.exitCode, 0);
assert.ok(
  frozenDurationCompleted.wallTimeMs < 180,
  `completed process wall time must freeze at exit, got ${frozenDurationCompleted.wallTimeMs}ms`,
);

const interactive = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "process.stdin.once('data', data => { console.log('input:' + data.toString().trim()); process.exit(0); })"`,
  yieldTimeMs: 5,
});
assert.equal(interactive.running, true);
assert.ok(interactive.sessionId);
assert.equal(typeof interactive.sessionId, "number");

const inputResult = await manager.write({
  workspaceId: "workspace-a",
  sessionId: interactive.sessionId,
  chars: "hello\n",
  yieldTimeMs: 2_000,
});
assert.equal(inputResult.running, false);
assert.match(inputResult.output, /input:hello/);

const defaultInteractive = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "process.stdin.once('data', data => setTimeout(() => { console.log('default-input:' + data.toString().trim()); process.exit(0); }, 100))"`,
  yieldTimeMs: 5,
});
assert.equal(defaultInteractive.running, true);
assert.ok(defaultInteractive.sessionId);

const defaultInputResult = await manager.write({
  workspaceId: "workspace-a",
  sessionId: defaultInteractive.sessionId,
  chars: "hello\n",
});
assert.equal(defaultInputResult.running, false);
assert.match(defaultInputResult.output, /default-input:hello/);

const noisyInteractive = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "setInterval(() => console.log('tick'), 10); process.stdin.once('data', data => { console.log('input:' + data.toString().trim()); process.exit(0); })"`,
  yieldTimeMs: 100,
});
assert.equal(noisyInteractive.running, true);
assert.ok(noisyInteractive.sessionId);

await new Promise((resolve) => setTimeout(resolve, 50));
const noisyInputResult = await manager.write({
  workspaceId: "workspace-a",
  sessionId: noisyInteractive.sessionId,
  chars: "hello\n",
  yieldTimeMs: 2_000,
});
assert.equal(noisyInputResult.running, false);
assert.match(noisyInputResult.output, /input:hello/);

const interruptible = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "setInterval(() => console.log('tick'), 10)"`,
  yieldTimeMs: 100,
});
assert.equal(interruptible.running, true);
assert.ok(interruptible.sessionId);

await new Promise((resolve) => setTimeout(resolve, 50));
const interrupted = await manager.write({
  workspaceId: "workspace-a",
  sessionId: interruptible.sessionId,
  chars: "\u0003",
  yieldTimeMs: 2_000,
});
assert.equal(interrupted.running, false);
if (process.platform !== "win32") assert.equal(interrupted.signal, "SIGINT");

let buffered = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "console.log('x'.repeat(5000)); setTimeout(() => {}, 100)"`,
  yieldTimeMs: 50,
  maxOutputTokens: 100,
});
if (!buffered.outputTruncated && buffered.sessionId) {
  buffered = await manager.write({
    workspaceId: "workspace-a",
    sessionId: buffered.sessionId,
    yieldTimeMs: 2_000,
    maxOutputTokens: 100,
  });
}
assert.equal(buffered.outputTruncated, true);
if (buffered.sessionId) manager.terminate("workspace-a", buffered.sessionId);

const terminable = await manager.start({
  workspaceId: "workspace-a",
  cwd: process.cwd(),
  command: `${node} -e "setInterval(() => {}, 1000)"`,
  yieldTimeMs: 5,
});
assert.equal(terminable.running, true);
assert.ok(terminable.sessionId);
const terminated = await manager.terminateAndWait("workspace-a", terminable.sessionId, 2_000);
assert.equal(terminated.running, false);

try {
  if (process.platform === "win32") {
    const pty = await manager.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: "echo pty-ok",
      tty: true,
      yieldTimeMs: 10_000,
    });
    assert.equal(pty.running, false);
    assert.match(pty.output, /pty-ok/);
  } else {
    const pty = await manager.start({
      workspaceId: "workspace-a",
      cwd: process.cwd(),
      command: `${node} -e "setTimeout(() => console.log('columns:' + process.stdout.columns), 250)"`,
      tty: true,
      columns: 80,
      rows: 24,
      yieldTimeMs: 10,
    });
    assert.equal(pty.running, true);
    assert.ok(pty.sessionId);

    const resizedPty = await manager.write({
      workspaceId: "workspace-a",
      sessionId: pty.sessionId,
      columns: 120,
      rows: 30,
      yieldTimeMs: 2_000,
    });
    assert.equal(resizedPty.running, false);
    assert.match(resizedPty.output, /columns:120/);
  }
} finally {
  manager.shutdown();
}
