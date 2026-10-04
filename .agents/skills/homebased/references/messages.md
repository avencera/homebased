# Send a direct message to a session or thread

`homebased message send` sends one message to a Claude Code session or a Codex
thread on this or another Fleet machine, or to the running Claude worker of a
Homebased task.

## Choose the destination

Use `--machine` with exactly one destination selector:

```bash
homebased --json message send \
  --machine code \
  --thread <thread-uuid> \
  --message "Please review the cluster design."
```

Or select a thread by its working directory on the receiving machine:

```bash
homebased --json message send \
  --machine code \
  --cwd '~/code/homebased' \
  --message "Please review the cluster design."
```

`--machine` accepts a known machine name or stable machine UUID. `--thread`
must be an exact Claude Code session id (`$CLAUDE_CODE_SESSION_ID`) or Codex
thread UUID. Do not use a Claude session display name such as `homebased-82`;
it changes each time the session process restarts, but the session id does
not. `--cwd` must be an absolute path or start with `~/`. The receiving daemon
expands `~/` with its own home directory and picks the most recently active
Codex session with that exact working directory. `--cwd` never selects a
Claude session, because headless Claude workers share their owner's directory;
use `--thread` or `--task` for a Claude session. The receipt includes the
resolved thread UUID and working directory. No match returns
`agent_thread_not_found`.

You can also send to the origin thread of a known task. Use `--task` alone,
without `--machine`, `--thread`, or `--cwd`:

```bash
homebased --json message send \
  --task <task-uuid> \
  --message "The build is ready for review."
```

The local daemon finds the task route in the known Fleet and sends to its
origin machine and original thread. If it cannot check a peer that may hold the
route, it returns `cluster_lookup_incomplete` instead of claiming no route
exists.

`--task` targets the task's origin thread, not the worker's thread. If the
source thread is also the destination, the send fails with `message_to_self`.
Use `homebased task followup <task-id> --message "..."` to resume a finished
Codex worker with new information. See [submit.md](submit.md).

## Send to a running worker

Use `--worker` alone, without `--machine`, `--thread`, `--cwd`, or `--task`, to
give a running Claude worker new instructions:

```bash
homebased --json message send \
  --worker <task-uuid> \
  --message "Also update the changelog before you report."
```

The local daemon finds the task in the known Fleet and sends to its execution
machine and its `worker_thread`. A Claude worker records `worker_thread` when it
starts running. The worker reads the message at its next turn boundary. If it
finishes its final turn first, the message is never read, and the sender cannot
tell. Send while the worker still has work left, and confirm from its reports
that it acted on the message.

`--worker` returns:

- `worker_message_unavailable` with `reason: no_worker_thread` for a held or queued
  task, or a running task whose agent records no thread while it runs. A Codex
  worker records its thread only when it finishes, so it cannot take a live
  message.
- `worker_message_unavailable` with `reason: terminal` once the task has ended.
  Use `homebased task followup` to resume a finished Codex worker.
- `task_not_found` or `cluster_lookup_incomplete` when no checked machine has
  the task.
- `message_to_self` when the source task is the worker's own task.

## Choose the source and conversation

By default, the CLI uses `HOMEBASED_TASK_ID` as the source task. Otherwise, it
uses `CODEX_THREAD_ID`, then `CODEX_SESSION_ID`, then `CLAUDE_CODE_SESSION_ID`, as the source thread. Pass one
explicit source when these values are not set or when you need a different
reply route:

```bash
--source-thread <thread-uuid>
--source-task <task-uuid>
```

The flags are mutually exclusive. The queued `HOMEBASED_MESSAGE` JSON has a
`source` route with the source machine and thread or task. A recipient can use
the source route to reply.

The receiver delivers the line in one of three ways:

- A live Claude Code session gets it as its next user turn through its
  messaging socket. When a T3 Code thread on the V2 orchestrator owns the
  session, T3 takes the message instead so the thread shows it, and the socket
  is the fallback.
- A Claude Code session with no live process gets it as a new turn in the T3
  Code thread that owns the session. T3 Code before V2 runs no Claude process
  between turns, so this is the usual case for an idle T3 session there. If no
  T3 thread owns the session, the send fails with `message_delivery_failed`.
- A Codex thread that a T3 Code thread owns gets it as a new turn in that
  thread, so T3 shows it. If T3 cannot take it, or no T3 thread owns the Codex
  thread, it goes through `codex queue`.

A deleted T3 thread is refused without a change. An archived T3 thread is unarchived before the turn. Before a T3 send, Homebased saves the retry route. If it cannot save that route, the send fails without sending the message. If T3 may have taken the
message but its reply was lost, the send fails with `message_delivery_failed`,
and a retry with the same `--message-id` goes only through T3, which drops the
repeat. With T3 unreachable, that retry keeps failing instead of sending a
second copy another way.

`--reply-to <message-uuid>` links a reply to an earlier message. Use
`--conversation <uuid>` to set a shared conversation UUID. By default, the
conversation UUID is the message UUID.

## Retry safely

Delivery is synchronous. The sender saves the message identity and request
content before it resolves the destination. After resolution, it saves the
destination before it delivers the line. After delivery succeeds, it saves a
receipt before it returns success. It does not keep an offline mailbox or
resume an unfinished message attempt after restart. The sender also saves the
selected machine UUID before delivery. A retry with the same message UUID
cannot move to another machine if a name changes owners.

The CLI creates a message UUID unless you pass `--message-id <uuid>`. Choose
the UUID before the first send if you may need to retry after a lost response.
Retry with the same message UUID, destination, source, body, and other options.
The receiver returns a saved receipt without another queue call when it has
one. It uses the saved thread when it retries a `--cwd` destination. Reusing a
message UUID with different request content returns `message_conflict`.
If target resolution fails, the UUID stays reserved for that request. Retrying
with the same content resolves again; changing the content returns
`message_conflict`. A rejected self-send stays unbound, so the same retry
returns `message_to_self` again.

```bash
message_id="$(uuidgen)"
homebased --json message send \
  --machine code \
  --thread <thread-uuid> \
  --source-thread <source-thread-uuid> \
  --message "Please review the cluster design." \
  --message-id "$message_id"
```

Delivery is at-least-once, not exactly-once. If the destination accepts a
message and the receiver stops before it saves the receipt, an explicit retry
can deliver the same message again. Include the message UUID in the conversation
handling and ignore a duplicate. The destination must be reachable for each
attempt. A failed attempt returns a typed error; Homebased does not retry a
message in the background.

`message_outcome_unknown` means the receiver may have queued the message. Retry
with the same message UUID and request. The sender uses the saved machine UUID
and recipient, even if the original machine name now resolves elsewhere.
`message_delivery_failed`
means the receiver did not return a success receipt. Its message gives the
reason. Retry explicitly with the same request identity. A saved receipt suppresses a repeat after a lost HTTP
response.
