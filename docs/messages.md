# Direct messages

Send a message to a Claude Code session or Codex thread on a known machine.
`--thread` takes any thread reference that `homebased thread` takes, looked up
on that machine; a UUID the lookup cannot find is sent as given. The daemon
looks it up after it checks the message's saved binding, so a retry with the
same `--message-id` reaches the thread the first attempt chose:

```sh
homebased --json message send \
  --machine code \
  --thread <thread-ref> \
  --source-thread <source-thread-uuid> \
  --message "Please review this change."
```

The CLI can also select a Codex thread by receiver-side `--cwd`, or send to
the origin thread for a task with `--task <task-uuid>` alone. This is the
origin thread, not the task worker. A send to its own source thread returns
`message_to_self`; use `task followup` for a finished worker. A Claude
session with no live process gets the message as a new turn in the T3 Code
thread that owns it, and so does a live session that a T3 thread owns. A
Codex thread that T3 owns also gets it as a T3 turn, and any other Codex thread
gets it through `codex queue`. A message to a thread that is running a turn
waits for that turn to end; `--steer` delivers it into the running turn
instead, for a stop, a correction, or an answer the turn is blocked on. Pass the same
`--message-id <uuid>` to retry after a lost response. Delivery is synchronous
and at-least-once. A receiver crash before it stores the receipt can lead to a
duplicate message. Read [messages.md](../.agents/skills/homebased/references/messages.md)
for source, reply, and retry rules.

Use `--worker <task-uuid>` alone to send new instructions to a running Claude
worker. The daemon sends to the task's execution machine and `worker_thread`.
A worker message always steers, and the worker reads it at its next turn boundary; if it finishes its final
turn first, the message is never read, and the sender cannot tell. A held or queued
task, or a running Codex worker, which records its thread only at exit, returns
`worker_message_unavailable` with `reason: no_worker_thread`. A finished task
returns `reason: terminal`.

The operator skill [`.agents/skills/homebased/SKILL.md`](../.agents/skills/homebased/SKILL.md) is the contract for submit, events, inspect, errors, and worker sessions.
