---
name: homebased
description: Run long, unattended agent tasks (codex, claude, grok) through the homebased daemon and handle the HOMEBASED_EVENT callback that returns to this Codex thread. Use when work should run in the background and report back later, when a HOMEBASED_EVENT message arrives, when checking, cancelling, or resubmitting homebased tasks, or when this session is itself a homebased worker. Do not use for work that finishes inside the current turn.
---

# Homebased

`homebased` is a user daemon that runs one agent CLI per task, detached from this session, and sends exactly one `HOMEBASED_EVENT` message back to the submitting Codex thread when the process exits. It owns the reporting contract end to end: the daemon spawns the worker, the worker appends reports with `homebased task report`, and the daemon delivers the event with `codex queue`. The orchestrator submits a JSON spec, ends its turn, and acts when the event arrives.

## Rules that hold everywhere

- Never run `codex queue` yourself, and never tell a worker to run it. Delivery belongs to `homebased`.
- Submit only with `homebased task submit --spec <file|->`. There are no per-field submit flags.
- Always pass `--json` on data commands and parse the result. Every JSON object carries `api_version: 1`.
- Do not poll a running task in a loop. Submit, tell the user the task id, end the turn, and wait for the event. Inspect on demand only.
- Use `homebased daemon stop` or `homebased daemon restart`, never raw `systemctl` or `launchctl`, so in-flight tasks are protected.
- Task ids are full UUIDs. Prefix matching does not exist.
- Delivery is at-least-once. Treat a repeated event for the same task and event name as a duplicate, not a new result.

## Route

Pick the first row that matches, then read only that file.

| Situation | Read |
| --- | --- |
| `HOMEBASED_TASK_ID` is set in this session's environment | [worker.md](references/worker.md). You are the worker, not the orchestrator. |
| A message starting with `HOMEBASED_EVENT ` arrived | [events.md](references/events.md) |
| Starting background work, writing a spec, choosing agent or timeout, or finding the thread id | [submit.md](references/submit.md) |
| Listing, showing, reading logs, or cancelling tasks | [inspect.md](references/inspect.md) |
| A command exited non-zero, `daemon_unavailable`, or the socket is down | [errors.md](references/errors.md) |
| `homebased` is missing, the daemon is not installed, or the binary was rebuilt | [setup.md](references/setup.md) |

## Minimal flow

```bash
homebased --json daemon status                      # socket must be "up"
homebased --json task submit --spec "$spec" --dry-run   # validate, see child argv
homebased --json task submit --spec "$spec"         # returns {"id": "<uuid>", "status": "queued"}
```

Then end the turn. The event arrives later as one line: `HOMEBASED_EVENT {...}`.
