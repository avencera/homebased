---
name: homebased
description: Run long, unattended agent CLIs and general task commands through the homebased daemon and handle the HOMEBASED_EVENT callback that returns to this Codex thread or Claude Code session. Use when the user invokes $homebased or $hbd, when work should run in the background and report back later, when a HOMEBASED_EVENT message arrives, when checking, cancelling, or resubmitting homebased tasks, or when this session is itself a homebased worker. Do not use for work that finishes inside the current turn.
---

# Homebased

`homebased` (invoke as `$homebased` or `$hbd`) is a user daemon that runs one detached child per supervised task and sends `HOMEBASED_EVENT` messages to the submitting Codex thread or Claude Code session. A task is either an `agent` workload (Codex, Claude, Grok, or OpenCode with a prompt and reporting trailer) or a `task` workload (arbitrary argv such as `cargo build --release` or `gh pr checks --watch`). The daemon owns the lifecycle end to end: queue, run, stream combined output to `output.log`, send a check reminder when output stays idle for the timeout, cancel only on explicit request, and deliver events. The orchestrator submits a task, ends its turn, and acts when events arrive.

## Rules that hold everywhere

- Never run `codex queue` yourself, never write to a Claude Code messaging socket yourself, and never tell a worker to do either. Delivery belongs to `homebased`.
- Leave `thread` out. Submit fills it in: a worker's parent task thread, otherwise the calling Codex thread or Claude Code session (`CODEX_THREAD_ID`, `CLAUDE_CODE_SESSION_ID`). A plain terminal submits a task whose events reach no session.
- Run a command with `homebased task run --name "<goal>" -- <program> <args...>`. Write a JSON spec and `homebased task submit --spec <file|->` only for an `agent` or `container` workload, or for fields `task run` lacks.
- Inside a T3 Code thread (the `t3-code` MCP tools are present), read [t3.md](references/t3.md) before submitting: T3 delivery, `delegate_task`, and `task wait`.
- Submit on the machine that owns the thread. To run the child elsewhere, pass `--machine` (or set `machine` in the spec); add `--wait-for-machine` for a machine that is often asleep. See [submit.md](references/submit.md#wait-for-an-offline-machine).
- Send all GPU work through the resource queue. Read [resource-queue.md](references/resource-queue.md).
- Set `name` to a short goal label, not the agent or the CLI.
- Pass `--json` on data commands and parse the result.
- Event delivery is at-least-once. Deduplicate by `(task, seq)`.
- Do not poll a running task in a loop. Submit, tell the user the task id, end the turn, and wait for events.
- To start work only after other tasks succeed, submit it now with `--after <id>` (or `after` in the spec). Do not wait for their events to submit the next phase.
- An agent worker with a long command submits it as its own task, reports `waiting`, and exits. On `TASK_WAITING`, do nothing.
- Prefer a `task` workload for long commands and CI watchers. Use `agent` only when a model must reason and produce a report.
- Use `homebased message send` for a direct message to a session, `message send --worker <task>` to steer a running Claude worker, and `task followup` to resume a finished one. Read [messages.md](references/messages.md).
- Use `homebased t3 launch --machine <name>` to start a top-level T3 Code thread on another Fleet machine. Read [t3.md](references/t3.md#launching-a-thread-on-another-machine) first.
- Use `homebased daemon stop` or `homebased daemon restart`, never raw `systemctl` or `launchctl`, so in-flight tasks are protected.

## Route

Pick the first row that matches, then read only that file.

| Situation | Read |
| --- | --- |
| `HOMEBASED_TASK_ID` is set in this session's environment | [worker.md](references/worker.md). You are the worker, not the orchestrator. |
| A message starting with `HOMEBASED_EVENT ` arrived | [events.md](references/events.md) |
| GPU work, resource queues, job specs, priority, preemption, or Attention | [resource-queue.md](references/resource-queue.md) |
| Writing a spec for an agent or container, following up a finished task, chaining work with `after`, or retrying a lost submit | [submit.md](references/submit.md) |
| Listing, showing, reading logs, or cancelling tasks, or finding the tokens workers spent | [inspect.md](references/inspect.md) |
| Inside a T3 Code thread (the `t3-code` MCP tools are present) | [t3.md](references/t3.md) |
| Starting a T3 Code thread on another machine | [t3.md](references/t3.md#launching-a-thread-on-another-machine) |
| Configuring Fleet or discovering machines | [fleet.md](references/fleet.md) |
| Sending a direct message to a session or thread | [messages.md](references/messages.md) |
| A command exited non-zero, `daemon_unavailable`, or the socket is down | [errors.md](references/errors.md) |
| `homebased` is missing, the daemon is not installed, or the binary was rebuilt | [setup.md](references/setup.md) |

## Minimal flow

```bash
homebased task run --name "release build" --timeout 2h -- cargo build --release
homebased task run --name "CI for #123" -- gh pr checks 123 --watch --interval 30
homebased --json task submit --spec spec.json --dry-run   # agent or container: validate first
homebased --json task submit --spec spec.json
```

Each prints the task id. Then end the turn. Events arrive later as one line: `HOMEBASED_EVENT {...}`.
