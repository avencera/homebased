# Inspect and cancel tasks

Inspection commands need the local daemon socket. `task report` writes to the local state database, and `daemon status` can inspect the local state while the socket is down.

## List

```bash
homebased --json task list --thread <thread-uuid>          # this thread's tasks
homebased --json task list --status running,queued         # in flight
homebased --quiet task list --status running               # bare ids, one per line
```

Status values: `held`, `queued`, `running`, `succeeded`, `failed`, `cancelled`, `lost`, `preempted`. `--status` accepts repeats or a comma list. `held` means the task waits on this machine for its `after` dependencies, or a continuation waits for the tasks its worker parked on, and has no process yet. Status is the process status: a worker that parked exited 0 and reads `succeeded`; its `chain` says the work is still waiting.

Each entry: `id`, `name`, `status`, `workload`, `worker_thread`, `thread`, `cwd`, `project_root`, `origin_machine`, `execution_machine`, `pid`, `callback`, `timeout_secs`, `check_timeout`, `exit_reason`, `cancel_requested_at`, `created_at`, `updated_at`, and `chain` for a run of parked work. `worker_thread` is the worker's own thread. A Claude task records its session id when it starts running. A Codex task records it when the task ends, whether it succeeds, fails, is cancelled, or is lost, if Homebased finds a session header in the first 64 KiB of `output.log`. It is omitted when unknown. `project_root`, `origin_machine`, and `execution_machine` can be absent. Entries come back in id order, which is creation order. Human list output shows `name`.

`task list` reads tasks stored on this machine. It does not query every Fleet peer. It includes tasks held here, also those that will run on another machine, and held tasks that ended before they started. Those entries also have `after`: one `{"task", "state"}` per dependency, where `state` is `pending`, or `ended` with an `outcome` of `succeeded`, `failed`, `blocked`, `cancelled`, `lost`, or `unknown`. `unknown` means the task ended but no record says how; it never counts as success.

## Show

```bash
homebased --json task show <id>
```

| Field | Meaning |
| --- | --- |
| `name` | Submitted goal label. |
| `status` | Process status, or `held`, see above. |
| `after` | Present for a task submitted with `after`: each dependency and its `state`, as in the list. A held task waits for every `pending` entry. A held continuation lists the tasks its worker waits on and starts once each one ended, however it ended. |
| `chain` | Present for an agent run that parked or continues parked work: `id` (the first run), this task's `run` number, the chain's `runs`, and its `state`. `running` names the `current` run; `waiting` names the parked run as `current`, the tasks it waits `on`, and its `continuation`; `ended` has the `outcome` of the last run, which is the outcome of the whole chain. |
| `workload` | `{"type":"agent","agent":"…","model":null\|string}`, `{"type":"task","command":[…]}`, or `{"type":"container","image":"…","args":[…]}` with optional `entrypoint` and `gpus`. Container environment values are not shown. |
| `worker_thread` | Worker thread UUID. A Claude worker's session id, recorded when it starts running; send it new instructions with `message send --worker`. A Codex worker's thread, recorded when the task ends, including lost tasks, if Homebased found a valid session id in the first 64 KiB of `output.log`. Omitted when unknown. |
| `exit_reason` | `null` while running, else the tagged payload (`exit`, `signal`, `cancelled`, `spawn_failed`). |
| `callback` | Delivery of the terminal event to the submitting thread: `pending`, `sending`, `waiting`, `sent`, or `failed`. `null` when this machine delivers no callback for the task: on the executor of a task submitted from another machine (ask the origin), and on a queue run, which reports through its job. Use `failed_events` for per-event callback failures. |
| `cancel_requested_at` | Set once `task cancel` ran. |
| `reports` | Worker reports with `seq`, `outcome`, `summary`, `reported_at`, and `notified_at` when `--notify` succeeded. A `waiting` report also has `waiting_on` and the worker's `notes`. |
| `evidence` | Task directory. |
| `output_log` | Path of the combined stdout and stderr of the child. |
| `last_event` | The event object already sent, or the one that will be sent. `null` while running with no interim event. |
| `pid` | Worker pid, for display only. Liveness is the lock, not the pid. |
| `timeout_secs` | Output-inactivity timeout in seconds. Not remaining execution budget. |
| `check_timeout` | `pending` or `sent` for the inactivity reminder. |
| `created_at` | Insert time. |
| `updated_at` | Last row change. For a terminal task this is the finish time. |

On the origin, a held task, or one that ended before it started, shows `availability` `held` or `not_started`, its `submission` phase, and the origin's own `last_event`. It has no log; `task log` returns `task_not_started`.

Fleet-aware `show` results can also include `origin_machine`, `execution_machine`, `found_on`, `availability`, `submission`, `last_accepted_seq`, `last_settled_seq`, `last_update`, and `failed_events`. `submission` describes durable acceptance; it is not process status. A callback failure does not change the process status.

When the task is not stored locally, `task show` checks known Fleet machines in parallel. It follows any saved origin route to its execution machine. A remote result also has `origin_machine`, `execution_machine`, `found_on`, and `availability`. The executor record is the source for process state, reports, log path, and evidence. If an origin route is known but its executor is offline, `show` returns the cached submission and last known state with an unavailable marker. The cached response does not invent a process status for an unknown submission.

The lookup scope is the known Fleet at the time of the request. If any machine cannot give a definitive answer, the CLI returns the retryable `cluster_lookup_incomplete` error with the unchecked machine UUIDs. It returns `task_not_found` only when every machine in that scope gives a definitive negative answer. A known route whose executor cannot be reached is not `task_not_found`.

Use `homebased task followup <id> --message "..."` to resume a terminal Codex
worker. Run it on the task's origin or execution machine. Only one follow-up
can resume a thread at a time. It reads `worker_thread` from the executor task
view, including for a Fleet task. See [submit.md](submit.md) for its options
and limits.

## Log

```bash
homebased task log <id> --tail 100
homebased --json task log <id>       # {"id", "log", "truncated"}
```

The local daemon reads `output.log` on the execution machine. For a remote task, it gets the log from the executor over Fleet. The log can be empty while the child has not written anything yet. `--tail` keeps at most 5000 lines and reads at most 1 MiB of log bytes. A longer line can return only its end. `truncated` is true when earlier lines or bytes were dropped. If the executor is offline or the log has been removed, the CLI returns `task_unavailable`. A task prevented before it started has no log and returns `task_not_started`.

## Usage

Claude workers run without session persistence, so their tokens never reach `~/.claude/projects`. Homebased reads Claude Code's own accounting from each finished Claude worker's `output.log` and keeps it per task.

```bash
homebased --json task usage --since 7d                   # totals, every grouping, and every task
homebased --json task usage --since 24h --thread <uuid>  # one thread's workers
homebased task usage --by thread                         # table by model (default), day, thread, or task
```

The JSON has `since`, `totals`, `by_model`, `by_day`, `by_thread`, and `tasks`. Totals and groups have `tasks`, `partial_tasks`, `input_tokens`, `output_tokens`, `cache_read_tokens`, `cache_write_tokens`, and `cost_usd`. A group's `key` is the model id, the `YYYY-MM-DD` submission day in the machine's time zone, or the thread id. Each task has `task`, `name`, `thread`, `cwd`, `evidence`, `status`, `created_at`, and `usage`: `complete`, `turns`, the same token fields, and `models` with one entry per model. Tasks count by submission time. `cost_usd` is the list-price cost Claude Code reports, not subscription usage.

`complete: false` means a run stopped before Claude Code wrote its final accounting, such as a cancelled or lost worker. Its input and cache tokens are exact, its output tokens are a lower bound, and it adds no cost.

To find where usage went, read this together with the session transcripts:

- `thread` is the submitter: a Claude Code session, in `~/.claude/projects/*/<thread>.jsonl`, or a Codex thread, in `~/.codex/sessions/**/rollout-*-<thread>.jsonl`.
- A Claude worker's own session id is its task id, so a task whose `thread` is another task's `task` was submitted by that worker.
- Worker tokens appear only here, never in those transcripts, so adding the two never counts a token twice.
- `<evidence>/prompt.txt` says what the worker was asked to do.

Usage covers Claude workers that ran on this machine. Run the command on each Fleet machine that executes workers. Codex, Grok, OpenCode, task, and container workloads have no usage here.

The terminal event of a Claude worker that ran on its submitting machine carries the same `usage` object, and `task show` on the executing machine has it in `last_event`.

## Dashboard

The daemon serves a read-only HTTP dashboard only when `--web-listen` / `HOMEBASED_WEB_LISTEN` is a host:port. Open that URL in a browser to see the tasks of every Fleet machine, their status, and their log tail without an agent turn.

```bash
homebased --json daemon status       # "web" holds the URL, or null when the dashboard is off or the socket is down
curl -s http://main:7677/v1/tasks
curl -s "http://main:7677/v1/fleet/tasks?status=queued,running"
curl -s "http://main:7677/v1/tasks/<id>/log?tail=200"
```

The listener answers `GET /v1/status`, `GET /v1/tasks`, `GET /v1/fleet/tasks`, `GET /v1/tasks/<id>`, `GET /v1/usage?since=<RFC 3339>&thread=<uuid>`, which returns the `task usage` JSON, and `GET /v1/tasks/<id>/log?tail=<lines>`, which returns `{"id", "log", "truncated"}`. The dashboard's usage page shows the same report. `/v1/tasks` lists only this machine. `/v1/fleet/tasks` takes the same `status` and `thread` filters and adds every Fleet peer: `machines` has each machine's name, Homebased daemon version, location, and whether its task read succeeded, and `tasks` has one entry per task with the machine that runs it. The local daemon reports its package version; a peer reports the version from its last identity probe, including when its task read fails. A peer that does not answer is listed as `unavailable` with a reason, and the tasks of the other machines stay. Task submit and cancel are refused there with 405; they belong to the Unix socket. Queue reads and move, cancel, and release are available on the web listener. Job submit and resource registration are socket-only. See [setup.md](setup.md) for `--web-listen`.

A queue run can be `preempted` while its job waits to resume. Use [resource-queue.md](resource-queue.md) to inspect the job and its run history. Run task IDs cannot be used in `after`.

## Task directory

`evidence` points at `<home>/tasks/<id>/` on the execution machine. For a remote task, `<home>` is the executor's state directory. The origin keeps its callback log and delivery lock in its own task directory. For a local task, origin and executor share one machine.

| File | Content |
| --- | --- |
| `prompt.txt` | Agent only: the submitted prompt, byte for byte. |
| `prompt.trailer.txt` | Agent only: the reporting trailer, when enabled. |
| `prompt.feed.txt` | Agent only: what the agent actually received. |
| `output.log` | Child stdout and stderr. |
| `exit.json` | Written by the worker parent at exit. Absent for `lost` tasks. |
| `callback.log` | Output of the last `codex queue` attempt, or the Claude Code session socket of the last attempt. |
| `delivery.lock` | Serializes callback delivery across daemon restarts. |
| `runner.lock` | Liveness lock. Held while the worker parent is alive. |
| `worker.log` | Stderr of the worker parent. Explains a worker that stopped before `exit.json`, such as a `lost` task that never started its child. |

`<home>` is `--home`, else `HOMEBASED_HOME`, else `$XDG_STATE_HOME/homebased`, else `~/.local/state/homebased`. The SQLite database is `<home>/homebased_v1.sqlite` and is the source of truth; do not edit it.

## Cancel

```bash
homebased --json task cancel <id>
```

Sends SIGTERM to the worker, which forwards it to the child's process group, waits for the group to disappear, and sends SIGKILL after 10 seconds if descendants remain. The exit event arrives as `TASK_CANCELLED`. Cancelling a terminal task exits 0 and changes nothing. A queued task with no worker yet is cancelled directly.

Cancelling any run of a parked chain cancels the run that owns the work now: its held continuation before it starts, or the continuation that already runs. The response `id` names that run. The chain then ends `cancelled`.

Cancelling a held task on its origin cancels it before it starts, sends `TASK_CANCELLED` with `cancel_reason` `requested`, and cancels the tasks held on it. Run it on the machine that accepted the submit; another machine gets a `usage` error. Once a held remote task has begun to start, cancel follows the remote path below.

When the task is not local, `task cancel` looks up its origin and executor, then saves a cancellation request on the machine where you ran the command. The local daemon retries delivery after a network failure or restart. The JSON response has `delivery.state`: `pending` means the executor has not acknowledged the request; `delivered` means it has saved the request. The executor result can still be `pending_application`, so check task status to learn when the child has stopped. A terminal task keeps its actual result. A task with incomplete Fleet lookup returns `cluster_lookup_incomplete`; it is not reported as cancelled.
