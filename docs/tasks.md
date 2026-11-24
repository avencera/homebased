# Tasks

## Submit

Run a command with `task run`, or submit a JSON spec for an agent or container
workload. The local daemon can run the task on this machine or send it to a
Fleet machine. The submitting machine owns the Codex thread and returns events
to it. Tell the user the task id, and end the turn. Results arrive later as one
line: `HOMEBASED_EVENT {...}`. Do not poll a running task.

```sh
homebased --json daemon status                         # socket must be "up"
homebased task run --name "release build" -- cargo build --release
homebased --json task submit --spec "$spec" --dry-run  # validate, print child argv
homebased --json task submit --spec "$spec"            # submit to the local machine
```

`task run` builds a `task` spec from its flags: it runs the command after `--`
in the current directory (`--cwd` overrides), names it after the command unless
`--name` is given, and takes `--timeout`, `--machine`, `--after`,
`--wait-for-machine`, `--mem-peak`, `--priority`, `--gpu-visible`, `--no-envrc`,
`--dry-run`, and `--request-id`.

Always pass `--json` on data commands. Every JSON object carries `api_version: 1`. `--request-id` is an optional CLI flag for a stable retry identity; it is not a task field.

## Workloads

| Work | `workload.type` |
| --- | --- |
| Long commands: `cargo build`, test suites, CI watchers | `task` |
| A model must reason and produce a report | `agent` |
| Work in a pinned Docker image, such as checkpoint evaluation | `container` |

A `task` runs an argv array with no shell. A caller that needs shell syntax must request it, for example `["sh", "-lc", "..."]`. An `agent` runs Codex, Claude, Grok, or OpenCode with a prompt file. Prefer `task` unless a model must reason.

A `task` also gets the direnv environment of its `cwd`, the same variables a terminal opened there would have, such as key paths that a repository's `.envrc` exports for a release. Before the command starts, the executor runs the `direnv` found on the absolute entries of the task's `PATH` and loads only an `.envrc` (never a `.env`, even with direnv's `load_dotenv`) that direnv already allows for that directory; Homebased never runs `direnv allow`. The first line of `output.log` names the file and the variables it set, never their values. When an `.envrc` exists but does not load, because direnv has not allowed or has denied it, is not installed, or fails, the command runs without it and that line says why. Cancelling the task while the `.envrc` loads stops it and never starts the command. The `.envrc` runs with the task's GPU selection, so it sees only the devices the command may use. Variables Homebased sets, such as `HOMEBASED_TASK_ID`, take precedence. Set `"envrc": "ignore"` on the workload, or pass `task run --no-envrc`, to run without it. GPU queue job steps of type `task` load it too, except in a CI job with a `git` checkout, which runs without one: the clone holds the previous commit until the checkout wrapper runs. Agent and container workloads never load an `.envrc`: an agent keeps the environment it was configured with, since an `.envrc` could switch its account or API key, and a container gets only the variables its spec names. The program itself is resolved on the submit-time `PATH`, so a tool that only the `.envrc` adds to `PATH` needs an absolute path or `["sh", "-c", "..."]`.

A `container` runs an image pinned by digest, with typed fields for the entrypoint, arguments, GPUs, memory limit, user, working directory, mounts, and environment. Homebased builds the Docker calls itself: it creates the container, saves its ID, starts it, streams its logs to `output.log`, waits for it, and removes it. The task exit code is the container exit code. If the watching worker stops, the container keeps running and the daemon adopts it. Docker options that the fields do not model, such as privileged mode, are refused. See [submit.md](../.agents/skills/homebased/references/submit.md) for the fields.

Claude agent workloads use streaming JSON output by default, so `output.log` records progress during a turn. Haiku workloads (`haiku` or a `claude-haiku-*` model ID) also use `--autocompact 100k` by default; an explicit `--autocompact` in `extra_args` replaces that default. A caller can select a different Claude output format with `extra_args`; exact spellings of Homebased-managed standalone switches are reserved tokens there, so Homebased treats every exact match as that switch, not as another option's value, and emits each at most once. Homebased also drops, with their values, options that the agent would reject because Homebased already sets them: Codex `-C`/`--cd` and `-s`/`--sandbox` (Codex always runs with full access), Grok `--cwd`, and Codex or Grok `-m`/`--model` when the workload sets `model`. Other extra arguments keep their order and spelling.

OpenCode agent workloads use `opencode run --standalone` with JSON events and the prompt feed on stdin. `model` is optional and is passed as one provider-qualified value, such as `zai-coding-plan/glm-5.3-flash` or `provider/model#variant`. OpenCode gets full work-tool permissions for that child only. Homebased sets the child `PWD` to `cwd`, adds a generated primary agent, and leaves persistent OpenCode configuration unchanged. An inherited `OPENCODE_CONFIG_CONTENT` value may contain JSONC; Homebased preserves its unrelated settings and rejects malformed content or a generated-agent name collision. OpenCode extra arguments cannot replace the agent, working directory, model, prompt, session, server, or standalone mode.

## Spec

Submit specs use `api_version: 1` and a `workload` object. The required top-level `name` is a short goal label for the dashboard and events. Name the work, not the agent or the CLI. `thread` is the Codex thread or Claude Code session UUID that should receive events. Submit fills in an omitted `thread`: a Homebased worker gets its parent task's thread, and anyone else gets the calling session from `CODEX_THREAD_ID`, `CODEX_SESSION_ID`, or `CLAUDE_CODE_SESSION_ID`. Submitted from a plain terminal, or with `"thread": null`, a `task` or `container` workload runs without a thread: its events are recorded and shown by `task show` and the dashboard, but no session is woken. `cwd` is the host directory the child runs in, and it must already exist on the machine that runs the task; for a `container` it is a host path, not a path inside the container. Omit `machine` to run on the local machine. Set `machine` to a Fleet machine name to run there. The remote machine checks `cwd` on its own file system. For remote agent work, `prompt_file` must be an absolute path on the submitting machine; the CLI reads the file and sends its text.

`timeout` is an output-inactivity timer (default `1h`, minimum `30m`). Homebased resets it when `output.log` receives bytes. If the live child produces no output for the full timeout, Homebased sends `TASK_CHECK_DUE` and leaves the child running. Only explicit cancel, a signal, or process exit stops the child.

Codex agent specs may set `workload.resume_thread` to resume a Codex thread
with the new prompt. Claude workers discard their session at exit unless the
spec sets `workload.persist_session: true`; a Claude spec's `resume_thread`
forks such a saved session into the new task's session. Use `task followup` for
a finished Codex task, or a Claude task that saved its session, so Homebased
uses its recorded worker thread and keeps the task settings.

Agent example:

```json
{
  "api_version": 1,
  "name": "implement file browser",
  "cwd": "/path/to/project",
  "timeout": "30m",
  "workload": {
    "type": "agent",
    "agent": "claude",
    "model": "fable",
    "prompt_file": "/tmp/prompt.md"
  }
}
```

Task example:

```json
{
  "api_version": 1,
  "name": "cargo release build",
  "cwd": "/path/to/project",
  "timeout": "2h",
  "workload": {
    "type": "task",
    "command": ["cargo", "build", "--release"]
  }
}
```

To run the task on a Fleet machine, add `machine` at the top level of the
spec. This example is a remote task spec:

```json
{
  "api_version": 1,
  "name": "run cargo checks on code",
  "machine": "code",
  "cwd": "~/code/homebased",
  "timeout": "2h",
  "workload": {
    "type": "task",
    "command": ["cargo", "check"]
  }
}
```

Run a remote dry run to validate against the selected executor and see its
child argv. A dry run does not submit a task or store a request identity.

```sh
request_id="$(uuidgen)"
homebased --json task submit --spec remote.json --request-id "$request_id" --dry-run
homebased --json task submit --spec remote.json --request-id "$request_id"
```

Keep the same request UUID if the submit response is lost. Retry with that UUID;
do not make a new request UUID for the same intended task. Without
`--request-id`, the CLI creates a new one for each submit command. Local submits
use request UUIDs the same way, and the CLI retries a local submit whose outcome
is unknown, or that meets `daemon_busy`, for about 90 seconds.

## Start after other tasks

Set `after` to a list of task or GPU queue job UUIDs to start a task only when
those tasks and jobs succeed. The daemon that accepts the submit holds the task
and starts it itself, so the orchestrator does not have to be running when the
last dependency finishes.

```json
{
  "api_version": 1,
  "name": "phase b2: integrate",
  "cwd": "/path/to/project",
  "after": ["01a0b06f-306c-749e-aa9e-9e1a619ee915"],
  "workload": { "type": "task", "command": ["cargo", "test"] }
}
```

Each dependency must be a task or job submitted through the same daemon, local
or remote; held work can be a dependency too, so chains work. Success means
`TASK_SUCCEEDED`, or `JOB_SUCCEEDED` for a job; `JOB_FAILED` and
`JOB_CANCELLED` cancel the held task, and `JOB_PREEMPTED` does not. A job's
events come back to the machine that submitted it, so a GPU job on another
Fleet machine can feed a CPU task here. The submit returns `held` while a
dependency is pending, or `queued` when all of them already succeeded. When a
dependency ends any other way, the held task is cancelled before it starts and
its thread gets `TASK_CANCELLED` with a `cancel_reason`. A held task starts
with the environment and callback context saved at submit. `task cancel`
cancels a held task before it starts. Tasks held on a cancelled task are
cancelled too.

## Wait for an offline machine

A remote submit fails with `machine_unavailable` when its machine does not
answer. Set `wait_for_machine: true` to hold the task on the submitting daemon
instead, for a laptop that sleeps. Before each send, the daemon asks the
machine whether it takes the task now. The submit returns `queued` when the
machine takes it at once, and `held` otherwise; the daemon then asks again
each time the machine answers, and sends the task once it says yes.

A Mac with a battery says yes only after someone has used it for two minutes
without a break. A laptop also answers during a dark wake, a few seconds of
network with the lid closed, and would sleep again while the task ran; in a
dark wake, or with the display asleep from idleness, no one is using it, so
the task keeps waiting. Once someone uses the machine, the task starts within
about three minutes. A Mac without a battery, and any Linux host, takes the
task as soon as it answers. Set `power.waiting_tasks` to `"always"` or
`"when_in_use"` in the machine's config to choose; `homebased config
validate` shows the result:

```toml
[power]
waiting_tasks = "always"
```

An executor from a release without this question takes the task as soon as
it answers.

Set `coalesce` with `wait_for_machine` for work where only the newest request
matters, such as an update. Once a submit with a `coalesce` key is accepted,
every older task with the same key and machine that still waits on this daemon
is cancelled with `cancel_reason` `superseded`. A task whose launch already
began is left alone.

```json
{
  "api_version": 1,
  "name": "fleet update",
  "cwd": "~/",
  "machine": "mba",
  "wait_for_machine": true,
  "coalesce": "fleet-update",
  "workload": { "type": "task", "command": ["cmd", "fleet", "update", "--local"] }
}
```

`task list` on the submitting machine shows waiting tasks as `held`, and
`task show` adds `wait_for_machine` and `coalesce`. `task cancel` cancels a
waiting task before it starts. Only the submitting daemon holds the task, so
two machines that each submit with the same key both send theirs.

## GPUs are hidden from tasks

A `task` or `agent` runs with `CUDA_VISIBLE_DEVICES` set to empty, so a CPU
step that imports a CUDA library cannot take GPU memory from the GPU queue.
Set `"gpu": "visible"` in the spec, or pass `task run --gpu-visible`, to run
the command with `CUDA_VISIBLE_DEVICES` unset, so it sees every GPU. GPU queue
jobs are unchanged: each run sees the GPU its resource names.

## Long commands inside an agent worker

An agent worker must not wait in the foreground for a long command or detach
it: headless `claude -p` ends with its turn and kills what it started, so
Claude workers run with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`. The report
trailer tells every worker to submit the command as its own task, or as a GPU
queue job, instead, then report `waiting` with notes for later and exit:

```bash
homebased task report --outcome waiting --on <task-or-job-id> \
  --summary "<what is running and why>" --notes-file - <<'NOTES'
<what each outcome means and the next steps>
NOTES
```

The orchestrator gets `TASK_WAITING`, unless it sleeps on the worker (see
[Sleep until long work ends](t3.md#sleep-until-long-work-ends)). Homebased holds a
continuation until every named task ends and every named job sends its final
event (`JOB_SUCCEEDED`, `JOB_FAILED`, or `JOB_CANCELLED`), with any outcome,
then starts it with the notes: a
Codex continuation resumes the run's thread when one is known and receives
only the continuation block. Otherwise it starts a fresh session with the
original prompt plus the block. Other agents always start a fresh session
with the original prompt plus the block. The runs of one unit of work
form a chain of at most 20 runs. A task held `after` any run waits for the
chain's last run, and `task cancel` on any run cancels the run that owns the
work now. The worker, its origin, and every waited task must be on one
machine, and every waited job must be submitted from it; the job itself may
queue on another machine. Name a job by its job id, not by a run's task id.

`homebased task schema` prints the JSON Schema. The full field contract is in [`.agents/skills/homebased/references/submit.md`](../.agents/skills/homebased/references/submit.md).

## Inspect and cancel

```sh
homebased --json task list --status running,queued
homebased --json task show <id>
homebased --json task followup <id> --message "The missing detail is X. Continue and report."
homebased task log <id> --tail 200
homebased --json task cancel <id>
```

`task show` and `task list` include `worker_thread`. A Claude task records its
session id when it starts running. A Codex task records the thread from its
session header when it ends, whether it succeeds, fails, is cancelled, or is
lost.
`task followup` accepts `--message` or `--message-file`, and resumes only a
terminal Codex task with a recorded worker thread or a terminal Claude task
submitted with `persist_session: true`. Only one follow-up can resume a Codex
thread at a time. `resume_thread_busy` names the active task; wait for its
event before trying again. Each Claude follow-up forks the saved session, so
several can branch from one task.

Run `task followup` on the task's origin or execution machine. A request from
another machine returns `followup_wrong_machine`. Use `--request-id <uuid>` to
retry a follow-up after a lost response. Use
`--allow-other-thread` only when a worker must send events to a thread other
than its parent task's thread.

Task ids are full UUIDs. Prefix matching does not exist. `task list` shows tasks
stored on the local machine, including tasks held here with status `held`. `task show`, `task log`, and `task cancel` use the
local daemon and can find or operate on a task in the known Fleet. A show result
includes `origin_machine`, `execution_machine`, `found_on`, and `availability`.
When the executor is offline, show can return cached origin state. Log data
comes from the executor and is unavailable while that machine is offline. When
the dashboard is enabled, `daemon status` reports its URL.

Errors with `--json` use the versioned error envelope. For example,
`submission_outcome_unknown` means the executor may have accepted the task. Use
the same `--request-id` to resolve or retry. Without a retry, the origin daemon
asks the executor again about once a minute and saves its answer, so the task
stops counting as open work once the executor answers. `cluster_lookup_incomplete` means
one or more known peers could not be checked. Retry the lookup; the daemon only
returns `task_not_found` after every machine in the current known Fleet gives a
definitive negative result.

## Worker usage

Claude workers run without session persistence unless submitted with
`persist_session: true`, so their tokens usually never reach
`~/.claude/projects`. Homebased reads Claude Code's accounting from each
finished worker's `output.log` and keeps it per task:

```sh
homebased task usage --since 7d --by thread
homebased --json task usage --since 24h
```

The report covers workers that ran on this machine. The JSON has totals,
groups by model, day, and thread, and each task with its thread, cwd, task
directory, and tokens and list-price cost per model. A task's `thread` is the
Claude Code session or Codex thread that submitted it, so the report can be read
together with the session transcripts. A worker stopped before Claude Code's
final accounting is marked `complete: false`; its tokens and cost are lower
bounds. The terminal event of a Claude worker that ran on its submitting
machine carries the same `usage`, and the dashboard's usage page shows the
report. Tasks that ended before this was recorded get their usage when the
daemon starts.

## Events

Delivery is at-least-once. For new events, use `(task, seq)` to detect a
duplicate. Old events can lack `seq`; for those events, use `(task, event)`.
New event payloads also include `origin_machine` and `execution_machine`.
Only the origin machine sends `codex queue` to the original thread. It uses the
environment and directory captured when the task was submitted. The executor
runs the child and owns its process state, reports, and logs. A network outage
does not stop the child. The executor keeps unacknowledged events and retries
delivery. Callback delivery can repeat after a crash, so use the event identity
to ignore duplicates.

An event for a thread that is running a turn waits for that turn to end. Only
the events that need action before then steer into the running turn: the
alerts (`TASK_CHECK_DUE`, `TASK_MEMORY_REVIEW`, `JOB_ATTENTION`, `JOB_BLOCKED`,
`JOB_CHECK_DUE`, and the first `JOB_GPU_IDLE` of an idle episode), a
`--notify` report's `TASK_REPORTED`, `THREAD_CONTEXT_OVER`, `THREAD_STALLED`,
and `SERVICE_EXITED`.

| `event` | Meaning |
| --- | --- |
| `TASK_REPORTED` | Worker sent an interim report. The process is still running. |
| `TASK_CHECK_DUE` | No output for the full inactivity timeout. The child is still live. |
| `TASK_MEMORY_REVIEW` | For ten minutes, the part of this running task's `mem_peak` it has not allocated alone kept the first task waiting for memory from starting. Sent once per task. The child is still live and keeps its peak. |
| `TASK_WAITING` | An agent worker submitted long tasks or GPU queue jobs, reported `waiting` on them, and exited. Homebased continues the work in a new run, `continuation`, once every task and job in `waiting_on` ends. |
| `TASK_SUCCEEDED` | Exit 0 and last report was `succeeded`, or exit 0 with no report from a task, a container, or an agent without the report trailer. |
| `TASK_BLOCKED` | Last report was `blocked`. The process has exited. Answer and submit a new spec. There is no resume. |
| `TASK_FAILED` | Failed report, non-zero exit, signal, or spawn failure. Also an agent with the report trailer that exited 0 without reporting (`reason: "no_report"`). |
| `TASK_CANCELLED` | `task cancel` or `daemon stop --yes` ended it. With `cancel_reason`, a held task was cancelled before it started: a dependency ended, `task cancel` asked, or a newer task with the same `coalesce` key replaced it (`superseded`). |
| `TASK_LOST` | Worker disappeared without `exit.json`. |
| `THREAD_CONTEXT_OVER` | A watched thread's context passed the watch's limit while it ran no turn. Identified by `(watch, seq)`. With `compaction.outcome: "started"` on a thread other than the caller, only a running `homebased wait` gets it. `watch_ended` is true, with `end_reason: "once"`, when it ends a `--once` watch, which then sends no `THREAD_WATCH_ENDED`. |
| `THREAD_STALLED` | A thread watched with `--stalled-after` sat idle past the grace with nothing due to wake it and its work not done. Identified by `(watch, seq)`. It fires once per idle stretch. `watch_ended` is true, with `end_reason: "once"`, when it ends a `--once` watch. |
| `THREAD_WATCH_ENDED` | A watch's last event, unless a `--once` watch fired, with `reason`: `unwatched`, `archived`, `deleted`, `session_gone`, `no_context_window` (a percent watch on a thread that runs Claude Code), `finished` (the T3 thread stayed done and quiet for an hour), or `caller_gone` (the caller's T3 thread was archived, deleted, or stayed done and quiet for an hour). Only a running `homebased wait` gets `unwatched`, `finished`, and `caller_gone`. |
| `SERVICE_EXITED` | A service that was ready exited on its own, or its worker was lost. Identified by `service`; each service sends at most one. See [Services](services.md). |
