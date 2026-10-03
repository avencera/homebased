# Run GPU work through the queue

Use the resource queue for any GPU work on a machine. Never run GPU work outside the queue on a machine that has it. Use `task submit` for work that does not use a GPU. An agent prepares the command and submits a job. A job cannot run an agent workload.

## Choose priority and preemption

| `priority` | Use when |
| --- | --- |
| `high` | A decision or person is blocked on the result now. |
| `medium` | The result is needed soon. |
| `low` | The result is nice to have and can wait. |

New jobs join the back of their level. Only a strictly higher level can preempt a run. Equal levels never preempt. A preempted job keeps its queue slot. Each machine has one queue and one exclusive lane per resource. A busy pinned resource does not stop jobs from using other free resources.

Choose in this order:

1. Split the work into `steps`. Each successful step is a checkpoint. Use `wait` if a step cannot yield.
2. Use `yield` when the command can save progress and check for a yield request between units of work.
3. Use `restart` for short, idempotent work whose current step can safely run again from the start.
4. Use `wait` when none of these choices works. Higher-priority work waits for the step to finish.

`preempt` is an object with `mode`: `yield`, `restart`, or `wait`. For `yield` and `wait`, optional `restart_within` lets Homebased stop and repeat a young run for higher-priority work. Set it to the maximum amount of work you can safely discard, from `1m` through `24h`. Run age must be less than that duration. After the window, `yield` requests a checkpoint and `wait` waits. `restart` always permits restart and refuses this field. The window is permission to restart, not a timer that stops work.

## Checkpoint contract

Each attempt gets a new task UUID and run directory. Save checkpoints in `HOMEBASED_JOB_DIR`. That directory stays the same across attempts and steps. Completed steps do not run again. A restart repeats the current step. A yield resumes that step with `HOMEBASED_RESUME=1`. Read your checkpoint before doing more work.

| Run variable | Value |
| --- | --- |
| `HOMEBASED_TASK_ID` | This attempt's task UUID. Cleanup uses this process marker. |
| `HOMEBASED_JOB_ID` | Stable job UUID. |
| `HOMEBASED_JOB_DIR` | Writable checkpoint directory. On the host: `<home>/jobs/<job>/state`. In a container: `/homebased/job`. |
| `HOMEBASED_RUN_NUMBER` | Attempt count across the job, starting at 1. |
| `HOMEBASED_STEP_INDEX` | Step index, starting at 0. |
| `HOMEBASED_RESUME` | `1` after a yield of this step, otherwise `0`. |
| `HOMEBASED_YIELD_FILE` | File that appears when this run must yield. Host: `<home>/tasks/<task>/control/yield`. Container: `/homebased/run/yield`. |
| `HOMEBASED_RESOURCE` | Assigned resource name. |
| `CUDA_VISIBLE_DEVICES` | Assigned GPU index when the resource has one. Removed when it has no index. |

`HOMEBASED_HOME` is installation configuration, not a run marker. Do not replace run variables. In containers, `/homebased/job` is writable and `/homebased/run` is read-only. Do not set `gpus`, mount at or under `/homebased`, or set `HOMEBASED_*` in the container spec. Homebased owns these values.

When the yield file appears, finish the current unit, save progress, and exit 75. Exit 0 completes the step. Exit 75 without a committed yield request fails the job. A yield request does not kill the run after a deadline. Clean up all child work before exit.

This Python example stores the next unit after each unit. Replace the `print` with one finite unit of GPU work. Keep checkpoint writes atomic.

```python
import os
from pathlib import Path

state = Path(os.environ["HOMEBASED_JOB_DIR"])
checkpoint = state / "next"
next_unit = int(checkpoint.read_text()) if checkpoint.exists() else 0
for unit in range(next_unit, 3):
    print(unit, flush=True)
    temporary = state / "next.tmp"
    temporary.write_text(str(unit + 1))
    temporary.replace(checkpoint)
    if Path(os.environ["HOMEBASED_YIELD_FILE"]).exists():
        raise SystemExit(75)
```

The same contract works in a shell command:

```sh
next=0
if [ -f "$HOMEBASED_JOB_DIR/next" ]; then
  next=$(cat "$HOMEBASED_JOB_DIR/next")
fi
while [ "$next" -lt 3 ]; do
  echo "$next"
  next=$((next + 1))
  printf '%s\n' "$next" > "$HOMEBASED_JOB_DIR/next.tmp"
  mv "$HOMEBASED_JOB_DIR/next.tmp" "$HOMEBASED_JOB_DIR/next"
  if [ -f "$HOMEBASED_YIELD_FILE" ]; then
    exit 75
  fi
done
```

## Target and submit a job

Omit `machine` to use the local machine. Set it to a Fleet name or full machine UUID to use that machine's queue. Paths and executables must exist there. Remote runs use that machine's environment. Omit `resource` to use any of its resources. Set it to a resource name or full UUID to pin the job.

Get the receiving `thread` as described in [submit.md](submit.md). A worker must use its parent's thread. `--allow-other-thread` permits another known thread, but does not bypass the known-thread check. Save the job UUID before the first submit. Retry with the same UUID and unchanged spec after a lost reply. Do not create another job for the same request.

Save this example as `job.json`. Replace `thread` with your receiving thread UUID. `/tmp` must exist on the target machine.

```json
{
  "api_version": 1,
  "thread": "11111111-1111-4111-8111-111111111111",
  "name": "Checkpoint example",
  "cwd": "/tmp",
  "timeout": "1h",
  "priority": "low",
  "preempt": {"mode": "wait", "restart_within": "1m"},
  "resource": "gpu0",
  "steps": [
    {"type": "task", "command": ["sh", "-c", "printf 'first\\n' > \"$HOMEBASED_JOB_DIR/result\""]},
    {"type": "task", "command": ["sh", "-c", "cat \"$HOMEBASED_JOB_DIR/result\""]}
  ]
}
```

| Spec field | Rule |
| --- | --- |
| `api_version` | Required. Must be 1. |
| `thread`, `name`, `cwd` | Required. Thread UUID, non-blank label, existing host directory. One cwd for all steps. |
| `priority`, `preempt` | Required. Use the choices above. |
| `timeout` | Output-inactivity notice per run. Default `1h`, minimum `30m`. Never stops work. |
| `machine`, `resource` | Optional target selectors. |
| `workload` or `steps` | Exactly one. One workload or 1 to 32 ordered steps. Each is `task` or `container`. |

Task commands are argv arrays. Homebased does not add a shell. Use `sh -c` explicitly when needed. Container fields follow [submit.md](submit.md), with the reserved fields above. Unknown fields fail validation. Job specs do not support `after`. Submit once the inputs are ready. Run task IDs cannot be dependencies in a task spec's `after` list.

## Command reference

Use `homebased --json` before each command and parse the result. All results carry `api_version: 1`. Global flags are `--home <path>`, `--config <path>`, `--json`, `--quiet`, and `--help`. `--quiet` prints IDs and conflicts with `--json`. Full UUIDs are required.

| Command after `homebased --json` | Flags and purpose |
| --- | --- |
| `resource register --name <name>` | Optional `--device <index>`. Register on the local machine. Do not create two lanes for one physical GPU. |
| `resource list` | Optional `--machine <name-or-uuid>`. Resources and active run phases. |
| `resource jobs` | Optional `--machine <name-or-uuid>`. Non-terminal jobs in serving order. |
| `resource schema` | Print JobSpec JSON Schema. No daemon needed. |
| `resource job submit --job-id <uuid> --spec <file>` | Use `--spec -` for stdin. Optional `--allow-other-thread`. Machine comes from the spec. No dry-run flag. |
| `resource job show <job>` | Optional `--machine <name-or-uuid>`. Job, slot, progress, events, active run, and run history. |
| `resource job move <job>` | Placement flags below. Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. |
| `resource job cancel <job>` | Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. Cancel queued work or stop and clean up the active run. |
| `resource release --attention <uuid>` | Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. A person must inspect the machine first. |

Move with `--front`, `--back`, or `--priority <low-or-medium-or-high>`. Front and back can include priority. Priority alone joins the new level's back. `--before <job>` and `--after <job>` take the target's level. An optional priority must match that level. Do not combine two placement flags or move relative to the same job.

Machine reads and release default to local. Show, move, and cancel use the saved job authority when machine is absent. If you submitted from another origin, give the authority explicitly.

For scripts, save an operation UUID before move, cancel, or release. Retry the exact command with the same `--operation-id`. New content needs a new operation UUID. Without the flag, the CLI makes one UUID for its own transport retries. A later CLI invocation makes a new one.

Get a run task UUID from job show, then read its log:

```sh
homebased --json resource job submit --job-id "$job" --spec job.json
homebased --json resource job show "$job"
homebased task log "$task" --tail 200
homebased --json resource job move "$job" --priority high --front --operation-id "$move"
homebased --json resource job cancel "$job" --operation-id "$cancel"
```

## Job events

Job callbacks start with `HOMEBASED_EVENT` followed by a space, like task callbacks. Deduplicate by `(job, seq)`, never by the run task UUID or event name. The sequence starts at 1. Delivery is at-least-once and ordered per job. Failed delivery stays pending and retries. Intermediate successful steps do not send callbacks. Run tasks have no ordinary task callback route.

| Event | Action |
| --- | --- |
| `JOB_SUCCEEDED` | Read run logs and verify the result. All steps succeeded. |
| `JOB_FAILED` | Inspect the run log and `process`. Later steps did not start. |
| `JOB_CANCELLED` | Cancellation took effect. Inspect cleanup if the resource remains held. |
| `JOB_PREEMPTED` | The job is queued again. Let the queue resume it. Do not resubmit. |
| `JOB_ATTENTION` | Cleanup could not finish safely. Ask a person to inspect the machine. |
| `JOB_BLOCKED` | Inspect the listed blockers. This is a notice, never a stop. |
| `JOB_CHECK_DUE` | Inspect the current run and its recent output. Inactivity does not stop it. |

| Callback field | Meaning |
| --- | --- |
| `api_version`, `event`, `job`, `seq`, `at` | Version 1, event name, job UUID, per-job sequence, recorded timestamp. |
| `thread`, `name`, `machine` | Receiving thread, job label, authority machine UUID. |
| `resource`, `task`, `run_number`, `step` | Resource UUID, task UUID for logs, attempt count, zero-based step. Each is explicitly `null` when no run exists, including blocked notices and never-started cancels. |
| `run` | Optional object with `resource`, `task`, `run_number`, `step`. Absent when no run exists. |
| `process` | Optional tagged exit result. Uses the task exit forms in [events.md](events.md). |
| `attention` | Optional exact Attention UUID for release. |
| `blocked` | Only on blocked notices: `episode`, `blocked_since`, and `blockers`. |

Each blocker has `kind`. A `run` blocker has `resource`, `job`, and `task`. An `attention` blocker has `resource` and `attention`. The first blocked queued job gets one notice per blocking episode. Default delay is 15 minutes behind a requested yield, or 30 minutes behind other blockers, including `wait` and Attention. These delays never stop a run. See the [runbook](../../../../docs/resource-queue.md) for configuration.

## Attention and cleanup limits

After a run ends, Homebased cleans up its process group, marked processes, and container before it serves new work on that resource. If cleanup cannot finish safely, the resource enters `Attention`. Other resources can still serve work. A person must inspect the machine, stop remaining work, and release the exact Attention ID. An agent must not release it from a timeout or assume that exit means the GPU is free.

Cleanup cannot reliably find work with a cleared environment, work under another user, or Apple platform binaries in a new session. It cannot attribute work handed to an existing `tmux` or `screen` server, `launchctl`, `systemd-run`, `docker run`, or `ssh`. Do not use these escapes inside a script. Keep work in the run and preserve its marker. Use a container workload instead of starting Docker inside a host command.

Task entry points named `tmux`, `screen`, `launchctl`, `systemd-run`, `docker`, `podman`, `nerdctl`, `ssh`, or `mosh` are refused. The check includes resolved symlink targets. A script can still call these tools internally, so this check does not make such a script safe.

## Errors

| Code | Exit | Do this |
| --- | --- | --- |
| `invalid_queue_input` | 2 | Fix the full UUID, level, resource name, step count, or restart window. |
| `move_refused` | 2 | Give one valid placement. Check target existence, state, and level. |
| `job_not_found` | 3 | Check the full job UUID and authority machine. |
| `resource_not_found` | 3 | Read resource list on the target machine. |
| `attention_not_found` | 3 | Refresh resource list. Do not release a different Attention without another machine check. |
| `job_terminal` | 5 | Read the result. Submit a new job only for new work. |
| `job_conflict` | 5 | Restore the original spec for this job UUID. Use a new UUID for a different job. |
| `operation_conflict` | 5 | Restore the original operation content. Use a new UUID for a new action. |
| `resource_conflict` | 5 | Use the existing resource. Names and device indices must be unique. |
| `stale_run` | 5 | Refresh job and resource state. Do not act on an old run. |
| `internal` | 1 | Keep the error and state evidence. Ask the operator to check it. Do not edit the database. |

For `invalid_spec`, `invalid_cwd`, `executable_missing`, thread errors, daemon errors, and Fleet errors, use [errors.md](errors.md). Fix the cause before retry. After an uncertain submit, keep the same job UUID and spec. After an uncertain mutation, keep the same operation UUID and request.
