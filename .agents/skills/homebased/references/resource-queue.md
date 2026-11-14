# Run GPU work through the queue

Use the resource queue for any GPU work on a machine. Never run GPU work outside the queue on a machine that has it. Use `task submit` for work that does not use a GPU. An agent prepares the command and submits a job. A job cannot run an agent workload.

## Choose priority and preemption

| `priority` | Use when |
| --- | --- |
| `high` | A decision or person is blocked on the result now. |
| `medium` | The result is needed soon. |
| `low` | The result is nice to have and can wait. |

New jobs join the back of their level. Only a strictly higher level can preempt a run. Equal levels never preempt. A preempted job keeps its queue slot. Each machine has one queue and one exclusive lane per resource. A busy pinned resource does not stop jobs from using other free resources.

Every run can be preempted for a waiting job of a higher level, at any age, unless its job sets `"hold": true`. A held run keeps its resource once it is 30 minutes old; younger held runs are preempted like any other. Set `hold` only on a low or medium job whose step loses too much work if it is stopped late; high jobs are never preempted. When a person or decision is blocked behind a held run, run `resource job preempt` (see [Command reference](#command-reference)) instead of waiting or cancelling it.

Choose in this order:

1. Split the work into `steps`. Each successful step is a checkpoint, so a preemption repeats only the current step.
2. Use `yield` when the command polls `HOMEBASED_YIELD_FILE` between units of work and saves progress. A preempted `yield` run is asked to stop at its next checkpoint and resumes from it. A step labelled `yield` that never reads the file holds its GPU while higher-priority work waits; label it `restart` instead.
3. Use `restart` for everything else. A preempted `restart` run is killed and its current step runs again from the start.
4. Add `"hold": true` to keep the resource once the run is 30 minutes old.

`preempt` is an object with `mode`, `yield` or `restart`, and optional `hold`. Specs written for earlier releases may say `wait` or set `restart_within`: `wait` now stops like `restart`, `restart_within` is ignored, and submit prints a warning for both. A CI job still uses `{"mode": "wait"}` and is never preempted. A queue machine on an older release refuses a spec with `hold`: the submit fails with `cluster_protocol_incompatible`.

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
| `CUDA_VISIBLE_DEVICES` | Assigned host GPU index for host steps; `0` inside containers, where Docker exposes only the selected host GPU. Removed when the resource has no index. |
| `CUDA_DEVICE_ORDER` | `PCI_BUS_ID` whenever `CUDA_VISIBLE_DEVICES` is set, so the index means the same GPU as in `nvidia-smi`. |

`HOMEBASED_HOME` is installation configuration, not a run marker. Do not replace run variables. In containers, `/homebased/job` is writable and `/homebased/run` is read-only. Do not set `gpus`, mount at or under `/homebased`, or set `HOMEBASED_*` in the container spec. Homebased owns these values.

When the yield file appears, finish the current unit, save progress, and exit 75. Exit 0 completes the step. Exit 75 without a committed yield request fails the job. A yield request from the scheduler does not kill the run after a deadline; one from the lease guard (below) cancels the job if the step still runs 10 minutes later. Clean up all child work before exit.

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

Omit `machine` to use the local machine. Set it to a Fleet name or full machine UUID to use that machine's queue. Paths and executables must exist there. Remote runs use that machine's environment. Omit `resource` to use any of its resources. Set it to a resource name or full UUID to pin the job only when the machine has several GPUs and the work needs a particular one.

Leave `thread` out: submit fills in the calling session, or a worker's parent thread, as described in [submit.md](submit.md#1-thread-and-machine). `--allow-other-thread` lets a worker name another known thread. Save the job UUID before the first submit. Retry with the same UUID, unchanged spec, and the same `--after` list, from the same session, after a lost reply. Do not create another job for the same request.

Save this example as `job.json`. `/tmp` must exist on the target machine.

```json
{
  "api_version": 1,
  "name": "Checkpoint example",
  "cwd": "/tmp",
  "timeout": "1h",
  "priority": "low",
  "preempt": {"mode": "restart"},
  "steps": [
    {"type": "task", "command": ["sh", "-c", "printf 'first\\n' > \"$HOMEBASED_JOB_DIR/result\""]},
    {"type": "task", "command": ["sh", "-c", "cat \"$HOMEBASED_JOB_DIR/result\""]}
  ]
}
```

| Spec field | Rule |
| --- | --- |
| `api_version` | Required. Must be 1. |
| `thread`, `name`, `cwd` | `name` and `cwd` required: non-blank label, existing host directory, one cwd for all steps. `thread` is filled in by submit. |
| `priority`, `preempt` | Required. Use the choices above. |
| `timeout` | Output-inactivity notice per run. Default `1h`, minimum `30m`. Never stops work. |
| `machine`, `resource` | Optional target selectors. |
| `workload` or `steps` | Exactly one. One workload or 1 to 32 ordered steps. Each is `task` or `container`. |
| `gpu_idle` | Optional. `"off"` turns this job's GPU idle alerts off. `{"after": "10m", "util_below": 5}` sets its idle window (any positive duration), threshold (1 to 100 percent), or both. Omit it to use the machine's defaults. A queue machine on an older release cannot take it: the submit fails with `cluster_protocol_incompatible`, also for a job held for `after` when that machine is reachable at submit. A held job whose machine was unreachable then is retried after release until that machine updates. |
| `after` | Optional. 1 to 16 task or job UUIDs that this machine submitted. Each must succeed before the job joins the queue; a job succeeds at `JOB_SUCCEEDED`. Same as `--after`; both lists are merged. |

Task commands are argv arrays. Homebased does not add a shell. Use `sh -c` explicitly when needed. Container fields follow [submit.md](submit.md), with the reserved fields above. Unknown fields fail validation. Run task IDs cannot be dependencies in any `after` list. Name their job instead.

## GPU jobs contain only GPU work

Never put a CPU-only step in a GPU job. A GPU job runs prebuilt binaries only; build and prepare in a CPU `homebased task run` task and submit the job with `resource job submit --after <task-id>`.

A job holds its GPU from the first step to the last. Any CPU step leaves the GPU idle and keeps every other job waiting. One job held gpu0 for 3.5 minutes while it compiled PTX. Keep these out of the job and run them as ordinary CPU tasks:

- builds: `cargo build`, `cargo test` (build test binaries with `cargo test --no-run` and run the binary in the job), `cargo oxide`, `build-ptx.sh`, PTX and cubin compiles (`nvcc`, `ptxas`), `sass_mix.sh`, `make`, `pip install`, first-run kernel compilation
- data prep: downloads, decoding, tokenizing, packing, shuffling, writing shards
- CPU evaluation and scoring of a checkpoint or of model outputs
- reports, plots, and summaries
- uploads of checkpoints, datasets, or results

Saving a checkpoint that takes a few minutes can stay in the job. Chain the parts with `--after`: prepare in a CPU task, hold the GPU job after it, and hold the evaluation after the job:

```bash
job="$(uuidgen)"
prep="$(homebased --quiet task run --name "build trainer and pack data" -- ./prep.sh)"
homebased --json resource job submit --job-id "$job" --after "$prep" --spec job.json
homebased --json task run --name "score checkpoint" --after "$job" -- ./eval.sh
```

- The job gets its id at once. Your origin machine holds it out of the queue until every `--after` entry succeeds. `resource job show "$job"` reports `held` with each dependency's state, and `homebased wait --next "$job"` works right away.
- If an `--after` entry fails or is cancelled, the job is cancelled before it reaches the queue. It sends a `JOB_CANCELLED` event whose `held_ending` names that task or job, and it uses no GPU time. `resource job cancel` ends a held job the same way. `show` and `cancel` find a held job on its origin machine, so a `--machine` that names this machine or the job's queue machine still reaches it.
- The evaluation task starts once the job sends `JOB_SUCCEEDED`. `JOB_FAILED` or `JOB_CANCELLED` cancels it, and `JOB_PREEMPTED` leaves it held while the job queues again.
- Submit refuses a job whose step argv or `sh -c` script runs a build tool. Pass `--allow-build` only when the build itself needs the GPU, such as an autotuner that times kernels; the job is then submitted with the warning on stderr and in `warnings`. The check does not see inside wrapper scripts or other CPU work, so split that out yourself.

### The lease guard

The queue machine checks every 5 seconds whether any process of a running GPU task step holds a CUDA context on its GPU (NVML compute processes, not utilization). Container steps are not watched. After 5 minutes with none, it releases the lease: a `yield` job is asked to yield and is cancelled if still running 10 minutes later, and a `restart` job, or one from an earlier release that says `wait`, is killed and queued again. A job's second trip always cancels it. The ending event (`JOB_PREEMPTED` or `JOB_CANCELLED`, or the job's final event when the tripped step finished anyway) carries `lease_idle` with a `reason`. When you get one, move the CPU work into its own task, chain the GPU job with `--after`, and resubmit. Startup that compiles or loads for a minute or two before the first CUDA call is fine.

`resource jobs` flags a yield request pending for 10 minutes or more: the step is not polling `HOMEBASED_YIELD_FILE`. Fix the step or resubmit it as `restart`.

### Handle a `JOB_GPU_IDLE` alert

The queue machine watches each running attempt's GPU. Your thread gets `JOB_GPU_IDLE` when the GPU stays below 10% utilization for 5 minutes after it was busy, or for the first 10 minutes of an attempt that never got busy. Reminders follow 15 and 30 minutes later, then hourly, while it stays idle. Homebased never cancels or yields the job for this. When the alert arrives:

1. Read `homebased task log <task> --tail 50`, and the alert's `gpu_idle.process` (the run's busiest process, its state and CPU use) and `gpu_idle.last_output_at`.
2. If the run is stuck or broken, cancel it: `homebased --json resource job cancel <job>`.
3. If CPU work runs inside the lease (data prep, scoring, reports, uploads), cancel it and resubmit as a chain with that work in its own task (`--after`, above).
4. If the gap is short and expected, such as a large checkpoint write, snooze it for that long: `homebased --json resource job gpu-idle <job> --snooze 15m --reason "writing a 30 GB checkpoint"`. If the work really runs at low GPU use, turn alerts off with `--off --reason "..."`. Both are recorded with their reason in `resource job show`, and other agents read them before acting.

Do not snooze to hide CPU work in the lease. Other jobs wait for that GPU.

### Cancel a job that leaves its GPU idle

Any agent may cancel a GPU job, its own or another thread's, whose GPU has stayed near 0% utilization for 20 minutes or more with no snooze or off switch. The owner got its alert at 5 minutes. That job is doing CPU work, or is stuck, while others wait. Check before you cancel:

1. `homebased --json resource job show <job>` names the active run's resource and `gpu_uuid`. Its `gpu_idle.silence` is `watching` unless an agent snoozed alerts or turned them off, and `gpu_idle.controls` lists each control with its reason. Leave a snoozed job alone until the snooze ends. Respect an off switch unless its reason no longer holds. `gpu_idle.attempts` counts the attempt's idle, busy, and unknown 10-second buckets.
2. `nvidia-smi` on the queue machine, or the machine's Netdata GPU charts for the last 20 minutes, shows utilization near 0% on that GPU.
3. `homebased task log <run-task> --tail 50` shows what the job is doing, such as tokenizing, scoring, or uploading.

Then run `resource job cancel <job>`. A cancel removes no checkpoints: what the job saved in `HOMEBASED_JOB_DIR` stays in `<home>/jobs/<job>/state/` on the queue machine. The owner gets `JOB_CANCELLED`, moves the CPU part into its own task, and resubmits the GPU part. The new job gets a new, empty `HOMEBASED_JOB_DIR`, so have it read the old checkpoint from that path, or copy it in at the start of its first step.

### Read the GPU summary

Final callbacks (`JOB_SUCCEEDED`, `JOB_FAILED`, `JOB_CANCELLED`) carry `gpu_summaries`, one per attempt of the job, and `JOB_PREEMPTED` carries the attempt it ended. `resource job show` has the same summary on each run. Read each summary's `brief` first, then the fields you need. Before you resubmit or report a result, use it to judge whether the job used its GPU well:

- Low `util_avg` with low `idle_pct` means the GPU was busy only in bursts. The input pipeline is likely the bottleneck: add data-loader workers, prefetch, pin memory, or raise the batch size.
- High `idle_pct` means CPU work sat inside the lease: data prep, scoring, uploads, or a loop that never ended. Move that work into its own task before the next run ([GPU jobs contain only GPU work](#gpu-jobs-contain-only-gpu-work)).
- A large `first_busy_after_secs` means a slow start, such as loading or tokenizing data before the first step. Prepare that data in a CPU task and point the job at it.
- `mem_peak_mib` far below `mem_total_mib` leaves room for a larger batch. A peak near the total explains out-of-memory failures.
- `idle_alerts` and `snoozes` show whether the job was alerted and why it was silenced.

Each field has its own `covered_secs`, `coverage_pct`, and `source` (`netdata` or `nvidia-smi`). Treat a field with low coverage as a partial reading. The summary's `source` is `mixed` when its fields came from both. `status: "unavailable"` comes with a `reason`, such as a machine with GPU sampling off. The numbers are device-level: other work on the same GPU is included.

## Run GPU work on a Mac

On Apple silicon the GPU shares the machine's memory. One large Metal job, such as MLX or PyTorch MPS, can push the Mac into swap and stall everything else on it. A Mac has a single unindexed `gpu0` lane, and homebased only serializes jobs on it: it does not limit, reserve, or watch memory. The lease guard, idle alerts, and GPU summaries need an NVIDIA GPU UUID, so none of them run on a Mac, and the 20-minute idle-cancel rule above does not apply there. Until homebased handles memory itself, follow these rules on any macOS queue machine:

- Submit every Metal or MPS run as a job on the Mac's queue (`--machine <mac>`), never as a task or a direct command, so two large runs never overlap.
- Cap the job's memory inside the job, leaving at least 16 GB for the rest of the machine. In MLX, call `mx.set_memory_limit(<bytes>)` before the first allocation. For PyTorch MPS, prefix the step's argv with `env PYTORCH_MPS_HIGH_WATERMARK_RATIO=0.6` (or lower), since the spec has no `env` field. A run that passes the cap then fails with an out-of-memory error instead of making the Mac swap; lower the batch size and resubmit.
- Chain memory-heavy CPU work, such as data packing, large evaluations, or model conversion, with `--after` so it runs before or after the GPU job instead of beside it. When it cannot be ordered that way, submit it as a job on `gpu0` too. This exception to [GPU jobs contain only GPU work](#gpu-jobs-contain-only-gpu-work) holds only on a Mac, because there memory, not GPU time, is the contended resource and no idle check runs.
- To start only once memory is free, make the first step a gate: `["sh", "-c", "while [ \"$(sysctl -n kern.memorystatus_vm_pressure_level)\" != 1 ]; do sleep 30; done"]`. Level 1 is normal, 2 is warn, and 4 is critical. The gate holds the lane while it waits.

## Command reference

Use `homebased --json` before each command and parse the result. All results carry `api_version: 1`. Global flags are `--home <path>`, `--config <path>`, `--json`, `--quiet`, and `--help`. `--quiet` prints IDs and conflicts with `--json`. Full UUIDs are required.

| Command after `homebased --json` | Flags and purpose |
| --- | --- |
| `resource register --name <name>` | Optional `--device <index>`. Register on the local machine. Do not create two lanes for one physical GPU. |
| `resource label <resource> <label>` | Or `--clear` instead of a label. Optional `--machine <name-or-uuid>`. Set a display nickname of 1 to 40 characters. Specs and pins keep using the name. |
| `resource list` | Optional `--machine <name-or-uuid>`. Resources, active run phases, labels, and each indexed GPU's `gpu_uuid` from `nvidia-smi -L`. |
| `resource jobs` | Optional `--machine <name-or-uuid>`. Non-terminal jobs in serving order. |
| `resource schema` | Print JobSpec JSON Schema. No daemon needed. |
| `resource job submit --job-id <uuid> --spec <file>` | Use `--spec -` for stdin. Optional `--allow-other-thread`, and `--after <task-or-job-uuid>` (repeatable) to hold the job until those tasks and jobs succeed. A held job's result has `held` in place of the queue's answer. Machine comes from the spec. No dry-run flag. |
| `resource job show <job>` | Optional `--machine <name-or-uuid>`. Job, slot, progress, events, active run, run history with the `gpu_uuid` each attempt launched on, and `resources`: the pinned resource and each one a run held, with names and labels. A job held for its `after` entries, or ended before it queued, shows `held` instead: `state` (`held`, `cancelled`, or `failed`), each `after` entry as `{"task", "state"}`, plus `"kind": "job"` for a job, and the `reason` it ended. |
| `resource job move <job>` | Placement flags below. Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. |
| `resource job preempt <job>` | Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. Kill the running job's run, even a held one, and requeue it at the front of its level; it sends `JOB_PREEMPTED` and reruns its current step after the waiting higher-priority job. Refused with `preempt_refused` (exit 5) unless the run is executing (or already stopping for preemption, which it joins without changing how the run stops) and a waiting job of a higher level can start on its resource; `input.reason.kind` is `not_running`, `launching`, `preemption_pending` (another run is already stopping for higher-priority work; retry once it has), `cancelling`, `ci`, or `nothing_waiting`. Refused by a queue machine on an older release. |
| `resource job cancel <job>` | Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. Cancel queued work or stop and clean up the active run. |
| `resource job gpu-idle <job>` | Exactly one of `--snooze <duration>`, `--off`, or `--clear`. `--snooze` and `--off` require `--reason <text>`. Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. Snooze for any positive duration, or turn this job's GPU idle alerts off; `--clear` restores the spec's setting. The latest control wins. Never stops the job. Refused for a job still held for its `after` entries, and by a queue machine on an older release. |
| `resource release --attention <uuid>` | Optional `--operation-id <uuid>` and `--machine <name-or-uuid>`. Check the machine first. |

Move with `--front`, `--back`, or `--priority <low-or-medium-or-high>`. Front and back can include priority. Priority alone joins the new level's back. `--before <job>` and `--after <job>` take the target's level. An optional priority must match that level. Do not combine two placement flags or move relative to the same job.

Machine reads and release default to local. Show, move, preempt, cancel, and gpu-idle use the saved job authority when machine is absent. If you submitted from another origin, give the authority explicitly.

For scripts, save an operation UUID before move, preempt, cancel, gpu-idle, or release. Retry the exact command with the same `--operation-id`. New content needs a new operation UUID. Without the flag, the CLI makes one UUID for its own transport retries. A later CLI invocation makes a new one.

Get a run task UUID from job show, then read its log:

```sh
homebased --json resource job submit --job-id "$job" --spec job.json
homebased --json resource job show "$job"
homebased task log "$task" --tail 200
homebased --json resource job move "$job" --priority high --front --operation-id "$move"
homebased --json resource job cancel "$job" --operation-id "$cancel"
```

## Job events

When telling the user where a job runs, call the GPU by its label, or just name the machine when it has one GPU. Resource names such as `gpu0` belong in specs and commands.

Job callbacks start with `HOMEBASED_EVENT` followed by a space, like task callbacks. In a Claude Code session in a T3 Code thread, start `homebased wait --next "$job"` as a background Bash command right after submit to receive them in the live session ([t3.md](t3.md#waiting-in-a-claude-thread)). Deduplicate by `(job, seq)`, never by the run task UUID or event name. The sequence starts at 1. Delivery is at-least-once and ordered per job. Failed delivery stays pending and retries. Intermediate successful steps do not send callbacks. Run tasks have no ordinary task callback route.

| Event | Action |
| --- | --- |
| `JOB_SUCCEEDED` | Read run logs and verify the result. All steps succeeded. |
| `JOB_FAILED` | Inspect the run log and `process`. Later steps did not start. |
| `JOB_CANCELLED` | Cancellation took effect. Inspect cleanup if the resource remains held. With `held_ending`, the job never reached the queue. |
| `JOB_PREEMPTED` | The job is queued again. Let the queue resume it. Do not resubmit. |
| `JOB_ATTENTION` | Cleanup could not finish safely. Check the machine and release the hold if nothing from the run remains. |
| `JOB_BLOCKED` | A lower-level run or an Attention has held the job up past the threshold. Inspect the listed blockers. This is a notice, never a stop. |
| `JOB_CHECK_DUE` | Inspect the current run and its recent output. Inactivity does not stop it. |
| `JOB_GPU_IDLE` | The run's GPU has been near idle. Follow [Handle a `JOB_GPU_IDLE` alert](#handle-a-job_gpu_idle-alert). This is a notice, never a stop. |

| Callback field | Meaning |
| --- | --- |
| `api_version`, `event`, `job`, `seq`, `at` | Version 1, event name, job UUID, per-job sequence, recorded timestamp. |
| `thread`, `name`, `machine` | Receiving thread, job label, authority machine UUID. |
| `resource`, `task`, `run_number`, `step` | Resource UUID, task UUID for logs, attempt count, zero-based step. Each is explicitly `null` when no run exists, including blocked notices and never-started cancels. |
| `resource_name`, `resource_label` | Name and display label of the run's resource, such as `gpu0` and `RTX 5090`. `null` when no run exists, the resource has no label, or an older authority sent the event. |
| `run` | Optional object with `resource`, `task`, `run_number`, `step`. Absent when no run exists. |
| `process` | Optional tagged exit result. Uses the task exit forms in [events.md](events.md). |
| `attention` | Optional exact Attention UUID for release. |
| `blocked` | Only on blocked notices: `episode`, `blocked_since`, and `blockers`. |
| `gpu_idle` | Only on GPU idle alerts. `attempt` (run task), `episode`, `reminder` (0 for the first alert), `never_busy`, `gpu_uuid`, `idle_since`, `idle_secs`, `window_secs`, `idle_buckets` and `known_buckets` of the window, `util_below`, and the window's `util_peak_pct`, `power_peak_watts`, `power_peak_pct`, `memory_used_mib`, `memory_total_mib`, each `null` when unread. `last_output_at` is the run's last log output. `process` is the run's busiest process: `pid`, `command`, `state`, `cpu_pct` (one full core is 100), and `marked`, the count of the run's processes. `source` is `netdata` or `nvidia_smi`. `next_steps` repeats what to do. |
| `lease_idle` | On the `JOB_PREEMPTED` or `JOB_CANCELLED` that ends an attempt the lease guard stopped. `attempt` (run task), `gpu_uuid`, `idle_since`, `tripped_at`, `idle_after_secs`, `action` (`yield`, `restart`, or `cancel`), `trip` (1 for the job's first), `escalated_at` when an ignored yield became a cancel, and `reason`, one line that says what to do. Absent for jobs from a machine on an older release. See [The lease guard](#the-lease-guard). |
| `gpu_summaries` | On `JOB_SUCCEEDED`, `JOB_FAILED`, and `JOB_CANCELLED`, one summary per attempt on a GPU resource; on `JOB_PREEMPTED`, the attempt it ended. Absent when no attempt ran on one, or when the job came from a machine on the previous release. Each has `attempt` (run task), `run_number`, `step`, `status` (`ok` or `unavailable` with `reason`), `gpu_uuid`, `source` (`netdata`, `nvidia-smi`, or `mixed`), `attempt_secs`, `util_avg`, `power_pct_avg` (percent of the power limit), `mem_peak_mib`, `mem_total_mib`, `idle_pct`, `first_busy_after_secs`, `idle_alerts`, `snoozes` (each control in force during the attempt: `action`, `reason`, `at`, `until`), and `brief`, one line of the same. Each measured field is `{value, covered_secs, coverage_pct, source}` and is absent without samples. See [Read the GPU summary](#read-the-gpu-summary). |
| `held_ending` | Only when a job held for its `after` entries ended before it queued. `reason` is `dependency` (with the `task` or `job` and its `outcome`), `requested` (cancelled while held), or `refused` (with the queue's `code` and `message`; the event is `JOB_FAILED`). |

Each blocker has `kind`. A `run` blocker has `resource`, `job`, and `task`. An `attention` blocker has `resource` and `attention`. The first blocked queued job gets one notice per blocking episode, only when a lower-level run or an Attention holds it up; waiting behind runs of its own level or higher sends nothing. Default delay is 15 minutes behind a requested yield, or 30 minutes behind other blockers, including lower held runs and Attention. These delays never stop a run. See the [runbook](../../../../docs/resource-queue.md) for configuration.

## Attention and cleanup limits

After a run ends, Homebased cleans up its process group, marked processes, and container before it serves new work on that resource. If cleanup cannot finish safely, the resource enters `Attention`. Other resources can still serve work. Check the machine yourself: stop any remaining work from the run, then release the exact Attention ID. Do not release it only because a timeout passed or the job exited, and do not assume that exit means the GPU is free. Ask the user only when you cannot tell whether a remaining process belongs to the run. Report each release with the processes you checked and why they did not belong to the run.

If a same-user process started at or after the run's recorded workload start and its environment cannot be read, cleanup judges it by its ancestry. Environments are inherited, so cleanup walks up its parents to the nearest one whose environment it can read. If that ancestor does not carry the run marker, the process is not run work and is ignored. A sandboxed browser such as headless Chrome shows this pattern: its zygote and renderers hide their environment, while the browser process above them is readable. An ancestor that started before the run also clears it. The process stays a suspect when that ancestor carries the marker. It also stays a suspect when the walk reaches init, the daemon, a worker, or one of the daemon's same-user ancestors such as `systemd --user` first, since those adopt orphaned run work. The same holds when an ancestor's process entry cannot be read, or when it started after its child, which means its PID was reused. An ancestor whose environment cannot be read, or reads as empty, as Apple platform binaries do, is climbed past. A suspect is never signalled without a readable run marker. Its scan is not empty. If it remains at the cleanup bound, the resource enters `Attention`. The cleanup message names each suspect's PID, command name, and kernel start time, and names its process tree once: the marked ancestor, the root of the orphaned tree and what adopted it, or where the walk stopped. Check those processes on the authority machine. A suspect that the run did not start, with no GPU processes left in `nvidia-smi`, does not block a release. Unreadable processes that started before the run and unreadable processes under another user are ignored, as are processes under another user's daemon that predates the run. That last rule covers ssh logins: an `sshd-session` hides its environment, but its root `sshd` parents are not run work. If the workload start was not recorded, unreadable processes are ignored as before.

Cleanup cannot reliably find work that cleared its environment or dropped the run marker, or the sandboxed processes under it, work under another user, or Apple platform binaries in a new session. It cannot attribute work handed to an existing `tmux` or `screen` server, `launchctl`, `systemd-run`, `docker run`, `ssh`, or a browser automation server started outside the run. Do not use these escapes inside a script. Keep work in the run and preserve its marker. Use a container workload instead of starting Docker inside a host command.

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
