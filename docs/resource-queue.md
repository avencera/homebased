# Operate the GPU resource queue

Use the resource queue for all GPU work on a machine that has it. Each machine has one queue. Each resource runs one attempt at a time. Work outside the queue can use a GPU that Homebased considers free.

## Check resources and jobs

At daemon startup, Linux detects NVIDIA GPU indices with `nvidia-smi -L` and creates `gpu0`, `gpu1`, and so on. macOS gets `gpu0` without a device index. If `nvidia-smi` is not installed, Linux gets an unindexed `gpu0`. If an installed probe fails or returns no usable devices, Homebased logs a warning and waits until a later start to detect resources. This exclusive lane does not prove that a physical GPU is available. A later successful detection assigns the first device to a detected fallback and keeps its UUID. Detection waits if that fallback has a run in any phase, including cleanup or Attention. Manual registrations do not change.

```sh
homebased --json resource list
homebased --json resource jobs
```

Use human output for a quick view. Use `--json` for scripts and Attention IDs.

| Output | Meaning |
| --- | --- |
| Resource `ID`, `NAME`, `DEVICE` | Resource UUID, machine-local name, optional CUDA device index. |
| Resource `STATE`, `RUN / YIELD AGE` | Active run phase, task UUID, and elapsed seconds since a yield request. No run means idle. |
| Job `PRIORITY`, `POSITION` | Level and slot within that level. High serves before medium, then low. |
| Job `STATE`, `TARGET / NAME` | Job state, pinned resource or `any`, and submitted label. |

New jobs join the back of their level. Only higher-priority work can preempt. Equal-priority work waits. A pinned job waiting for a busy resource does not stop other jobs from using free resources.

Registration is local and socket-only. Add a lane only for a separate exclusive resource. Do not register another lane for a GPU that already has one. Names and device indices must be unique.

```sh
homebased --json resource register --name auxiliary
```

For an indexed GPU, add `--device <index>`. Homebased exports that index as `CUDA_VISIBLE_DEVICES` for host steps. For containers, Docker selects that host device with `--gpus device=<index>` and `CUDA_VISIBLE_DEVICES=0` selects the one exposed GPU inside the container. An unindexed resource removes that variable. Registration does not test the physical device.

## Inspect, move, or cancel

Set `job` to the full job UUID. Job show lists every run task UUID. Set `task` to one of these to read its output. Set `move` and `cancel` to separate new UUIDs before the first action. Save them for retries.

```sh
homebased --json resource job show "$job"
homebased task log "$task" --tail 200
homebased --json resource job move "$job" --priority high --front --operation-id "$move"
homebased --json resource job cancel "$job" --operation-id "$cancel"
```

Retry an action with the same operation UUID and unchanged arguments after a lost reply. A new action needs a new UUID. Without `--operation-id`, each CLI invocation creates its own UUID.

Use `--front` or `--back` within a level. Add `--priority low`, `medium`, or `high` to change levels. Priority alone joins the new level's back. `--before <job>` and `--after <job>` take the target's level. Do not combine placement flags. Moving a job does not let equal-priority work preempt.

Cancellation stops queued work directly. For an active job, it requests a stop and cleanup. The response does not prove that cleanup has finished. Check job show and resource list.

Add `--machine <Fleet-name-or-UUID>` to inspect another machine. Show, move, and cancel use the saved job authority when machine is absent. For a job submitted from another origin, specify its authority. Resource list, resource jobs, and release default to local.

The web listener serves queue reads and move, cancel, and release. Job submit and resource registration require the socket. Read the [agent queue guide](../.agents/skills/homebased/references/resource-queue.md) for specs, submission, events, and all CLI flags.

## Clear Attention after a machine check

`Attention` means Homebased could not finish cleanup safely. The resource stays held until a person checks the machine and releases that exact Attention UUID. Other resources can continue to serve work. A job result does not prove that its resource is clear.

If a same-user process started at or after the run's recorded workload start and its environment cannot be read, cleanup treats it as a suspect. It is never signalled without a readable run marker. Its scan is not empty. If it remains unreadable at the cleanup bound, the resource enters `Attention`; the cleanup message names its PID and kernel start time. Check that exact process on the authority machine. Unreadable processes that started before the run, and unreadable processes under another user, are ignored. If the workload start was not recorded, unreadable processes are ignored as before.

Cleanup cannot reliably find work with a cleared environment, work under another user, or Apple platform binaries in a new session. A successful cleanup does not prove that these processes are absent.

1. Read resource list with `--json`. Save the resource, task, and Attention UUID from the active run phase. Read job show for cleanup details and the last stop cause.
2. On the authority machine, inspect processes and GPU use. On Linux, read `nvidia-smi` output. On macOS, use Activity Monitor and inspect the run's processes. Inspect Docker containers for container runs.
3. Read the run logs. Find and stop remaining work from the run. Check for work passed to another user, session, server, service manager, or remote machine. Homebased cannot reliably attribute these escapes.
4. Confirm that the resource can safely run another job. Set `attention` to the exact UUID you checked. Set `release` to a saved new operation UUID. Release on that machine.

```sh
homebased --json resource release --attention "$attention" --operation-id "$release"
homebased --json resource list
```

A stale UUID cannot release a newer Attention. If `attention_not_found` occurs, refresh state. Check the machine again before releasing a different hold. Do not edit the database or release only because a notice arrived.

## Configure blocked notices

The first blocked queued job gets one `JOB_BLOCKED` notice per blocking episode. The default delay is 15 minutes behind a requested yield, or 30 minutes behind other blockers, including `wait` and Attention. The notice goes to the blocked job's thread. It never stops a run.

Put this table in `~/.config/homebased/config.toml`, or the file selected by `--config` or `HOMEBASED_CONFIG`. Values must be positive duration strings.

```toml
[resource.notify_blocked_after]
yield = "15m"
wait = "30m"
```

```sh
homebased --json config validate
```

Use `homebased daemon restart` to load changed settings. Let active jobs finish first. Do not use a raw service-manager stop. Output-inactivity notices are separate. Each job spec has `timeout`, default `1h`, minimum `30m`. `JOB_CHECK_DUE` also leaves the run alive.

## Find checkpoints and logs

`<home>` is `--home`, then `HOMEBASED_HOME`, then `$XDG_STATE_HOME/homebased`, then `~/.local/state/homebased`.

| Path | Content |
| --- | --- |
| `<home>/jobs/<job>/state/` | Checkpoints shared by steps and attempts. Container path: `/homebased/job`. |
| `<home>/tasks/<task>/output.log` | Combined stdout and stderr. Read with `task log`. |
| `<home>/tasks/<task>/exit.json` | Exit evidence when the worker writes it. |
| `<home>/tasks/<task>/worker.log` | Worker errors. |
| `<home>/tasks/<task>/control/yield` | Request for this exact run. Container path: `/homebased/run/yield`. |
| `<home>/jobs/<job>/delivery/` | Callback logs and delivery lock on the origin. |

Run directories and checkpoints live on the authority machine. Callback evidence lives on the origin. Each attempt has a new task UUID. A run task can be `preempted` while its job waits to resume. Use job show to inspect the whole job. Do not resubmit because one attempt was preempted.
