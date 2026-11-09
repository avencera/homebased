# Operate the GPU resource queue

Use the resource queue for all GPU work on a machine that has it. Each machine has one queue. Each resource runs one attempt at a time. Work outside the queue can use a GPU that Homebased considers free.

## Keep GPU jobs to GPU work

A job holds its GPU from its first step to its last, so any CPU step idles the GPU while other jobs wait. Run builds, data prep (downloads, tokenizing, packing, shards), CPU evaluation and scoring, reports, and uploads as ordinary tasks, and chain them to the job with `--after`. A checkpoint save of a few minutes can stay in the job.

```sh
job="$(uuidgen)"
prep="$(homebased --quiet task run --name "build trainer and pack data" -- ./prep.sh)"
homebased --json resource job submit --job-id "$job" --after "$prep" --spec job.json
homebased --json task run --name "score checkpoint" --after "$job" -- ./eval.sh
```

The job stays out of the queue until the prep task succeeds, and the evaluation starts at `JOB_SUCCEEDED`. Submit warns when a step runs a build tool. It does not detect other CPU work.

Homebased watches each running attempt's GPU and sends the job's thread a `JOB_GPU_IDLE` alert when the GPU stays near idle for 5 minutes (see [GPU idle alerts](#gpu-idle-alerts)). The alert only notifies. The owner decides whether to cancel, split the CPU work out, or snooze. Any agent or operator may cancel a GPU job whose GPU has stayed near 0% utilization for 20 minutes or more with no snooze or off switch. Confirm it first: `resource job show` names the active run's resource and `gpu_uuid`, and its `gpu_idle` section shows any snooze or off switch with its reason; `nvidia-smi` or the machine's Netdata GPU charts show that GPU's utilization over the last 20 minutes; and `task log <run-task> --tail 50` shows what the job is doing. Cancelling removes no checkpoints, and `<home>/jobs/<job>/state/` stays on the queue machine. The owner then moves the CPU part into its own task and resubmits the GPU part, pointing it at the old checkpoint, since the new job starts with an empty `HOMEBASED_JOB_DIR`.

## Check resources and jobs

At daemon startup, Linux detects NVIDIA GPU indices with `nvidia-smi -L` and creates `gpu0`, `gpu1`, and so on. macOS gets `gpu0` without a device index. If `nvidia-smi` is not installed, Linux gets an unindexed `gpu0`. If an installed probe fails or returns no usable devices, Homebased logs a warning and waits until a later start to detect resources. This exclusive lane does not prove that a physical GPU is available. A later successful detection assigns the first device to a detected fallback and keeps its UUID. Detection waits if that fallback has a run in any phase, including cleanup or Attention. Manual registrations keep their names and indices.

The same `nvidia-smi -L` output names each GPU's UUID, such as `GPU-6f3c1d2e-…`. Each detection that lists devices gives every indexed resource, manual ones included, the UUID listed at its index, or none when the index is not listed. A resource registered later takes the UUID from the last listing. A machine that never had `nvidia-smi` has no UUIDs. A start without `nvidia-smi` and a probe that fails list no devices, so both keep the saved ones. Each run attempt keeps the UUID its resource had when it launched, so a later card swap does not rewrite history.

```sh
homebased --json resource list
homebased --json resource jobs
```

Use human output for a quick view. Use `--json` for scripts and Attention IDs.

| Output | Meaning |
| --- | --- |
| Resource `ID`, `NAME`, `DEVICE` | Resource UUID, machine-local name, optional CUDA device index. |
| Resource `STATE`, `RUN / YIELD AGE` | Active run phase, task UUID, and elapsed seconds since a yield request. No run means idle. |
| Resource `GPU UUID` | UUID of the GPU at the device index from the last detection, or `-`. |
| Resource `LABEL` | Optional nickname, or `-`. |
| Job `PRIORITY`, `POSITION` | Level and slot within that level. High serves before medium, then low. |
| Job `STATE`, `TARGET / NAME` | Job state, pinned resource or `any`, and submitted label. |

New jobs join the back of their level. Only higher-priority work can preempt. Equal-priority work waits. A pinned job waiting for a busy resource does not stop other jobs from using free resources.

Registration is local and socket-only. Add a lane only for a separate exclusive resource. Do not register another lane for a GPU that already has one. Names and device indices must be unique.

```sh
homebased --json resource register --name auxiliary
```

For an indexed GPU, add `--device <index>`. Homebased exports that index as `CUDA_VISIBLE_DEVICES` for host steps, with `CUDA_DEVICE_ORDER=PCI_BUS_ID` so CUDA numbers GPUs the way `nvidia-smi` and Docker do; CUDA's default order puts the fastest GPU first, which can select a different card on a host with mixed GPUs. For containers, Docker selects that host device with `--gpus device=<index>` and `CUDA_VISIBLE_DEVICES=0` selects the one exposed GPU inside the container. An unindexed resource removes `CUDA_VISIBLE_DEVICES`. Registration does not test the physical device.

## Give a resource a nickname

A label is a display nickname, such as `RTX 5090` for `gpu0`. `resource list` and the queue page show it next to the name. The dashboard's GPU queues panel shows only the label, and only on a machine with more than one resource. Job callbacks carry the run's `resource_name` and `resource_label` as they were when the event was recorded, and `resource job show` lists the job's resources with their labels, so agents can name a GPU the way you labeled it. Specs, pins, and commands keep using the name, so relabeling never touches queued or running work. A label has 1 to 40 characters with no control characters. Surrounding spaces are trimmed. Labels need not be unique.

```sh
homebased resource label gpu0 "RTX 5090"
homebased resource label gpu0 --clear
```

Name the resource by name or UUID. Add `--machine <Fleet-name-or-UUID>` to label a resource on another machine. A peer on an older release refuses the request. In the dashboard, hover a resource name on the queue page and click the pencil. Press Enter to save or Escape to cancel. Saving an empty field clears the label.

## Inspect, move, or cancel

Set `job` to the full job UUID. Job show lists every run task UUID, with the GPU UUID each attempt launched on (`gpu_uuid` in `--json`). Set `task` to one of these to read its output. Set `move` and `cancel` to separate new UUIDs before the first action. Save them for retries.

```sh
homebased --json resource job show "$job"
homebased task log "$task" --tail 200
homebased --json resource job move "$job" --priority high --front --operation-id "$move"
homebased --json resource job cancel "$job" --operation-id "$cancel"
```

Retry an action with the same operation UUID and unchanged arguments after a lost reply. A new action needs a new UUID. Without `--operation-id`, each CLI invocation creates its own UUID.

Use `--front` or `--back` within a level. Add `--priority low`, `medium`, or `high` to change levels. Priority alone joins the new level's back. `--before <job>` and `--after <job>` take the target's level. Do not combine placement flags. Moving a job does not let equal-priority work preempt.

A job submitted with `--after <task-or-job>`, or with `after` in its spec, waits on the machine that submitted it until each of those tasks and jobs succeeds; a job succeeds at `JOB_SUCCEEDED`, and `JOB_PREEMPTED` does not end it. Only then does it reach the queue. Until then, `resource jobs` and the dashboard queue page do not list it. `resource job show` on the submitting machine reports it as `held`, with each dependency's state. If a dependency ends any other way, the job is cancelled with a `JOB_CANCELLED` event, and it never takes a resource. A task's `after` takes job UUIDs too, so a CPU task can wait for a GPU job. Only jobs this machine submitted count: their events come back here from whichever machine queued them. Cancelling a held job ends it on the submitting machine. Run `show` and `cancel` there; a `--machine` that names that machine or the job's queue machine still finds the held job.

Cancellation stops queued work directly. For an active job, it requests a stop and cleanup. The response does not prove that cleanup has finished. Check job show and resource list.

Add `--machine <Fleet-name-or-UUID>` to inspect another machine. Show, move, cancel, and gpu-idle use the saved job authority when machine is absent. For a job submitted from another origin, specify its authority. Resource list, resource jobs, and release default to local.

The web listener serves queue reads and move, cancel, gpu-idle, and release. `GET /v1/fleet/queues` and the dashboard's `/queue` page show the resources and jobs of every Fleet machine. Job submit and resource registration require the socket. Read the [agent queue guide](../.agents/skills/homebased/references/resource-queue.md) for specs, submission, events, and all CLI flags.

## Clear Attention after a machine check

`Attention` means Homebased could not finish cleanup safely. The resource stays held until an agent or person checks the machine and releases that exact Attention UUID. Other resources can continue to serve work. A job result does not prove that its resource is clear.

If a same-user process started at or after the run's recorded workload start and its environment cannot be read, cleanup treats it as a suspect. It is never signalled without a readable run marker. Its scan is not empty. If it remains unreadable at the cleanup bound, the resource enters `Attention`; the cleanup message names its PID and kernel start time. Check that exact process on the authority machine. Unreadable processes that started before the run, unreadable processes under another user, and unreadable processes started by another user's daemon that predates the run are ignored. That last rule covers ssh logins: an `sshd-session` hides its environment, but its root `sshd` parents are not run work. If the workload start was not recorded, unreadable processes are ignored as before.

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

The first blocked queued job gets one `JOB_BLOCKED` notice per blocking episode, but only when a run of a lower level or an Attention holds it up. Waiting behind runs of its own level or higher is normal queue order and sends nothing. The default delay is 15 minutes behind a requested yield, or 30 minutes behind other blockers, including lower `wait` runs and Attention, counted from the start of the episode. The notice goes to the blocked job's thread. It never stops a run.

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

## GPU idle alerts

On Linux with NVIDIA GPUs, the queue machine samples the GPU of each attempt that is executing, or that was asked to yield and is still running. It reads the machine's Netdata agent first and falls back to `nvidia-smi`. Resources without a device index or a saved GPU UUID are not watched. macOS is not watched.

- Netdata charts come from the go.d `nvidia_smi` collector. They are found by context (`nvidia_smi.gpu_utilization`, `nvidia_smi.gpu_power_draw`, `nvidia_smi.gpu_frame_buffer_memory_usage`) and matched to the GPU by its `uuid` label, or by the UUID in the chart id. A GPU with no charts is looked for again at most once a minute. When a GPU's data goes stale, its charts are listed again, at most once a minute, before falling back. A listing changes only the charts of the GPUs it looked for. The power limit comes from `nvidia-smi` when sampling starts, then hourly.
- The `nvidia-smi` fallback runs once per interval, only while some attempt needs it. A failed probe or an `[N/A]` reading leaves the time unknown, never idle.
- Time is counted in 10-second buckets from the attempt's start, so an earlier attempt's busy tail never counts. A bucket is idle when its peak utilization stays below 10%, and unknown when it has too few samples. Unknown buckets never count as idle, and an alert needs at least half the window readable.

The first alert comes when at least 90% of the known buckets in the last 5 minutes are idle, after the attempt was busy at least once. An attempt that is never busy alerts 10 minutes in, when every known bucket is idle. Reminders follow 15 and 30 minutes after the first alert, then hourly, while the GPU stays idle. One minute of continuous busy ends the episode; a bucket without enough samples breaks that minute, so it starts over. A later idle stretch is a new episode with its own alert. Each alert names the attempt, idle time, utilization, power and memory peaks, last log output, and the busiest process of the run with its CPU use. An alert whose attempt ended, whose episode ended, or whose job was silenced since it was recorded is dropped before delivery.

The owner, or any agent, controls a job's alerts. Homebased never caps or overrides these choices:

```sh
homebased --json resource job gpu-idle "$job" --snooze 15m --reason "writing a 30 GB checkpoint" --operation-id "$op"
homebased --json resource job gpu-idle "$job" --off --reason "known CPU phase, splitting it out next run"
homebased --json resource job gpu-idle "$job" --clear
```

`--snooze` takes any positive duration, and `--off` silences alerts for the rest of the job. Both need a reason, which `resource job show` lists with each control. The latest control wins. `--clear` drops both, so the spec's setting applies again. Controls are stored on the job's queue machine and routed like move and cancel. A job still held for its `after` entries refuses them. A peer on an older release refuses them too, and jobs submitted from such a peer get no alerts. A job spec can turn alerts off with `"gpu_idle": "off"`, or set its own window and threshold with `"gpu_idle": {"after": "10m", "util_below": 5}`. A queue machine on an older release cannot take a spec with `gpu_idle`, so the origin refuses such a submit with `cluster_protocol_incompatible` instead of sending it. A job held for its `after` entries is checked when it is submitted, if its queue machine is reachable then, so it fails before its dependencies run rather than after. One held while its queue machine was unreachable is checked when it is sent, and the origin keeps retrying until that machine updates.

Machine defaults go in the same config file as blocked notices. Durations must be positive.

```toml
[resource.gpu_idle]
source = "auto"            # auto, netdata, nvidia-smi, or off
netdata_url = "http://127.0.0.1:19999"
after = "5m"               # idle window before the first alert
util_below = 10            # percent, 1 to 100
interval = "10s"           # bucket length and sampling interval
never_busy_after = "10m"   # default twice after
recover_after = "1m"       # continuous busy that ends an episode
remind_after = "15m"       # first reminder; then 2x, then every 4x
```

With `source = "netdata"`, a GPU without Netdata charts has unknown buckets and gets no alerts. `off` stops sampling on the machine. A spec's `after` also sets that job's never-busy point to twice its value.

## GPU summaries

Each attempt on a resource with a device index gets a GPU summary when it ends, whether it finished a step, failed, was cancelled, or was preempted. `JOB_SUCCEEDED`, `JOB_FAILED`, and `JOB_CANCELLED` carry the summaries of every attempt of the job in `gpu_summaries`, and `JOB_PREEMPTED` carries the attempt it ended. A job submitted from a machine on the previous release gets no `gpu_summaries` in its events, since that origin would drop the field when storing an event and then refuse a retry that still carried it. `resource job show` on the queue machine prints each run's summary on a `gpu:` line, and `--json` puts it in `runs[].gpu_summary`.

The idle sampler keeps running totals for each attempt while it samples, from whichever source answered at the time. When the attempt's child exits, its worker reads the whole attempt from Netdata, using the charts the sampler last found, in at most 2 seconds. Each point of that read counts for the spacing Netdata returned, which is wider than the one asked for when the collector or an older tier is coarser. Memory size is the averaged sum of the framebuffer's free, used, and reserved memory, which stays the physical size, and only the memory in use is a peak. That read replaces a sampled field when it covers more of the attempt. If Netdata is slow, down, or never had the GPU's charts, the sampled totals stand, including the `nvidia-smi` ones. The worker saves that read in the same commit that ends the attempt, and the summary is computed once, in that commit, so every delivery of an event carries the same numbers. A summary ends when the child exited, as its worker recorded it, even when the daemon applies that exit later, such as at its next start; only a run lost with no exit record ends when it was found lost. The read delays the end of an attempt by at most 2 seconds, and it adds no write of its own that the exit commit could wait behind.

| Field | Meaning |
| --- | --- |
| `util_avg` | Average utilization, in percent. |
| `power_pct_avg` | Average power draw over the enforced power limit, in percent. The sampler uses the limit it read at the time; the end read uses the latest one. |
| `mem_peak_mib`, `mem_total_mib` | Highest framebuffer memory in use, and the memory size, in MiB. |
| `idle_pct` | Share of the sampler's 10-second buckets that were idle, out of those with enough samples. Always from the live buckets, never from Netdata's coarser history. |
| `first_busy_after_secs` | Seconds from the child's start to its first busy bucket; absent when it never got busy. |
| `idle_alerts`, `snoozes` | `JOB_GPU_IDLE` alerts produced for the attempt, and each snooze, off switch, and clear in force during it, with its reason and time. |

Each measured field is an object with `value`, `covered_secs`, `coverage_pct` (the share of the attempt's `attempt_secs` it covers), and `source`, `netdata` or `nvidia-smi`. A field with no samples is absent. The summary's own `source` is `mixed` when its fields, or parts of one field, came from both. `status` is `ok`, or `unavailable` with a `reason` when no field has samples, such as an attempt that ended before its child started, a machine with `source = "off"`, or a source that never answered. Callbacks add a one-line `brief` to each summary.

The numbers are device-level. Anything else on the same physical GPU, such as a process started outside the queue, is included. Without the end read, the last 10 to 20 seconds of an attempt are not covered, and coverage below 100% says so.

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
