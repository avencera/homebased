# Run builds and tests through the CI queue

The CI queue spreads build and test jobs across Fleet machines. A job names a class, such as `rust`, `android`, `xcode`, or `sim`. Homebased picks the machine: one with a free slot of the class that admits the run (not drained, load per core and free disk within limits). Use it instead of claiming a slot by hand with lock files.

## Submit

```sh
job="$(uuidgen)"
homebased --json ci run --class rust --job-id "$job" --git -- cargo test --workspace
homebased wait --next "$job"
```

- `--git` runs at this checkout's HEAD, in the same subdirectory, fetched from the branch's upstream remote. Push first: submit refuses a HEAD that no remote branch contains.
- `--repo <url> --commit <sha> [--workdir <dir>]` names the commit directly.
- Without either, the job runs in the current directory, which must exist at that path on the machine that runs it. Add `--machine <name>` for such a job.
- `--machine <name>` pins the job. Otherwise any machine that offers the class may run it.
- `--kill-after` (default `1h`, at most `24h`) is a hard limit. Past it the queue kills the run's whole process group and the job fails.
- `--priority high|medium|low` orders the waiting jobs. CI jobs never preempt each other.
- `--after <task-or-job>` holds the job until those succeed.
- `--keep <path>` keeps an untracked path through the clean before each run. Ignored files such as `target/` are always kept, so each slot's clone stays warm.

Submit fails with `class_not_offered` when no reachable machine it may use offers the class. `homebased ci slots` lists what each machine offers.

The run sees `HOMEBASED_RESOURCE` set to its slot, such as `rust-3`, and `HOMEBASED_JOB_ID`. Use the slot name to keep per-slot ports or emulators apart.

## Events

A CI job is a queue job. It sends `JOB_STARTED` when it starts on a machine, then one final event: `JOB_SUCCEEDED`, `JOB_FAILED`, or `JOB_CANCELLED`. A `JOB_FAILED` with a `timeout` object (`limit`, `ran_secs`, `at`) means the run passed `--kill-after`. Read the log with `homebased task log <run-task> --tail 100`, where `resource job show <job>` names the run task. Then fix the hang or raise the limit; do not resubmit unchanged.

## While it waits

- `homebased resource job show <job>` shows `held` while the job waits on this machine for a slot.
- `homebased ci move <job> --machine <name>`, `--any`, or `--priority <level>` changes a waiting job. A job already sent to a machine is refused; use `resource job move --machine <name>` there.
- `homebased resource job cancel <job>` cancels it, waiting or running.

## Operate

| Command | Effect |
| --- | --- |
| `ci capacity <class> <n> [--machine <name>]` | Set how many jobs of the class the machine runs at once. A cut lets running jobs finish |
| `ci drain --reason <why> [--machine <name>]` | Start no new CI jobs there; running ones finish |
| `ci undrain [--machine <name>]` | Reopen it |
| `ci slots [--machine <name>]` | Admission, load, disk, classes, and slots of each machine |
| `ci stats [--since 24h] [--machine <name>] [--class <class>]` | p50 and p90 queue wait and run time per machine and class, and timeouts |

Change capacity or drain a machine only when the user asks or the machine's owner runbook says to. See `docs/ci-queue.md` in the homebased repository for admission settings.
