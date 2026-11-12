# Operate the CI queue

The CI queue runs build and test jobs on whichever Fleet machine has a free slot of the job's class. A job names a class, such as `rust`, `android`, `xcode`, or `sim`, not a machine. Each machine offers some classes, each with a number of slots. [CI queue design](ci-queue-design.md) explains the model and why it extends the resource queue.

## Offer slots

A machine offers nothing until it is given slots. Set a class's slot count on this machine, or on another with `--machine`:

```sh
homebased ci capacity rust 6
homebased ci capacity android 1 --machine code
homebased ci capacity xcode 2 --machine mac-mini
```

The count is 0 to 32 and takes effect at once. Raising it opens new slots. Lowering it retires the highest slots: a retired slot finishes its run and takes no new one, and the class starts no new run while its busy slots number the new count or more. A count of 0 stops the machine taking the class. Slot counts are stored in the machine's database, so they survive restarts.

Each slot is a queue resource named `<class>-<n>`, such as `rust-3`. The name is reserved: `ci capacity` refuses a class whose slot name is already a GPU resource. Slots never show in `resource list`, `resource jobs`, or the GPU queue dashboard.

## Submit a job

```sh
homebased ci run --class rust --git -- cargo test --workspace
homebased ci run --class android --machine code --kill-after 45m -- ./gradlew connectedCheck
homebased ci run --class rust --repo git@github.com:me/app.git --commit "$sha" --workdir rust -- just test
```

| Flag | Meaning |
| --- | --- |
| `--class` | Class of slot the job needs. Submit fails with `class_not_offered` when no reachable machine it may use offers the class |
| `--machine` | Run only on this machine. Without it, any machine that offers the class |
| `--priority` | `high`, `medium` (default), or `low` |
| `--kill-after` | Hard limit on the run, from `1s` to `24h`, default `1h` |
| `--git` | Run at this checkout's HEAD, in the same subdirectory as the current one, fetched from the branch's upstream remote (or `--remote`). HEAD must be on a branch of that remote as last fetched, so push first |
| `--repo`, `--commit`, `--workdir` | Run at a full commit id fetched from this URL, in this subdirectory |
| `--keep` | Untracked path the clean before each run keeps; repeat for more. Ignored files, such as `target/`, are always kept |
| `--after` | Task or job that must succeed first; repeat for more |
| `--job-id` | Stable UUID, so a retry after a lost response does not submit twice |

Without `--git` or `--repo`, the job runs in the current directory, which must exist at the same path on whichever machine runs it. Pin such a job with `--machine`.

A job waits on the machine that submitted it until a machine that offers its class has a free slot, then runs there. Its final event reaches the submitting thread like any queue job's: `JOB_SUCCEEDED`, `JOB_FAILED`, or `JOB_CANCELLED`. `homebased wait --next <job>` waits on it, `resource job show <job>` shows it, and `resource job cancel <job>` cancels it, whether it is still waiting or already running.

### Placement

Each second, the submitting machine reads every reachable machine's offer: whether it admits CI runs, its load and free disk, and per class its slots, busy slots, and queued jobs. It walks its waiting jobs by priority, then by submit time, and sends each to an open machine with a free slot of the class, choosing the lowest load per core, then the most free slots. A job with no free slot anywhere keeps waiting, so it goes to whichever machine frees a slot first.

### Checkouts

With `--git` or `--repo`, each slot keeps its own clone under `ci.checkout_root`, in `<root>/<repo>-<hash>/<class>-<n>`. Before the command, the run removes git lock files a killed run left behind, fetches the commit if the clone lacks it, checks it out detached, and runs `git clean -fd` with the `--keep` paths excluded. Ignored files survive, so build caches such as `target/` stay warm per slot. The command then replaces the wrapper shell, so the queue's kill reaches it.

## Admission

A machine starts a CI run only while:

- it is not drained,
- its 1-minute load divided by its cores is at most `ci.max_load_per_core`, and
- the filesystem holding `ci.checkout_root` has at least `ci.min_free_disk_gib` free.

Offers carry the same answer, so placement skips a closed machine, and the machine checks again before each launch. Running jobs are never stopped by admission.

```toml
[ci]
checkout_root = "/build/ci"   # default: <home>/ci/checkouts
max_load_per_core = 3.0       # default 3.0
min_free_disk_gib = 10        # default 10
```

## Drain a machine

```sh
homebased ci drain --machine mac-mini --reason "OS update"
homebased ci undrain --machine mac-mini
```

A drained machine finishes its running CI jobs and starts no new ones, and placement sends it nothing. Jobs already in its queue wait there until it reopens. Cancel them, or wait, as needed.

## Move a waiting job

```sh
homebased ci move "$job" --machine code
homebased ci move "$job" --any --priority high
```

`ci move` changes the machine choice or level of a job that is still waiting on its origin. It runs on the machine that submitted the job. A job already sent to a machine's queue is refused with `move_refused`. It stays on that machine: `resource job move <job> --priority <level>` (or `--front`, `--before <job>`) reorders it in that machine's queue, and to run it elsewhere, cancel it and submit it again.

## Hard kill timeout

Past `--kill-after`, counted from the run's start, the queue stops the run the way a cancel does: `SIGTERM` to the command's process group, then `SIGKILL` after the grace period, and cleanup sweeps every process the run started. The job ends `failed`, and its `JOB_FAILED` event carries `timeout`:

```json
"timeout": { "limit": "45m", "ran_secs": 2700, "at": "2026-11-11T18:04:05Z" }
```

## Check slots and stats

```sh
homebased ci slots
homebased ci stats --since 7d
homebased ci stats --since 24h --class rust --machine code
```

`ci slots` lists each machine's admission, load per core, free disk, per-class counts, and each slot with its run. `ci stats` reports, per machine and class, the jobs that ended in the window or have not ended, how many are waiting, the p50 and p90 of queue wait and run time, and how many timed out. A job's wait runs from its submit on the origin to its run's start on the machine, so it includes time waiting on the origin. Both commands read every reachable machine and list those they could not read. `--json` prints the full answer.

## Protocol and upgrades

CI jobs need cluster protocol 6. A peer on an older release never answers an offer, so placement never sends it a CI job, and `ci capacity`, `ci drain`, and `ci stats` refuse it by name. GPU queue jobs are unchanged and keep working with older peers.
