# Run batch jobs on temporary cloud machines (pools)

A pool is a CI class whose machines cloudlease creates when its jobs wait and deletes when they are idle, stalled, or past the pool's deadline. Use it for batch work that needs more machines than the Fleet has, such as cleaning many scrolls on 16 droplets. Homebased schedules the jobs; cloudlease alone decides whether a machine exists, so no agent can keep one alive by mistake. Never create cloud machines yourself, and never hold or ask for a provider token.

## Create, submit, release

Run these on the pool's coordinator (the daemon with `[pools]` and `[fleet.iroh]` config, `code` today): only it reaches pool machines, over the iroh Fleet link. Ask the user before creating a pool, since it costs money.

```sh
homebased --json pool create afv --machine-class do-c16 --max 16 --deadline 6h --env-file ~/.config/afv/pool.env
job="$(uuidgen)"
homebased --json ci run --class afv --job-id "$job" --git --kill-after 2h -- ./clean_scroll.sh PHerc-0172
homebased wait --next "$job"
homebased --json pool show afv
homebased --json pool release afv
```

- The pool name is the CI class. Submit one `ci run --class <pool>` per unit of work; the coordinator asks for a machine whenever ready jobs find no free slot, up to `--max`. Do not write a dispatcher loop. `pool create` refuses a name a Fleet machine already offers as a CI class (`class_offered`, exit 5); pick another name. Once a pool is released, its class is the Fleet's again: a job submitted then waits for a Fleet machine that offers the class, and the old pool never fails it.
- `--deadline` is fixed at create (at most 24h). A job runs only on a machine that outlives its `--kill-after`, so a job that no machine of the pool could run to its kill limit, counting a new machine's boot, is refused with `pool_deadline_too_close`, and a waiting job that falls out of reach later (for example while its `--after` dependency runs) fails with `JOB_FAILED` carrying that code. The message names the longest `--kill-after` the pool takes now; size `--kill-after` to the job.
- `--env-file` holds `NAME=value` secrets, such as an upload token, on the coordinator. Machines get them after they join; never put a secret in a job's command line or the repository. A `PATH` in it applies inside the run only, so name a program outside the machine's default `PATH` by its full path or start it from a `--git` script.
- `--spend-cap` (US dollars, such as `25` or `12.50`) caps what the pool's cloudlease run may spend; cloudlease's own per-run cap applies without it, and a value above that cap ends the pool with `invalid_request`. Ask the user for the amount.
- `--slots` (default 1) is how many jobs one machine runs at once; `--idle-limit` (default 10m) and `--stall-after` (default 30m) tune the lifetime rules below. A machine that has not joined when cloudlease's boot grace ends (8 minutes by default, 20 on phoenixNAP) is deleted with reason `heartbeat_lapsed`, and the pool asks for another if jobs still wait.
- `--min-free-disk` (GiB, default 1) is the free disk below which a pool machine admits no job. Set it to what one job needs on disk, within what the machine class has free after boot, so a machine filled by earlier jobs stops taking new ones and idles out.
- Jobs run with `--git` (or `--repo` and `--commit`) from the pushed commit, or in a `cwd` that the boot leaves on the machine. A fresh machine has nothing else of yours on it.
- Private repositories work: the machine never gets a git credential. At submit the coordinator fetches the commit with its own git credentials and makes a one-commit bundle; the machine downloads it from the coordinator over the link when the run starts and checks it out. So the checkout is shallow (one commit, no tags). The submit is refused, before any machine is asked for, with `commit_unavailable` when the coordinator cannot fetch the commit (push it, or fix the remote or the coordinator's credentials), `submodules_unsupported` when the commit has a `.gitmodules`, or `bundle_too_large` past 2 GiB. A download that breaks is tried 3 times, with each failure in the run's log, before the run fails.
- `--job-retries` (default 2, at most 10) is how many times a job whose machine was interrupted, such as by an AWS Spot interruption, runs again on another machine. Every job of such a pool must be safe to rerun (see Interrupted machines); pass `--job-retries 0` for jobs that are not.

## Lifetime rules

- A machine renews its lease only while a run on it wrote output within `--stall-after`, jobs wait in its queue, or it still owes events. A job that goes silent longer than that loses its machine a few minutes later, when cloudlease's heartbeat window (3 minutes by default) ends.
- A machine with no work for `--idle-limit` stops offering the class and releases itself. On an hourly-billed provider (phoenixNAP, Latitude.sh) it first stays until 10 minutes before its paid hour ends, still taking jobs, as cloudlease names in its renew answers; it never starts a new paid hour for that, and never stays past the pool's deadline.
- At the pool's deadline, or after `pool release`, every machine is deleted, running or not.
- A machine whose lease cloudlease ended (`410 lease_ended`, such as at a spend cap) stops offering the class at once; its running job ends with `machine_released` when the machine is deleted.

## Interrupted machines

When the provider takes a machine back (reason `interrupted`, such as an AWS Spot interruption), the coordinator queues its jobs again for another machine and the pool asks for a replacement; do nothing. Each rerun shows as `JOB_PREEMPTED` with `machine_lost` (`pool`, `machine`, `reason`, `attempt` now queued, `max_attempts`), a progress event that does not wake a sleeping thread. cloudlease notices an interrupted machine about a minute after the provider takes it, and the job is queued again then.

So a job on such a pool must be safe to run again: derive output names from the input, upload each output whole (write, then rename or put at once), and skip work whose output exists.

A rerun ends with `JOB_FAILED` and `machine_released` with `not_rerun.cause`: `retries_exhausted` (it ran `max_attempts` times), `out_of_reach` (no machine could run it to its kill limit before the deadline), `pool_ended`, `pinned`, or `cancelled` (a cancel was asked for first). A replacement past the spend cap is refused, which stops the pool and fails the queued job with `spend_cap_reached` in `held_ending`. Report a job that kept getting interrupted to the user rather than resubmitting it in a loop. `heartbeat_lapsed` is never rerun, since a stalled job would stall again.

## Outputs

A job must upload its results before it exits. A job whose machine was deleted under it ends with `JOB_FAILED` carrying `machine_released` (`pool`, `machine`, `reason`, `hard_deadline_at`, `gone_at`, `output_lost`). `reason` is `hard_deadline`, `heartbeat_lapsed` (its output stalled or the machine died), `requested` (pool released), `interrupted` (see above), or another cloudlease reason. `attempt` says which run ended. Anything not uploaded is lost, and so is the run's log; resubmit after fixing the cause, with a longer `--deadline` pool if it hit the deadline. A job pinned to a machine with `--machine` that never reached it, such as one still waiting on `--after`, fails with `held_ending` code `machine_released` once that machine is gone. `resource job show <job>` on the coordinator still works after the machine is deleted: it shows the job under `ended` with its `state` and saved final `event`.

## Inspect

`pool show <name>` shows the pool's state (`open`, `stopped`, `releasing`, `released`) and each machine with its state: `requested`, `booting`, `joined`, `releasing`, or `released` with a `reason`. `last_error` holds the last cloudlease error. `ci slots` shows the joined machines' slots and `ends_at`.

A pool stops when cloudlease refuses a machine for good, such as with `run_ended` (its class was repriced or removed, or its run hit its spend cap) or `spend_cap_reached` (a per-run or hourly cap), or after 3 machines in a row that the provider could not create (`create_failed`; it waits longer after each). A stopped pool asks for no more machines and takes no new job, its waiting jobs fail with `JOB_FAILED` whose `held_ending` carries that code, and jobs already running finish on their machines; it is released once those machines are gone. Tell the user the code: these need a cloudlease or budget change, not a retry. A pool whose run cloudlease lost (`run_not_found`) is released at once, and so is one whose run cloudlease ended without the coordinator asking (`run_ended`, `cause` `spend_cap` or `released`, such as an operator's release): its waiting jobs fail with `run_ended`, and its machines drop out once cloudlease confirms them gone. Jobs still waiting when a pool reaches its deadline or is released fail the same way, with `pool_deadline` or `pool_released`.

See `docs/temporary-machines.md` in the homebased repository for the design and the cloudlease contract.
