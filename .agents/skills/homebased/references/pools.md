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

- The pool name is the CI class. Submit one `ci run --class <pool>` per unit of work; the coordinator asks for a machine whenever ready jobs find no free slot, up to `--max`. Do not write a dispatcher loop.
- `--deadline` is fixed at create (at most 24h). A job runs only on a machine that outlives its `--kill-after`, so a job that no machine of the pool could run to its kill limit, counting a new machine's boot, is refused with `pool_deadline_too_close`, and a waiting job that falls out of reach later (for example while its `--after` dependency runs) fails with `JOB_FAILED` carrying that code. The message names the longest `--kill-after` the pool takes now; size `--kill-after` to the job.
- `--env-file` holds `NAME=value` secrets, such as an upload token, on the coordinator. Machines get them after they join; never put a secret in a job's command line or the repository.
- `--spend-cap` (US dollars, such as `25` or `12.50`) caps what the pool's cloudlease run may spend; cloudlease's own per-run cap applies without it, and a value above that cap ends the pool with `invalid_request`. Ask the user for the amount.
- `--slots` (default 1) is how many jobs one machine runs at once; `--idle-limit` (default 10m), `--stall-after` (default 30m), and `--boot-timeout` (by default a minute before cloudlease's boot grace ends, read from each lease, so 7m on DigitalOcean and 19m on phoenixNAP) tune the lifetime rules below. On a class whose boot grace is longer than 8 minutes, such as phoenixNAP, pass `--boot-timeout` (`19m`) so jobs submitted before the first machine exists are sized for that boot.
- `--min-free-disk` (GiB, default 1) is the free disk below which a pool machine admits no job. Set it to what one job needs on disk, within what the machine class has free after boot, so a machine filled by earlier jobs stops taking new ones and idles out.
- Jobs run with `--git` from the pushed commit, or in a `cwd` that the boot leaves on the machine. A fresh machine has nothing else of yours on it.

## Lifetime rules

- A machine renews its lease only while a run on it wrote output within `--stall-after`, jobs wait in its queue, or it still owes events. A job that goes silent longer than that loses its machine a few minutes later, when cloudlease's heartbeat window (3 minutes by default) ends.
- A machine with no work for `--idle-limit` stops offering the class and releases itself.
- At the pool's deadline, or after `pool release`, every machine is deleted, running or not.
- A machine whose lease cloudlease ended (`410 lease_ended`, such as at a spend cap) stops offering the class at once; its running job ends with `machine_released` when the machine is deleted.

## Outputs

A job must upload its results before it exits. A job whose machine was deleted under it ends with `JOB_FAILED` carrying `machine_released` (`pool`, `machine`, `reason`, `hard_deadline_at`, `gone_at`, `output_lost`). `reason` is `hard_deadline`, `heartbeat_lapsed` (its output stalled or the machine died), `requested` (pool released), or another cloudlease reason. Anything not uploaded is lost, and so is the run's log; resubmit after fixing the cause, with a longer `--deadline` pool if it hit the deadline.

## Inspect

`pool show <name>` shows the pool's state (`open`, `stopped`, `releasing`, `released`) and each machine with its state: `requested`, `booting`, `joined`, `releasing`, or `released` with a `reason`. `last_error` holds the last cloudlease error. `ci slots` shows the joined machines' slots and `ends_at`.

A pool stops when cloudlease refuses a machine for good, such as with `run_ended` (its class was repriced or removed, or its run hit its spend cap) or `spend_cap_reached` (a per-run or hourly cap), or after 3 machines in a row that the provider could not create (`create_failed`; it waits longer after each). A stopped pool asks for no more machines and takes no new job, its waiting jobs fail with `JOB_FAILED` whose `held_ending` carries that code, and jobs already running finish on their machines; it is released once those machines are gone. Tell the user the code: these need a cloudlease or budget change, not a retry. A pool whose run cloudlease lost (`run_not_found`) is released at once, and so is one whose run cloudlease ended without the coordinator asking (`run_ended`, `cause` `spend_cap` or `released`, such as an operator's release): its waiting jobs fail with `run_ended`, and its machines drop out once cloudlease confirms them gone. Jobs still waiting when a pool reaches its deadline or is released fail the same way, with `pool_deadline` or `pool_released`.

See `docs/temporary-machines.md` in the homebased repository for the design and the cloudlease contract.
