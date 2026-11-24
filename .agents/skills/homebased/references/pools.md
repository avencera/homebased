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
- `--deadline` is fixed at create (at most 24h). A job whose `--kill-after` runs past it is refused with `pool_deadline_too_close`; size `--kill-after` to the job.
- `--env-file` holds `NAME=value` secrets, such as an upload token, on the coordinator. Machines get them after they join; never put a secret in a job's command line or the repository.
- `--slots` (default 1) is how many jobs one machine runs at once; `--idle-limit` (default 10m), `--stall-after` (default 30m), and `--boot-timeout` (default 15m) tune the lifetime rules below.
- Jobs run with `--git` from the pushed commit, or in a `cwd` that the boot leaves on the machine. A fresh machine has nothing else of yours on it.

## Lifetime rules

- A machine renews its lease only while a run on it wrote output within `--stall-after`, jobs wait in its queue, or it still owes events. A job that goes silent longer than that loses its machine about 10 minutes later.
- A machine with no work for `--idle-limit` stops offering the class and releases itself.
- At the pool's deadline, or after `pool release`, every machine is deleted, running or not.

## Outputs

A job must upload its results before it exits. A job whose machine was deleted under it ends with `JOB_FAILED` carrying `machine_released` (`pool`, `machine`, `reason`, `hard_deadline_at`, `gone_at`, `output_lost`). `reason` is `hard_deadline`, `heartbeat_lapsed` (its output stalled or the machine died), `requested` (pool released), or another cloudlease reason. Anything not uploaded is lost, and so is the run's log; resubmit after fixing the cause, with a longer `--deadline` pool if it hit the deadline.

## Inspect

`pool show <name>` lists each machine with its state: `requested`, `booting`, `joined`, `releasing`, or `released` with a `reason`. `last_error` holds the last cloudlease error. `ci slots` shows the joined machines' slots and `ends_at`. A pool that cloudlease refused (for example `spend_cap_reached`) or whose run it lost (`run_not_found`) releases itself and fails its waiting jobs with `JOB_FAILED` whose `held_ending` carries that code. Jobs still waiting when a pool reaches its deadline or is released fail the same way, with `pool_deadline` or `pool_released`.

See `docs/temporary-machines.md` in the homebased repository for the design and the cloudlease contract.
