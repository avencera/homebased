# CI queue design

Agents submit build and test jobs by class (`rust`, `android`, `xcode`, `sim`), and Homebased decides which Fleet machine runs each one and when. This page records the model and the choices behind it. [CI queue](ci-queue.md) covers operation.

## Model

| Concept | Type | Rule |
| --- | --- | --- |
| Class | `JobClass` | 1 to 31 lowercase letters, digits, and inner hyphens, starting with a letter |
| Capacity | `SlotCapacity` | 0 to 32 slots of one class on one machine |
| Slot | `SlotLane` | One unit of capacity. It is a queue resource named `<class>-<n>`, so a slot holds one run at a time |
| Job | `JobSpec` with `ci` | A one-step queue job with a class, a hard `kill_after`, and optionally a git checkout |
| Machine choice | `MachineChoice` | `Any` machine that offers the class, or `Pinned` to one |
| Offer | `MachineOffer` | What a machine tells placement: admission, load, cores, free disk, and per class its capacity, busy slots, and queued jobs |
| Admission | `Admission` | `Open`, or closed by a drain, by load per core over the limit, or by free disk under the floor |
| Timeout | `RunTimeout` | The limit and the time the run had when the queue killed it |
| Timing | `JobTiming` | Submit, start, and end times of a job on the machine that ran it |

A CI job is a queue job, so it keeps every guarantee of the queue: one run per slot, cleanup before the slot's next run, events delivered at least once to the submitting thread, `--after` chains, `resource job show`, and `homebased wait`.

## Extend the resource queue rather than build beside it

The resource queue already has priorities, positions, moves, cancels with operation IDs, held jobs on the origin, cross-machine event delivery, and process-group cleanup. A separate CI queue would have to copy all of it. What it lacked was a capacity counter, classes, placement across machines, admission, hard kills, and timing. Each of these fits the existing model:

- Capacity is a set of slot lanes. The queue's invariant of one active run per resource gives "at most N runs of this class" without a new counter, and each slot keeps its own warm checkout.
- Each class is a separate pool. The scheduler calls the same `decide` once for the GPU pool, which holds exactly the resources and jobs it held before, and once for each class. GPU scheduling, preemption, and blocked notices therefore see the same inputs as before.
- The GPU idle sampler and the lease guard run only on Linux, and only for resources with a GPU UUID. Slot lanes have no device index and no GPU UUID, so neither guard watches them on any platform. On macOS neither guard runs at all.

## Place late, on the origin

A CI job waits on the machine that submitted it, held like a job with `after`, until some machine that offers its class has a free slot. Placement then sends it to that machine, and it starts there within a scheduling pass.

- Late placement sends a job to whichever machine frees a slot first, which is the shortest real wait, with no estimate to get wrong. Among machines with a free slot, which all have an expected wait of zero, placement takes the lowest load per core.
- Moving a waiting job to another machine or level changes only the origin's saved route. No cross-machine handoff is needed.
- Each origin orders its own waiting jobs by priority, then by submit time. Two origins can send to the same free slot at once. The machine's queue then orders the two jobs, and the second waits there for the next free slot.
- A job's wait runs from its submit on the origin to its run's start, so it counts time held on the origin and time queued on the machine.

## Admission

A machine admits a CI run only when it is not drained, its 1-minute load per core is at or under `ci.max_load_per_core`, and the checkout root has at least `ci.min_free_disk_gib` GiB free. Offers carry the same answer, so placement skips a closed machine. The machine checks again before each launch, because an offer can be seconds old.

## Hard kill timeout

Every CI job has a `kill_after` limit, from 1 second to 24 hours, counted from the run's start. Past it, the queue records the timeout and stops the run through the same path as a cancel. The worker sends `SIGTERM` to the child's process group, then `SIGKILL` after the grace period, and cleanup sweeps the run's marker. The job ends `failed`, and its `JOB_FAILED` event carries `timeout` with the limit and the run time. The task's inactivity `timeout` is still only a reminder.

The stop is stored as the existing `user_cancel` stop cause, and a side table records that it was a timeout. The lease guard records its cancels the same way. A new stop cause would mean rebuilding the `resources` table, whose CHECK constraint lists the causes, while other tables reference it.

## Checkouts

A job that runs on another machine must say how its code gets there. `ci.git` names a remote URL and a commit. Before the command, the run fetches that commit into the slot's own clone under `ci.checkout_root`, checks it out detached, and runs `git clean`, which keeps ignored files such as `target/` and any listed untracked paths. It first removes git lock files a killed run left in the clone, since a slot runs one job at a time. This is what `cove-ci-offload.sh` does with its per-slot worktrees, and like that script it needs the commit to be fetchable from the remote. Without `ci.git`, the job runs in its absolute `cwd`, which must exist on whichever machine it lands on, so such a job is usually pinned.

## Preemption

CI jobs use `wait` with no restart window, and the spec refuses any other mode. Late placement only sends a job to a free slot, so a CI job never needs to stop another one. GPU jobs keep their modes.

## Not covered

- Unpushed commits: the run fetches from the remote, so the commit must be pushed there first.
- Rebalancing a job that already reached a machine's queue. It reaches a queue only when it can start there, except in the rare race above.
- Blocked notices for CI jobs. Their waits are in `ci stats` instead.
- Re-placing a job whose machine stops answering after placement sent it. The origin resends it to that machine, with backoff, until the machine answers or the job is cancelled.
- Capacity from config. Slot counts are set at runtime with `ci capacity` and stored in each machine's database.
