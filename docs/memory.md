# Wait for host memory

Set `mem_peak` to the most host memory, in GiB, that a task will hold, or pass
`task run --mem-peak 17`. The daemon holds the task until memory in use plus
this peak fits under the machine's memory bar, 40 GiB by default. GPU queue
jobs take the same field and `resource job submit --mem-peak`.

Memory in use is the daemon's cgroup (`homebased.service`) plus each
`run-*.scope` beside it, such as commands started with `systemd-run`, each
less its page cache, active or inactive, which the kernel reclaims under the
cgroup's cap; shmem (tmpfs) and unevictable (mlocked) pages stay counted, and
a configured cgroup that is itself a scope counts once. Work already started counts at its
declared peak until it grows that large, so two items that each fit alone
cannot both start into the same room. Items wait in a fixed order: GPU queue jobs in
serving order, then tasks by `priority`, and in submit order within a level. A
waiting item keeps its room, so smaller work behind it cannot start ahead of it
and keep it waiting. A job whose GPU is busy keeps no room until the GPU is
free, unless it was submitted with `--hold-room` (`hold_room` in the spec):
then, while it is the first job waiting for a busy GPU, it holds back from
tasks its peak less the peak of the job running there, which that job frees
when it ends. Once that job stops claiming its peak, such as while its run
cleans up, the whole peak is held. A trace of 28G queued behind a predict of 4G holds 24G, so
tasks cannot take the memory the trace needs before the predict ends, at the
cost of that room for as long as the predict runs. `resource jobs` prints
`holding 24.0G of its 28.0G peak from tasks until its resource is free` under
the job, and `--json` gives it under `memory_holds` by job id, with `peak` in GiB
and `held` in bytes. When the job ahead frees the whole peak, it prints `holds its 28.0G peak from tasks once the
job ahead of it ends`. A job ahead of it without `mem_peak` frees an unknown amount,
so the whole peak is held. Work without `mem_peak` starts as before and is
counted only through memory in use; set `require_peak` (below) on a machine
whose GPU jobs must not lose memory to undeclared tasks.

A task's `priority` is `high`, `medium` (the default), or `low`, set with
`task run --priority` or `priority` in the spec, and needs `mem_peak`. Change
it while the task waits, on the machine that accepted the submit:

```sh
homebased --json task priority <task-id> high
```

A task that already started keeps the room it was granted; priority only
orders tasks that still wait, and never puts a task ahead of a GPU job.

A consumer that needs a large peak only once its producer has output should
not hold that room while the producer waits for memory itself. Submit a small
gate task, such as `--mem-peak 0.1`, that waits for the producer's first output,
and submit the consumer `--after` the gate:

```sh
gate="$(homebased --quiet task run --mem-peak 0.1 -- ./wait-for-first-output.sh)"
homebased task run --mem-peak 33 --after "$gate" -- ./consume.sh
```

The gate's room counts against the producer's, so leave room for both peaks,
and submit the gate after the producer at no higher a level: a GPU queue job is
admitted before any task, and a task producer then goes first in submit order.
The producer keeps its claim while the consumer runs, so their peaks must fit
under the bar together. When they cannot, submit the consumer `--after` the
producer, so it starts once the producer ends.

Declare the most the work will hold, measured from a comparable run and
including every child process and the largest phase, plus about 10%. A memory
cap the command enforces on itself is an upper bound, not a peak: a task that
caps itself at 28G but holds 3.5G still keeps the other 24.5G from every task
behind it for as long as it runs. A started task keeps its whole peak until it
ends, since a later phase, such as a join after a light load, may need it.

When the part of one running task's peak that it has not allocated is, alone,
what keeps the first task waiting for memory from starting, and that lasts ten
minutes, the running task's thread gets one `TASK_MEMORY_REVIEW`. Its
`memory_review` gives the task's `mem_peak`, the bytes its process tree held
(`held_bytes`), the waiting task and its peak, and how long the wait has lasted
(`blocked_secs`). It is an alert, so it wakes a thread sleeping with `wait
--long`, and it changes nothing: the owner keeps the task when a later phase
needs the room, or cancels it and resubmits with a smaller peak. Cancelling it
also cancels work submitted `--after` it, and the replacement has a new id, so
resubmit that work `--after` the replacement. GPU queue job
runs, and tasks submitted from another machine, are not reviewed.

`task show` and `task list --json` give the `mem_peak` and `priority` of every
task submitted with one, also after it starts.

`task list`, `task show`, and `resource jobs` print why an item waits, such as
`waiting for memory: peak 17.0G, 9.2G free under the 40.0G bar`; `--json`
gives it as `memory_wait` on a task and under `memory_waits` by job id. When
memory in use cannot be read, items with a peak wait and say why. Submit
refuses a peak over the bar, a `mem_peak` on a machine whose daemon runs
outside a cgroup v2 memory controller, and `mem_peak` together with
`machine` on a task, since only the local daemon measures its own memory. A
GPU job's queue machine checks its peak; for a job held for `after` whose
queue is another machine, that happens when the job is sent. Set the bar, or
the cgroup to read, in the daemon's config:

```toml
[memory]
bar_gib = 40
# cgroup = "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/homebased.service"
# require_peak = true
```

With `require_peak = true`, submit refuses a command task submitted here to run
here without `mem_peak`, with `invalid_spec` at `/mem_peak`. Agent and
container tasks are exempt, and a task submitted with `--machine` or from
another machine is not checked, so the rule keeps local scripts honest rather
than closing every path. Give a task that only waits `--mem-peak 0.1
--priority high`: a waiting task keeps its room, so at medium a watcher would
queue behind any larger task waiting ahead of it.

Each task's command runs with an OOM score 300 above the daemon's, up to the
kernel's maximum, so when the cgroup reaches its cap the kernel prefers to kill
a task over the daemon. The Linux unit sets `OOMPolicy=continue`, so a task killed
for memory does not make systemd stop the daemon. `homebased update` keeps the
unit it finds, so a unit installed by an earlier release needs `homebased
daemon install` once, with the same `--config` it was installed with.
`daemon install` rewrites the whole unit, so keep settings of your own, such
as `MemoryMax`, in a drop-in like
`~/.config/systemd/user/homebased.service.d/memory-cap.conf`, which it leaves
alone. Adding `OOMPolicy=continue` to such a drop-in works without a
reinstall.
