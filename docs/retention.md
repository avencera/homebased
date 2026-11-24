# Retention

The daemon prunes the output that finished work leaves in its state
directory: task directories (`tasks/<id>`, which hold `output.log` and the
`evidence` of task events), queue job directories (`jobs/<id>`, which hold the
checkpoint state all runs of a job share), and idle CI slot clones. It runs a
pass 10 minutes after it starts and then every `retention.interval`.

A pass prunes only output whose task or job has ended and whose events are
delivered and settled. A task's age counts from the later of its end and its
last event settling. It keeps output that a task or job that has not ended
waits on with `after`, that a parked worker's report names, that belongs to a
parked agent's chain or to an earlier run of a job that has not ended, that
`homebased wait --long` still holds, or that is a Claude worker's log whose
usage has not been read yet. It also keeps the newest `keep_last` ended tasks
and jobs of each thread, whatever their age. A CI clone is pruned only while
its slot has no run and only if a run used it; its age counts from its last
run. A task directory that no record names, such as one an earlier release
left, is dated by its newest file and kept while a worker holds its runner
lock.

These protections read this machine's database only. Work that ran here for
another machine is kept until its events are delivered there, but a task or
parked worker on that machine that waits on it does not protect it, so keep
`max_age` longer than such waits.

A pruned task keeps its row, events, and reports. `task show` adds
`output_pruned_at`, and `task log` fails with `output_pruned`, which says when
the output went.

```toml
[retention]
enabled = true       # false: prune only with `homebased gc`
interval = "6h"
min_age = "3d"       # nothing younger is pruned, not even over a size limit

[retention.tasks]
max_age = "14d"
keep_last = 3        # newest ended tasks of each thread, always kept
# max_size_gib = 20  # no ceiling by default

[retention.jobs]
max_age = "14d"
keep_last = 1

[retention.ci]
max_age = "7d"       # slot clones; the next CI job on the slot clones again
```

Each `max_size_gib` is a ceiling on that kind's total size on disk. While a
kind is over it, its oldest ended output at least `min_age` old is pruned too.
`min_age` must be at least 3 days, the longest an event waits for its thread,
so a late final event never names output that is gone, and every `max_age` must
be at least `min_age`.

Run a pass now with `homebased gc`:

```sh
homebased gc --dry-run   # what would be pruned, and how much it would free
homebased gc             # prune
homebased gc --vacuum    # prune, then rebuild the database to return its free pages
homebased --json gc --dry-run
```

The report lists each kind's entries, size, and how many were pruned or kept
and why, the largest pruned entries, and the database size with its free
space. `--json` lists every pruned entry. A vacuum holds the database while it
runs, so other daemon work waits for it; run it only when the report shows
free space worth returning. `gc` exits non-zero when it could not remove
something, and the next pass tries again.
