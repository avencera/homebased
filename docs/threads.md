# Thread commands

```sh
homebased thread context [<ref>...] [--children [--live]] [--machine <name>]
homebased thread read [<ref>] [--before <pos>|--after <pos>|--oldest|--item <pos> [--offset <n>]] [--limit <n>] [--max-chars <n>] [--machine <name>]
homebased thread compact [<ref>] [--machine <name>]
homebased thread watch [<ref>...] --context-above <150k|80%> [--action compact] [--once] [--children]
homebased thread watch [<ref>...] --stalled-after <20m|1h> [--once] [--children]
homebased thread watches [--all]
homebased thread unwatch <watch-id>
homebased thread report --summary <text> [--file <path>|-] [--stop-children]
homebased thread detach [<ref>] [--machine <name>]
homebased thread interrupt <ref> [--stop] [--machine <name>]
homebased thread answer <ref> --request <id> --answer <text>... [--machine <name>]
```

## Thread references

A thread reference is T3 chip text (`[Thread: <title>; ref=<ref>]`),
`thread_<id>`, a `t3-thread://v2/<environment>/<id>` link, a bare T3 id (or the
`threadId: <id>` line of a `<t3_context>` block), a Claude Code session id, or
a Codex thread id. A chip's `ref` is the id of a context record in the message
that carried the chip, not a thread id, and is unique only within that
message. Run from a T3 thread, homebased reads the record in the newest of that
thread's last 200 user messages that carries the ref; otherwise, or when none
does, it reads the records with that ref in every user message in this
machine's T3, and fails with `thread_ref_ambiguous` when they name different
threads. A chip
with no record here works only in its
`thread_<id>` form, and any other fails with `thread_not_found` and a hint to
pass the `threadId` from the `<t3_context>` block. With none, the command uses the
calling thread; `--task <id>` names a task's origin thread, on the machine it
was submitted from, and `--worker <id>` its worker, on the machine that runs
it; a `--machine` naming another machine fails with `usage`. Each command looks
on this machine first, then asks every reachable Fleet machine about the
references it did not find; `--machine <name>` looks only there, so a
reference found only elsewhere fails with `thread_not_found`. With `--children`,
`--machine` limits only which machine's children are listed; the calling thread
is always read here. Each daemon advertises its
T3 environment id to Fleet peers, so a `t3-thread://` link goes straight to
the machine that runs it. Only `thread context` and `thread read` accept a
UUID prefix of at least 4 characters. A reference that matches several threads fails with
`thread_ref_ambiguous` (exit 5), and one that matches none with
`thread_not_found` (exit 3).

If T3's state exists but cannot be read, or lacks the thread tables homebased
reads, the lookup fails with `t3_unavailable` rather than reporting no T3
threads. A T3 thread's session is
the one T3 runs its next turn in: the active provider thread of the selected
provider instance, or none before the first turn or while a provider switch is
pending.

## thread context

`thread context` prints each thread's host (`t3`, `worker`, or `terminal`),
provider, session, T3 id and link, title, state (`waiting` when a homebased task
or job still owes the thread events, T3 is due to wake it with a T3
subagent's report, or a child thread it launched with `t3 launch --child` has not
reported yet), context tokens, context window
and percent (Codex only), last activity in US Central, whether the prompt cache
is warm, and the last compaction, looked up as far as 4 MiB back in the
transcript or rollout. `--children` adds every T3 subagent the calling
T3 thread started with `delegate_task`, with the parent link read from T3's thread
lineage, and every child thread it launched with `t3 launch --child`, on any
machine, with a `child_thread` record of its link, its parent, and whether its
report has arrived. Forks of the calling thread are not children. A child whose newest
delegated task T3 records as ended, and that runs no turn, has
`task_finished: true` and shows as `finished` while it is idle, and `settled`
says whether T3 settled it. A child is done when it is settled or finished and
runs no turn, even while a homebased task still owes it events and its state
shows `waiting`; that work wakes it with a new turn when it reports. `--live`
leaves out children that are archived or done; T3 records a new task when a
finished child gets a message, so it counts as live again.
A child T3 records no task for, such as one from an older T3, is judged by its
settled and archived state alone. When a reachable Fleet machine does
not answer, the result has `children_complete: false` and names it in
`unanswered`, and human output warns on stderr; `thread watch --children` then
fails with the retryable `thread_lookup_incomplete` and saves no watch.
Machines that Fleet lists as offline are not asked.

## thread read

`thread read` reads a T3 thread's messages from the state of the T3 on
whichever machine runs it, through that machine's daemon, so the T3 server
need not run. It prints the user and assistant messages T3 shows in the
thread, oldest first, without tool calls: the newest 20 by default, or
`--limit` 1 to 100, each text cut at `--max-chars` (4000 by default, at most
20000). Each message has its position, the index T3's own `t3_thread_read`
gives its item among all the thread's items, tool calls included, which
`--before` and `--after` page from and `--item` reads alone; a cut text has
`next_offset`, which `--item <pos> --offset <n>` reads on from. A thread
forked from a run starts with its parent's messages through that run, marked
`inherited_from`. A `running` message is still growing, so read it again with
`--item`. JSON output has `thread`, as `thread
context` prints it, then `messages`, `earlier`, and `later`, the positions to
page from, or `null` at either end. A message to the agent carries
`created_by` (`user` or `agent`), `source` (such as `web`, `server`, or
`mcp`), and `sender_thread`. A thread outside T3 fails with
`read_unsupported_host` (exit 5), and a position with no message with `usage`.

## thread compact

`thread compact` sends `/compact` to a T3 thread on whichever machine runs it,
and records it in `compactions.jsonl` with trigger `manual`. A thread that is
running a turn, including the calling thread, answers `queued`: T3 compacts it
once that turn ends, and asking again during the same turn queues no second
compaction. Nothing wakes the thread after a queued compaction, so a thread
that should resume on its own afterwards keeps a `homebased wait` running or
watches itself with `--action compact --once` instead. A thread outside
T3 fails with `compact_unsupported_host` (exit 5), and a T3 thread with no
session to compact yet with `thread_not_found` (exit 3).

## thread interrupt

`thread interrupt` ends a T3 thread's running turn on whichever machine runs
it, as the composer's stop button does (`run.interrupt`), and answers
`interrupted` with the `run`, or `idle` when no run was active. `--stop` does
what the thread's Stop button does instead (`thread.stop`): it ends the active
run, holds queued messages, and stops the thread's T3 subagents, and answers
`stopped`. A stop first stops the thread's own child threads that have not
reported, wherever they run, so none of them wakes it again. A stopped child
thread that has not reported counts as done: its link closes on its own
machine (`child_thread.closed` in lookups), so `--children --live`, stall
watches, and its parent's wait all leave it out, and on the calling machine
when the parent's end is there, which the output's `closed_link` names.
Homebased tasks and jobs the thread submitted keep running, and their final
events can start it again unless T3 still holds messages queued at the stop,
so cancel those with `homebased cancel` first. A refusal from T3 fails with
`thread_control_refused` (exit 5).

## thread answer

`thread answer` answers a question a T3 thread's agent is waiting on, on
whichever machine runs it (`runtime-request.respond`). Pass T3's request id
with `--request` and one `--answer` per question, in order; an answer that
names an offered option in another case picks that option. A request that is
not pending, a wrong number of answers, or an approval of a command or file
change fails with `thread_control_refused`. Approvals stay with a person in
T3 Code, since they gate what an agent may run.

## thread watch

`thread watch` keeps a watch on the calling machine and checks it every minute,
right after the idle compaction scan, which keeps its own 55-minute rule and
token limits. When the watched context climbs over the limit while the thread runs no
turn, the caller gets one `THREAD_CONTEXT_OVER` event; the watch fires again
only after a reading shows the context below the limit, so a missing reading
never re-arms it. The same thread, limit, and action again is the existing
watch; another limit or action on the same thread is a second watch, and each
fires on its own. `--action compact` compacts the
T3 thread first, with trigger `watch`, and puts the outcome in the event, which
wakes the caller only when the compaction did not start or the caller watches
itself and no T3 subagent's report, homebased task, or job is still
due to wake it; the
watch is read again just before, so one removed meanwhile compacts nothing. A
watched T3 thread that is done, settled or finished with its delegated task
and running no turn, even while it shows `waiting`, never fires or compacts, even over the limit, whether the watch names the
thread or the session it runs; it re-arms silently when it drops below the
limit, and once it is unsettled or takes a new task it fires as before.

`--stalled-after <grace>` (5 minutes to 24 hours) watches for a stall instead:
the caller gets one `THREAD_STALLED` once the thread has sat `idle` that long
since its last activity, or since a scan last saw it running or waiting, with no turn running, no homebased task or job owing
it events, no T3 subagent due to report to it, and no child thread that has
not reported. A done thread never fires, and neither does a child thread that has reported. One idle stretch
fires once; a turn, a wait, or new activity re-arms it.
It only notifies, so `--action compact` is refused, and each watch takes one
condition, so a context watch and a stall watch on one thread are two watches.
A T3 subagent's stall watch ends with `finished` an hour after its task ends,
and a child thread's as soon as it has reported and is settled. A top-level
thread that is not a child thread and sent its last report looks like a
stalled one unless it is settled, so unwatch it when the report arrives. A `t3_background_wait` on a
command outside homebased is invisible to homebased, so a thread that waits
that way past the grace reads as stalled.

Every firing event carries
`watch_ended`: a `--once` watch ends with the event that fires it, which says
`watch_ended: true` and `end_reason: "once"`, and sends no
`THREAD_WATCH_ENDED`. Any other end sends `THREAD_WATCH_ENDED`: the watch is
removed with `thread unwatch`; its thread is archived (`archived`), or found
gone on two scans in a row (`deleted`, `session_gone`), so one bad read ends
nothing; or its T3 thread stayed done, settled or finished with its task, for an
hour with no change in tokens or activity (`finished`), which also ends a
watch of the session that thread runs. A watch of a session
ends once the session's record is gone, even while T3 still names it, and a
running task worker's session counts as live without one. A percent watch on a
T3 thread that starts running Claude Code ends with reason
`no_context_window`. The watches of a caller that runs in a T3 thread on this
machine end with `caller_gone` once that thread is archived, is gone on two
scans, or stays settled or finished for an hour without activity; that end
event, and any earlier event of the watch still undelivered, reaches a running
`homebased wait` but never wakes the caller. No watch event is delivered to a
caller whose T3 thread is archived or deleted, whatever ended the watch, so a
`--once` event still waiting to go out never unarchives it; the stored event
and end reason stay as they were. A caller outside T3 is never ended this way, and a watch made before the caller's T3 thread was recorded saves it
the first time a scan finds it. The counts and the hour live in the
daemon's memory, so a restart only delays an end. A target on
another machine is read through that machine on each check; while it does not
answer, the watch is left as it was, and so is a caller while this machine's
T3 cannot be read. Events are delivered like task events, in
order per watch, except that a started compaction of another thread and the `unwatched` and
`finished` ends, which ask nothing of the caller, reach only a running
`homebased wait` and never wake it. An event can arrive twice, for example when a lost
acknowledgement after `homebased wait` printed it lets delivery send it again,
so recipients deduplicate by `(watch, seq)`. An event whose saved
callback can no longer run, or that fails 3 times, is given up so later events
still go out. Events for a stopped Claude session wait until it runs again, for
up to 3 days as task callbacks do. The skill's
[threads.md](../.agents/skills/homebased/references/threads.md) has the payload.

## thread report

`thread report` sends a child thread's report to its parent, run in the
child's T3 thread during a turn. It is refused with `thread_report_refused`
(exit 5), naming each piece, while the child has open work: a homebased task
or GPU queue job that owes it events, a sleep hold (`hold`, including what a
held worker parked on), a T3 subagent due to report to it, or a child thread
of its own that has not reported (one on a machine that does
not answer counts as open). Otherwise it sends `--summary`, followed by the
text of `--file` (`-` reads stdin), up to 16 KiB in all, to the parent as a
`HOMEBASED_MESSAGE` whose body starts `Report from child thread thread_<id>:`.
The report is saved first, under a message UUID derived from the link and the
report's number, so a retry or a daemon restart finishes the same report and
never sends a second. A child sends at most one report per T3 run: when a later
run reopens a reported child, such as one a user message started, a report
from it goes as a follow-up report (`seq` 1, 2, and so on) whose body starts
`Follow-up report from child thread thread_<id>:`, once the report before it
was delivered. The parent waited only on the first report, so a follow-up does
not make it wait again. Other text from the run that already reported fails with `thread_report_conflict`, whose
`reason` is `same_run` or `undelivered`, and a thread no `--child` launch made
fails with `not_child_thread`. Once each reporting turn
ends, the child's daemon asks T3 to settle the child (`thread.settle`, bound
to that run), unless a later run or a user message to the child came first or
T3 refuses. The output's `stage` is `sending`, `settle_pending`, `sent`
(delivered and left unsettled), or `settled`. A parent stops waiting on a
child that is archived, deleted, or settled without a report; for a child on
another machine, once a scan sees it so or misses it on two scans in a row.

`thread report --stop-children` stops each child thread that has not reported,
as `thread interrupt --stop` does, and stops waiting on it, when such children
are the only open work left; the output's `stopped` lists them. The report is
checked first, so a report that fails its own checks, such as its size,
stops nothing. A child that cannot be
stopped, such as one on a machine that does not answer, stays open work, so
the report is still refused, and the refusal's `input.stopped` lists the
children it did stop. Stopping is explicit rather than
automatic: an unreported child is work the parent asked for, the report's
refusal is how the parent learns of it, and a stop cannot be undone. Archiving
or settling a parent stops none of its children.

## thread detach

`thread detach [<ref>]` ends a child thread's link to its parent, for a child
whose work outgrew it, such as one that became a long-running coordinator.
With no reference it detaches the calling thread. The parent stops waiting on
it, `thread context --children` no longer lists it, a stop of the parent no
longer stops it, its questions no longer alert the parent, and `thread report`
in it fails with `not_child_thread`. Its own child threads stay linked to it.
The child's machine runs the detach: it first ends the parent machine's copy
of the link, then its own, so a failure never leaves the parent waiting on a
child that can no longer report, and the same command again finishes it. Then,
unless the parent ran the command, it tells the parent with a
`HOMEBASED_MESSAGE` from the child's session whose body starts `Child thread
thread_<id> detached`, under a message UUID derived from the link. The output's
`notice.outcome` is `sent`, `skipped` (the parent detached it), or `failed`
with a `message`, which leaves the detach in place. A detached link stays in
the database so a retried launch still finds it.

## Questions from child threads

A child thread waiting on a person, for a question its agent asked or an
approval, cannot go on, and its parent may be asleep on it. The child's
machine checks its T3 for pending requests of linked child threads every
minute, with the child-thread scan, and sends the parent one
`HOMEBASED_MESSAGE` per request, which steers into a running turn and wakes a
sleeping parent. Its body starts `Child thread thread_<id> is waiting on a
question (request <id>).` (or on an approval), then the child's T3 link, the
questions with their options or what the approval asks for, and the
`homebased thread answer` command. Its message UUID comes from the link and
the request, so a later scan or a daemon restart sends the same message,
which the message path delivers once it has a receipt; a restart in the
moment between delivery and receipt can deliver it again.

## HTTP routes

On the socket, `POST /v1/threads/context` resolves references and
`GET /v1/threads/watches?caller=<thread>` (or `all=true`) lists watches; both
are also served on the dashboard port. `POST /v1/threads/read`,
`POST /v1/threads/compact`, `POST /v1/threads/watches`,
`POST /v1/threads/watches/{id}/unwatch`, `POST /v1/threads/report`, and
`POST /v1/threads/control` (interrupt, stop, and answer) are socket only.
Fleet peers answer `POST /v1/cluster/threads/context`,
`POST /v1/cluster/threads/read`, `POST /v1/cluster/threads/compact`, and
`POST /v1/cluster/threads/control`.
