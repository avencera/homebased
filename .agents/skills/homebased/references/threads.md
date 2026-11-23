# Thread context, messages, compaction, and watches

`homebased thread` reads how full an agent thread's context is, reads a T3 thread's messages, compacts T3 threads, sends one event when a thread's context passes a limit or the thread stalls, and sends a child thread's report, and any follow-up reports, to its parent. It works on any Fleet machine. Pass `--json` and parse the result.

```bash
homebased --json thread context                      # this thread
homebased --json thread context --children           # this thread's T3 children, finished ones too
homebased --json thread context --children --live    # only children still working
homebased --json thread context 'thread_3f2a…' 9c1e  # several references; 9c1e is a prefix
homebased --json thread read 'thread_3f2a…'          # its newest 20 messages
homebased --json thread read 'thread_3f2a…' --before <position>
homebased --json thread compact 'thread_3f2a…'
homebased --json thread watch <subagent-thread-id> --context-above <limit> --action compact
homebased --json thread watch --children --stalled-after 20m
homebased --json thread watches
homebased --json thread unwatch <watch-id>
homebased --json thread report --summary "<report>" [--file report.md]  # in a child thread
```

## Thread references

Every subcommand takes the same references:

| Form | Example |
| --- | --- |
| T3 chip text, copied whole | `[Thread: Fix upload; ref=910af7b5-…]` |
| T3 thread id | `thread_3f2a…`, or the bare id, or the `threadId: …` line of a `<t3_context>` block |
| T3 link | `t3-thread://v2/<environment>/<id>`; the environment picks the machine |
| Claude Code session or Codex thread | its UUID |
| Nothing | the calling thread itself (`T3CODE_THREAD_ID`, `CODEX_THREAD_ID`, or `CLAUDE_CODE_SESSION_ID`) |
| `--task <id>` | the origin thread of a Homebased task |
| `--worker <id>` | the worker session of a Homebased task |

- The command looks on this machine first, then asks every reachable Fleet machine about the references it did not find here. `--machine <name>` looks only there; with `--children`, it limits only which machine's children are listed. A `--task` thread is on the machine the task was submitted from and a `--worker` thread on the machine that runs it, so a `--machine` naming another fails with `usage`.
- Only `thread context` and `thread read` accept a prefix: at least 4 characters of a UUID-shaped id. It must still name one thread.
- More than one match fails with `thread_ref_ambiguous` (exit 5) and lists the candidates; pass `exact_reference` from one of them. No match fails with `thread_not_found` (exit 3), whose message names the machines searched and those that did not answer.
- A chip's `ref` is not a thread id. It is the id of a context record in the message that carried the chip, unique only within that message, and homebased reads the thread from that record in this machine's T3. Run from a T3 thread, it reads the record in the caller's own recent messages first; otherwise a ref that records in different messages tie to different threads fails with `thread_ref_ambiguous`, so pass the `threadId` instead. A chip with no record here (for example, one pasted from another machine) works only when its ref is `thread_<id>`; any other fails with `thread_not_found` and the hint "chip ref is not a thread id; pass the threadId from the t3_context block". The `<t3_context>` block in the same message has a `threadId:` line for each chip; pass that.
- A T3 thread's session is the one T3 runs its next turn in: the active provider thread of the selected provider instance. Right after a provider switch, before the first turn on the new provider, the thread has no session. A T3 thread's own session matches as that T3 thread; older sessions of the same T3 thread match as plain terminal sessions.
- If T3's state exists but cannot be read or lacks the thread tables homebased reads, for example while T3 migrates it, the command fails with `t3_unavailable` (retryable) instead of reporting no T3 threads.

## `thread context`

Each entry has `machine_name`, `host` (`t3`, `worker`, or `terminal`), `provider`, `session`, `t3_thread`, `t3_link`, `title`, `state`, and the context fields:

- `tokens`: context size of the last request or compaction. `tokens_estimated` is `true` right after a compaction, until the next request.
- `context_window` and `percent`: Codex records the window; Claude does not, so both are `null` for Claude.
- `last_active`, `cache_warm` (whether the one-hour prompt cache is still warm, so compacting is cheap), and `compacted_at`, the last compaction within the last 4 MiB of the transcript or rollout.
- `state`: `running`, `idle`, `waiting` (a homebased task or job still owes it events, as for a thread that sleeps with `homebased wait --long`, T3 is due to wake it with a T3 subagent's report, or a child thread it launched has not reported yet), `settled`, `archived`, or `unknown` (no usage recorded yet). A thread waiting on its T3 subagents or child threads shows `waiting`, not `idle`. A wait registered with `t3_background_wait` on a command outside homebased does not count, since homebased cannot read it from T3.
- `task_finished`: `true` when T3 records the delegated task the thread runs as ended (completed, failed, cancelled, or interrupted) and no run is left. Human output shows such an idle thread as `finished`. A new message to the child starts a new task, so it reads `false` again. `false` for a thread T3 records no task for.
- `settled`: whether T3 settled the thread in its sidebar. `state` shows `waiting` instead while a homebased task still owes the thread events or a T3 subagent is due to report to it, so read this flag, not `state`, to tell whether T3 is done with it.
- A thread is done when it is `settled`, or has `task_finished: true`, and runs no turn. This holds while its `state` is `waiting`: the pending homebased work wakes it with a new turn when it reports, and that makes it live again.
- `session_exists`: whether the session's own record (Claude transcript or Codex rollout) is still on disk; `null` when not checked.
- `model`, `effort`, `project`, `branch`, `workspace`, `parent_title`, and `pull_request` when T3 records them.
- `child_thread`, only on a child thread launched with `t3 launch --child`: `link`, `parent` (its `machine` and `t3_thread`), `reported` (whether the parent's machine has its report), and `report`, how far its first report has got on the child's machine, once it has one (see [`thread report`](#thread-report)).

Human output gives times in US Central.

### The children ledger

`thread context --children` lists every T3 subagent that the calling T3 thread started with `delegate_task`, finished ones included, each with title, link, state, provider, model, and context. Forks of this thread are not children, so `--children` never reaches one. Each child has `task_finished`, so the ledger tells finished T3 subagents from working ones. Add `--live` to leave out children that are archived or done, including done children that show `waiting` because a homebased task still owes them events. A child T3 records no task for, such as one made by an older T3, is left out only when settled or archived. After a compaction, use it instead of memory to find out which children exist and what state they are in.

It also lists every child thread the calling T3 thread launched with `t3 launch --child`, on any machine, each with its `child_thread` record; see [Child threads](#child-threads). A grandchild is listed under its own parent only.

The list can be incomplete. When a reachable Fleet machine does not answer, the result has `children_complete: false` and names each such machine with the reason in `unanswered`. Human output warns on stderr. Run the command again before you rely on the list. Machines that Fleet lists as offline are not asked and not listed.

## `thread read`

Reads a T3 thread's messages on whichever machine runs it, so a thread on another Fleet machine reads like one here. It returns the user and assistant messages T3 shows in the thread, oldest first, without tool calls, reasoning, or tool output; for those, use `t3_thread_read` with `view: "activity"` on the thread's own machine.

```bash
homebased --json thread read <ref>                           # newest 20 messages
homebased --json thread read <ref> --before <position>       # the page before
homebased --json thread read <ref> --after <position>        # newer messages
homebased --json thread read <ref> --oldest                  # the first messages
homebased --json thread read <ref> --item <position> --offset <n>  # read on in one long message
```

- The result has `thread`, as `thread context` describes it, then `messages`, `earlier`, and `later`. Pass `earlier` to `--before` for older messages and `later` to `--after` for newer ones; each is `null` when there are none.
- `position` is the one `t3_thread_read` gives the same item: T3 numbers every item of the thread from 0, tool calls included, so positions skip, and either tool's positions work in the other.
- A thread forked from a run starts with its parent's messages through that run, each with `inherited_from` naming the parent; a T3 subagent inherits nothing.
- A message with `status: running` is still growing, and paging on with `--after` skips the rest of it; read it again with `--item <position>`.
- Each message has `position`, `role` (`user` or `assistant`), `status` (such as `completed`, `running`, or `interrupted`), `at`, `text`, and `chars`, the length of the whole text. A `user` message is anything sent to the agent: `created_by` says whether a `user` or an `agent` sent it, `source` how (`web`, `mobile`, `server` for homebased events and messages, `mcp` for another agent's `t3_thread_send`), and `sender_thread` which T3 thread sent it.
- `--limit` takes 1 to 100 messages (20 by default) and `--max-chars` 1 to 20000 characters of each text (4000 by default). A longer text has `next_offset`; read on with `--item <position> --offset <next_offset>`. A position with no message fails with `usage`.
- A thread outside T3 fails with `read_unsupported_host` (exit 5). A peer that predates the command fails with a usage error; run `homebased update` there.

## `thread compact`

Asks T3 to compact a T3 thread on whichever machine runs it. The result `outcome` is `started`, `queued`, `not_found`, `refused` (T3 would not compact now), or `unavailable`.

- A thread running a turn answers `queued`: T3 compacts it once that turn ends. That includes the calling thread, so run `homebased thread compact` mid-turn to compact yourself after the turn. A thread that sleeps with `homebased wait --long` needs none: idle compaction takes care of it, as below. Asking again during the same turn queues no second compaction.
- Nothing wakes a thread after a queued compaction. To resume right after it with no wait running, watch yourself with `--action compact --once` and end the turn instead.

- Only T3 threads can be compacted. Any other thread fails with `compact_unsupported_host` (exit 5); tell that session to run `/compact` instead.
- A T3 thread with no session yet fails with `thread_not_found` (exit 3).
- A peer that predates the command fails with a usage error; run `homebased update` there.

### Idle compaction

The daemon's own idle compaction is separate: after about 55 idle minutes, at 175K tokens or more, it compacts any T3 thread that is not archived, runs no turn, and will likely run again. That is one a homebased task or job, a T3 subagent's report, or a child thread's report will wake, or one that is neither settled nor a finished T3 subagent, such as an idle root or a thread waiting on the user.

A thread that sleeps with `homebased wait --long` ([t3.md](t3.md#sleeping-on-long-work)) is certain to run again hours later, after its cache lapsed, so its limit is 60K tokens instead. An event that arrives for a thread due such a compaction, or while T3 runs a `/compact` turn in it, waits behind the compaction instead of steering into it.

## `thread watch`

```bash
homebased --json thread watch <ref>... --context-above <limit> [--action compact] [--once]
homebased --json thread watch <codex-thread-id> --context-above 70%
homebased --json thread watch <ref>... --stalled-after 20m [--once]
homebased --json thread watch --children --stalled-after 20m
```

Each watch has one condition: `--context-above` or `--stalled-after`.

- `--context-above` takes tokens (`150000`, `150k`, `1.2m`) or a percent of the context window (`80%`). A percent needs a known window, so it is refused for Claude threads; a percent watch on a T3 thread that starts running Claude later ends with `THREAD_WATCH_ENDED` reason `no_context_window`; watch again with a token count, keeping `--action compact` if it had it.
- `--children` also watches the calling thread's live children at the time you run it: the T3 subagents not settled, not archived, and not finished with their delegated task, and the child threads it launched that are not settled or archived. Children started later need another `thread watch`. Use it for stall watches, not context watches. It has no model filter, so a context watch would also catch Codex children. Running it again also adds a watch at its limit next to any limit you raised for one child, since another limit on the same thread is a second watch. Give each context watch the thread ids it is for. If a reachable machine does not answer the children lookup, it fails with `thread_lookup_incomplete` (exit 1, retryable) and saves no watch. Retry it.
- The watch checks about once a minute. It sends `THREAD_CONTEXT_OVER` once when the context climbs over the limit while the thread is idle or waiting, never while a turn runs. It fires again only after a reading shows the context below the limit and it climbs over again. A missing reading, or one exactly at the limit, changes nothing.
- A watched T3 thread that is done (settled, or `task_finished`, with no turn running, even while it shows `waiting`) never fires and is never compacted, even over the limit, whether you watch it by T3 id or by the session it runs. Dropping below the limit while done re-arms the watch silently, so once it is unsettled or gets a new task, it fires as before.
- `--action compact` compacts the T3 thread when it fires and still sends the event, with the outcome in `compaction`. A compaction that started on another thread wakes no caller, since nothing is left to do; only a running `homebased wait` prints it. A caller that watches itself wakes for its own compaction, which is how a root that ended its turn to be compacted resumes, unless something else is due to wake it: a T3 subagent whose report T3 will still deliver to it, running or finished, or a homebased task or queue job that still owes it events. Then only a running `homebased wait` prints it, and the root picks the work back up when that report arrives. A T3 subagent whose report T3 disposed of, as when the root's run was stopped, or one started to report only once the root settles, does not count. A compaction T3 would not start (`refused`, `unavailable`, `not_found`) wakes the caller as usual. It is refused for threads outside T3. A watch removed while the check runs compacts nothing.
- `--stalled-after` takes minutes (`20m`, `20`) or hours (`1h`, `1h30m`), from 5 minutes to 24 hours. It sends `THREAD_STALLED` once the thread has been `idle` that long since its `last_active`, or since a check last saw it running or waiting, whichever is later: no turn running, no homebased task or job owing it events, no T3 subagent due to report to it, and no child thread that has not reported (those show `waiting`). It does not fire on a thread that is done (settled, or `task_finished`), nor on a child thread that has reported, and it fires once per idle stretch. A turn, a wait, or new activity re-arms it silently, so it fires again only if the thread stalls again. It only notifies, so `--action compact` is refused. A thread with no `last_active` yet never fires.
- A stalled T3 subagent ends its watch on its own once its task finishes and it stays quiet for an hour (`finished`). A child thread's watch ends with `finished` as soon as it has reported and is settled. A top-level thread that is not a child thread, such as one from `t3_thread_launch`, looks the same after it sends its final report as when it stalls, unless it is settled. Run `thread unwatch` when its report arrives, or the watch fires once more after the grace. A wait on a command outside homebased registered with `t3_background_wait` is invisible to homebased, so a thread that waits that way longer than the grace reads as stalled.
- `--once` ends the watch with the event that fires it: that `THREAD_CONTEXT_OVER` or `THREAD_STALLED` has `watch_ended: true` and `end_reason: "once"`, and no `THREAD_WATCH_ENDED` follows. Every other firing event has `watch_ended: false`.
- A watch is its caller, thread, condition, and action. The same four again update the existing watch's `--once` and callback (`created: false`). Another limit, grace, or action on the same thread adds a second watch, so one thread can have a context watch and a stall watch.
- Every other end sends `THREAD_WATCH_ENDED`, with `reason`:
  - `unwatched`: `thread unwatch` removed it. The command's output already confirms the end, so this event never wakes the caller; only a running `homebased wait` prints it.
  - `archived`: its T3 thread was archived. This ends it at once.
  - `deleted` or `session_gone`: two checks in a row found the T3 thread, or a plain session's record, gone, even while T3 still names the session. One bad read ends nothing. A running task worker's session counts as live without a transcript, since Claude workers save none unless asked.
  - `finished`: its T3 thread stayed done for an hour with no change in tokens or `last_active`, or it is a child thread that has reported and is settled. Any activity, or new work, starts the hour over. Nothing is left to do, so this event never wakes the caller; only a running `homebased wait` prints it.
  - `caller_gone`: the T3 thread that receives the events, on this machine, was archived, was gone on two checks, or stayed done for an hour without activity. This event, and any earlier event of the watch not yet delivered, goes only to a running `homebased wait` and never wakes the caller. A caller outside T3 never ends its watches this way.
- No watch event is delivered to a caller whose T3 thread is archived or deleted, whatever ended the watch, such as a `--once` event still waiting to go out when the caller was archived. Only a running `homebased wait` gets it, the stored event and end reason stay as they were, and an archived caller stays archived.
- The checks and the hour are counted in the daemon's memory, so a daemon restart starts them over and only delays an end.
- The watch lives on the calling machine. A target on another machine is checked through that machine; while it does not answer, the watch waits. While this machine's T3 cannot be read, nothing ends.

Events go to the calling thread like task callbacks, in order per watch, except those that ask nothing of it: a started compaction of another thread, `unwatched`, `finished`, and `caller_gone`. Those stay in the store, and `thread watches --all` shows each watch's state, `seq`, and end reason. A running `homebased wait` prints them, but they never wake the caller. An event whose callback can no longer run (a `codex queue` delivery whose saved directory or Codex binary is gone), or that fails 3 times, is given up so the later ones still go out. Events for a stopped Claude session wait until it runs again, and hold back that watch's later events meanwhile; after 3 days of waiting, counted from its first deferral since the daemon last started, the event is given up, as task callbacks are; [events.md](events.md#thread-watch-events) lists their keys. In a Claude thread inside T3, `homebased wait --next <watch-id>` takes them instead, as for tasks ([t3.md](t3.md#waiting-in-a-claude-thread)).

`thread watches` lists the calling thread's live watches; `--all` lists every caller's, ended ones included. `thread unwatch <id>` ends one, and repeating it is harmless.

## Child threads

A child thread is a top-level T3 thread launched with `homebased t3 launch --child` ([t3.md](t3.md#child-thread-recipe)). homebased saves the link on the parent's machine and the child's, so it holds wherever the child runs.

- Until the child reports, the parent's `state` is `waiting`, not `idle`: idle compaction treats the parent as one that resumes, and its stall watches leave it alone. A child that is archived, deleted, or settled without a report stops counting. For a child on another machine, the parent's machine stops counting it once a check sees it archived or settled, or does not find it on two checks in a row.
- A child that is idle, not settled, owed nothing, and has not reported is stalled, so `thread watch --children --stalled-after` fires `THREAD_STALLED` for it like any child.
- Once the child has reported and is settled, it is done: `--children --live` leaves it out, and its watches end with `finished`.
- A child can launch children of its own. Each thread lists and waits on its own children only.

## `thread report`

Run in a child thread, inside its turn, as its last step: `homebased --json thread report --summary "<report>" [--file report.md]`. `--file` appends that file's text after a blank line, or stdin's with `-`. The whole report is at most 16 KiB.

- It is refused with `thread_report_refused` (exit 5) while the child has open work, and sends nothing. `input.open` lists each piece by `kind`: `task` or `job` with its `id` (a homebased task or GPU queue job that has not sent this thread its final event), `owed_events` (such work that cannot be named, as from an earlier session of the thread), `hold` with its `id` (a task or job this thread sleeps on with `homebased wait --long`, including what a held worker parked on, until its final event ends the hold), `subagent` (T3 is due to wake the thread with a subagent's report), `child` (a child thread of its own that has not reported, with `child`), or `unconfirmed` (a child on a machine that did not answer, with `child` and `reason`). Wait for that work, then run it again.
- It sends the report to the parent as one `HOMEBASED_MESSAGE` ([messages.md](messages.md#child-thread-reports)). The report is saved before it is sent, so a retry, or a daemon restart, finishes the same report and never sends a second one. A retry returns the saved report with `resumed: true`.
- A child sends at most one report per T3 run. When a later run reopens a reported child, such as one a user message to it started, a report in that run goes as a follow-up report, `seq` 1, 2, and so on, once the report before it was delivered, even with the same text as before. Within one run, the same text is always a retry. The open-work check applies to it too. The parent waited only on the first report, so a follow-up does not make it `waiting` again.
- Other text fails with `thread_report_conflict` (exit 5) and sends nothing. `input.reason` is `same_run` when this run already sent a report: send more now with `homebased message send`, or report again from a later turn. It is `undelivered` when the latest report has not reached the parent: repeat its text to finish it first.
- After the turn ends, the child's machine asks T3 to settle the child, bound to the run the report came from, after a follow-up as after the first report. It leaves the child unsettled when a later run, or a user message to the child, came first, or when T3 refuses.
- The result's `report` has `link`, `parent`, `child`, `seq` (0 for the first report), `message_id`, `created_at`, `sent_at`, and `stage`: `sending` (not delivered yet; run the command again), `settle_pending` (delivered; the settle waits for the turn to end), `sent` (delivered, and left unsettled), or `settled`.
- A thread that no `--child` launch made fails with `not_child_thread` (exit 5); report with `homebased message send` instead. Outside a turn, or before T3 records the thread's session, it fails with `usage`.

## Orchestrator recipe

A T3 root that coordinates work runs each phase, experiment, or long run in a child thread and uses T3 subagents only for short work ([t3.md](t3.md#choosing-the-runner)):

1. Launch child threads with `homebased t3 launch --child` and start T3 subagents with `delegate_task`. Each child thread submits its own long work and sleeps on it; the root never waits on a child's tasks or jobs.
2. Watch the children for stalls: `homebased --json thread watch --children --stalled-after 20m` wakes you with `THREAD_STALLED` when a child thread or T3 subagent ends its turn without a report, a registered wait, or a sleep, and sits idle for 20 minutes. It covers the children live when you run it, so run it again after launching more. Read the child's last turn, then send it a message to carry on or report, or hand its work to a fresh child.
3. Watch context one thread at a time. Watch your own thread with `homebased --json thread watch --context-above <limit> --action compact`, with no reference. For each Claude T3 subagent, run `homebased --json thread watch <subagent-thread-id> --context-above <limit> --action compact` with the thread id that `delegate_task` returned; it compacts the child while it is idle once it passes the limit. A compaction that starts wakes you for nothing; you hear only when T3 would not compact it. After the compaction drops the context, the watch re-arms. To raise one child's limit, unwatch its old watch, since a new limit adds a second watch. Add a notify-only watch (no `--action`) only for a child whose work you would rather hand to a fresh thread than compact. Do not use `--children` for context watches (see `thread watch` above). A sleeping child thread is compacted by idle compaction, so it needs no watch from you. The orchestrate skill sets the default limits.
4. After your own compaction, run `homebased --json thread context --children` to rebuild the list of children, their links, and their states; `task_finished` marks the T3 subagents whose task ended, and `child_thread.reported` the child threads that reported. `--children --live` lists only those still working.
5. A child thread's report arrives as one `HOMEBASED_MESSAGE` ([messages.md](messages.md#child-thread-reports)), and homebased then settles it. A done child never fires, and its watches end with `finished`, at once for a reported, settled child thread and after an hour without activity for a T3 subagent, without waking you, so there is nothing to unwatch. Unwatching one yourself wakes you for nothing either. Archiving a child ends its watches at once, and settling or archiving the root ends the root's watches with `caller_gone`.
