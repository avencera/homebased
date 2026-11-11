# Thread context, compaction, and watches

`homebased thread` reads how full an agent thread's context is, compacts T3 threads, and sends one event when a thread's context passes a limit or the thread stalls. It works on any Fleet machine. Pass `--json` and parse the result.

```bash
homebased --json thread context                      # this thread
homebased --json thread context --children           # this thread's T3 children, finished ones too
homebased --json thread context --children --live    # only children still working
homebased --json thread context 'thread_3f2a…' 9c1e  # several references; 9c1e is a prefix
homebased --json thread compact 'thread_3f2a…'
homebased --json thread watch <child-thread-id> --context-above <limit> --action compact
homebased --json thread watch --children --stalled-after 20m
homebased --json thread watches
homebased --json thread unwatch <watch-id>
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
- Only `thread context` accepts a prefix: at least 4 characters of a UUID-shaped id. It must still name one thread.
- More than one match fails with `thread_ref_ambiguous` (exit 5) and lists the candidates; pass `exact_reference` from one of them. No match fails with `thread_not_found` (exit 3), whose message names the machines searched and those that did not answer.
- A chip's `ref` is not a thread id. It is the id of a context record in the message that carried the chip, unique only within that message, and homebased reads the thread from that record in this machine's T3. Run from a T3 thread, it reads the record in the caller's own recent messages first; otherwise a ref that records in different messages tie to different threads fails with `thread_ref_ambiguous`, so pass the `threadId` instead. A chip with no record here (for example, one pasted from another machine) works only when its ref is `thread_<id>`; any other fails with `thread_not_found` and the hint "chip ref is not a thread id; pass the threadId from the t3_context block". The `<t3_context>` block in the same message has a `threadId:` line for each chip; pass that.
- A T3 thread's session is the one T3 runs its next turn in: the active provider thread of the selected provider instance. Right after a provider switch, before the first turn on the new provider, the thread has no session. A T3 thread's own session matches as that T3 thread; older sessions of the same T3 thread match as plain terminal sessions.
- If T3's state exists but cannot be read or lacks the thread tables homebased reads, for example while T3 migrates it, the command fails with `t3_unavailable` (retryable) instead of reporting no T3 threads.

## `thread context`

Each entry has `machine_name`, `host` (`t3`, `worker`, or `terminal`), `provider`, `session`, `t3_thread`, `t3_link`, `title`, `state`, and the context fields:

- `tokens`: context size of the last request or compaction. `tokens_estimated` is `true` right after a compaction, until the next request.
- `context_window` and `percent`: Codex records the window; Claude does not, so both are `null` for Claude.
- `last_active`, `cache_warm` (whether the one-hour prompt cache is still warm, so compacting is cheap), and `compacted_at`, the last compaction within the last 4 MiB of the transcript or rollout.
- `state`: `running`, `idle`, `waiting` (a homebased task or job still owes it events, or T3 is due to wake it with a delegated task's report), `settled`, `archived`, or `unknown` (no usage recorded yet). A root waiting on its `delegate_task` children shows `waiting`, not `idle`. A wait registered with `t3_background_wait` on a command outside homebased does not count, since homebased cannot read it from T3.
- `task_finished`: `true` when T3 records the delegated task the thread runs as ended (completed, failed, cancelled, or interrupted) and no run is left. Human output shows such an idle thread as `finished`. A new message to the child starts a new task, so it reads `false` again. `false` for a thread T3 records no task for.
- `settled`: whether T3 settled the thread in its sidebar. `state` shows `waiting` instead while a homebased task still owes the thread events or a delegated task is due to report to it, so read this flag, not `state`, to tell whether T3 is done with it.
- A thread is done when it is `settled`, or has `task_finished: true`, and runs no turn. This holds while its `state` is `waiting`: the pending homebased work wakes it with a new turn when it reports, and that makes it live again.
- `session_exists`: whether the session's own record (Claude transcript or Codex rollout) is still on disk; `null` when not checked.
- `model`, `effort`, `project`, `branch`, `workspace`, `parent_title`, and `pull_request` when T3 records them.

Human output gives times in US Central.

### The children ledger

`thread context --children` lists every T3 thread that the calling T3 thread started with `delegate_task`, finished ones included, each with title, link, state, provider, model, and context. Forks of this thread are not children, so `--children` never reaches one. Each child has `task_finished`, so the ledger tells finished delegates from working ones. Add `--live` to leave out children that are archived or done, including done children that show `waiting` because a homebased task still owes them events. A child T3 records no task for, such as one made by an older T3, is left out only when settled or archived. After a compaction, use it instead of memory to find out which delegates exist and what state they are in.

The list can be incomplete. When a reachable Fleet machine does not answer, the result has `children_complete: false` and names each such machine with the reason in `unanswered`. Human output warns on stderr. Run the command again before you rely on the list. Machines that Fleet lists as offline are not asked and not listed.

## `thread compact`

Asks T3 to compact a T3 thread on whichever machine runs it. The result `outcome` is `started`, `not_found`, `refused` (T3 would not compact now, for example while a turn runs), or `unavailable`.

- Only T3 threads can be compacted. Any other thread fails with `compact_unsupported_host` (exit 5); tell that session to run `/compact` instead.
- A T3 thread with no session yet fails with `thread_not_found` (exit 3).
- A peer that predates the command fails with a usage error; run `homebased update` there.

The daemon's own idle compaction is separate: after about 55 idle minutes, at 175K tokens or more, it compacts any T3 thread that is not archived, runs no turn, and will likely run again. That is one a homebased task or a delegated task's report will wake, or one that is neither settled nor a finished delegate, such as an idle root or a thread waiting on the user.

## `thread watch`

```bash
homebased --json thread watch <ref>... --context-above <limit> [--action compact] [--once]
homebased --json thread watch <codex-thread-id> --context-above 70%
homebased --json thread watch <ref>... --stalled-after 20m [--once]
homebased --json thread watch --children --stalled-after 20m
```

Each watch has one condition: `--context-above` or `--stalled-after`.

- `--context-above` takes tokens (`150000`, `150k`, `1.2m`) or a percent of the context window (`80%`). A percent needs a known window, so it is refused for Claude threads; a percent watch on a T3 thread that starts running Claude later ends with `THREAD_WATCH_ENDED` reason `no_context_window`; watch again with a token count, keeping `--action compact` if it had it.
- `--children` also watches the calling thread's live T3 children at the time you run it: those not settled, not archived, and not finished with their delegated task. Children started later need another `thread watch`. Use it for stall watches, not context watches. It has no model filter, so a context watch would also catch Codex delegates. Running it again also adds a watch at its limit next to any limit you raised for one child, since another limit on the same thread is a second watch. Give each context watch the thread ids it is for. If a reachable machine does not answer the children lookup, it fails with `thread_lookup_incomplete` (exit 1, retryable) and saves no watch. Retry it.
- The watch checks about once a minute. It sends `THREAD_CONTEXT_OVER` once when the context climbs over the limit while the thread is idle or waiting, never while a turn runs. It fires again only after a reading shows the context below the limit and it climbs over again. A missing reading, or one exactly at the limit, changes nothing.
- A watched T3 thread that is done (settled, or `task_finished`, with no turn running, even while it shows `waiting`) never fires and is never compacted, even over the limit, whether you watch it by T3 id or by the session it runs. Dropping below the limit while done re-arms the watch silently, so once it is unsettled or gets a new task, it fires as before.
- `--action compact` compacts the T3 thread when it fires and still sends the event, with the outcome in `compaction`. A compaction that started on another thread wakes no caller, since nothing is left to do; only a running `homebased wait` prints it. A caller that watches itself wakes for its own compaction, which is how a root that ended its turn to be compacted resumes, unless something else is due to wake it: a delegated task whose report T3 will still deliver to it, running or finished, or a homebased task or queue job that still owes it events. Then only a running `homebased wait` prints it, and the root picks the work back up when that report arrives. A delegate whose report T3 disposed of, as when the root's run was stopped, or one started to report only once the root settles, does not count. A compaction T3 would not start (`refused`, `unavailable`, `not_found`) wakes the caller as usual. It is refused for threads outside T3. A watch removed while the check runs compacts nothing.
- `--stalled-after` takes minutes (`20m`, `20`) or hours (`1h`, `1h30m`), from 5 minutes to 24 hours. It sends `THREAD_STALLED` once the thread has been `idle` that long since its `last_active`, or since a check last saw it running or waiting, whichever is later: no turn running, no homebased task or job owing it events, and no T3 delegated task due to report to it (those show `waiting`). It does not fire on a thread that is done (settled, or `task_finished`), and it fires once per idle stretch. A turn, a wait, or new activity re-arms it silently, so it fires again only if the thread stalls again. It only notifies, so `--action compact` is refused. A thread with no `last_active` yet never fires.
- A stalled delegate ends its watch on its own once its task finishes and it stays quiet for an hour (`finished`). A top-level thread, such as one from `t3_thread_launch`, looks the same after it sends its final report as when it stalls, unless it is settled. Run `thread unwatch` when its report arrives, or the watch fires once more after the grace. A wait on a command outside homebased registered with `t3_background_wait` is invisible to homebased, so a thread that waits that way longer than the grace reads as stalled.
- `--once` ends the watch with the event that fires it: that `THREAD_CONTEXT_OVER` or `THREAD_STALLED` has `watch_ended: true` and `end_reason: "once"`, and no `THREAD_WATCH_ENDED` follows. Every other firing event has `watch_ended: false`.
- A watch is its caller, thread, condition, and action. The same four again update the existing watch's `--once` and callback (`created: false`). Another limit, grace, or action on the same thread adds a second watch, so one thread can have a context watch and a stall watch.
- Every other end sends `THREAD_WATCH_ENDED`, with `reason`:
  - `unwatched`: `thread unwatch` removed it. The command's output already confirms the end, so this event never wakes the caller; only a running `homebased wait` prints it.
  - `archived`: its T3 thread was archived. This ends it at once.
  - `deleted` or `session_gone`: two checks in a row found the T3 thread, or a plain session's record, gone, even while T3 still names the session. One bad read ends nothing. A running task worker's session counts as live without a transcript, since Claude workers save none unless asked.
  - `finished`: its T3 thread stayed done for an hour with no change in tokens or `last_active`. Any activity, or new work, starts the hour over. Nothing is left to do, so this event never wakes the caller; only a running `homebased wait` prints it.
  - `caller_gone`: the T3 thread that receives the events, on this machine, was archived, was gone on two checks, or stayed done for an hour without activity. This event, and any earlier event of the watch not yet delivered, goes only to a running `homebased wait` and never wakes the caller. A caller outside T3 never ends its watches this way.
- No watch event is delivered to a caller whose T3 thread is archived or deleted, whatever ended the watch, such as a `--once` event still waiting to go out when the caller was archived. Only a running `homebased wait` gets it, the stored event and end reason stay as they were, and an archived caller stays archived.
- The checks and the hour are counted in the daemon's memory, so a daemon restart starts them over and only delays an end.
- The watch lives on the calling machine. A target on another machine is checked through that machine; while it does not answer, the watch waits. While this machine's T3 cannot be read, nothing ends.

Events go to the calling thread like task callbacks, in order per watch, except those that ask nothing of it: a started compaction of another thread, `unwatched`, `finished`, and `caller_gone`. Those stay in the store, and `thread watches --all` shows each watch's state, `seq`, and end reason. A running `homebased wait` prints them, but they never wake the caller. An event whose callback can no longer run (a `codex queue` delivery whose saved directory or Codex binary is gone), or that fails 3 times, is given up so the later ones still go out. Events for a stopped Claude session wait until it runs again, and hold back that watch's later events meanwhile; after 3 days of waiting, counted from its first deferral since the daemon last started, the event is given up, as task callbacks are; [events.md](events.md#thread-watch-events) lists their keys. In a Claude thread inside T3, `homebased wait --next <watch-id>` takes them instead, as for tasks ([t3.md](t3.md#waiting-in-a-claude-thread)).

`thread watches` lists the calling thread's live watches; `--all` lists every caller's, ended ones included. `thread unwatch <id>` ends one, and repeating it is harmless.

## Orchestrator recipe

A T3 root that delegates long work:

1. Start children with `delegate_task`.
2. Watch context one thread at a time. Watch your own thread with `homebased --json thread watch --context-above <limit> --action compact`, with no reference. For each Claude delegate, run `homebased --json thread watch <child-thread-id> --context-above <limit> --action compact` with the child thread id that `delegate_task` returned; it compacts the child while it is idle once it passes the limit. A compaction that starts wakes you for nothing; you hear only when T3 would not compact it. After the compaction drops the context, the watch re-arms. To raise one child's limit, unwatch its old watch, since a new limit adds a second watch. Add a notify-only watch (no `--action`) only for a child whose work you would rather hand to a fresh thread than compact. Do not use `--children` for context watches (see `thread watch` above). The orchestrate skill sets the default limits for the root and each kind of delegate.
3. After your own compaction, run `homebased --json thread context --children` to rebuild the list of delegates, their links, and their states; `task_finished` marks the ones whose task ended. `--children --live` lists only those still working.
4. Watch the children for stalls too: `homebased --json thread watch --children --stalled-after 20m` wakes you with `THREAD_STALLED` when a child ends its turn without a report or a registered wait and sits idle for 20 minutes. Read its last turn, then send it a message to carry on or report, or hand its work to a fresh delegate. For threads you started as top-level threads, pass their references instead of `--children`, and unwatch each one when its report arrives.
5. A done child never fires, and its watches end with `finished` after an hour without activity, without waking you, so there is nothing to unwatch. Unwatching one yourself wakes you for nothing either. Archiving a child ends its watches at once, and settling or archiving the root ends the root's watches with `caller_gone`.
