# Thread context, compaction, and watches

`homebased thread` reads how full an agent thread's context is, compacts T3 threads, and sends one event when a thread's context passes a limit. It works on any Fleet machine. Pass `--json` and parse the result.

```bash
homebased --json thread context                      # this thread
homebased --json thread context --children           # this thread's T3 children, finished ones too
homebased --json thread context 'thread_3f2a…' 9c1e  # several references; 9c1e is a prefix
homebased --json thread compact 'thread_3f2a…'
homebased --json thread watch --children --context-above 150k
homebased --json thread watches
homebased --json thread unwatch <watch-id>
```

## Thread references

Every subcommand takes the same references:

| Form | Example |
| --- | --- |
| T3 chip text, copied whole | `[Thread: Fix upload; ref=thread_3f2a…]` |
| T3 thread id | `thread_3f2a…`, or the bare id |
| T3 link | `t3-thread://v2/<environment>/<id>`; the environment picks the machine |
| Claude Code session or Codex thread | its UUID |
| Nothing | the calling thread itself (`T3CODE_THREAD_ID`, `CODEX_THREAD_ID`, or `CLAUDE_CODE_SESSION_ID`) |
| `--task <id>` | the origin thread of a Homebased task |
| `--worker <id>` | the worker session of a Homebased task |

- The command looks on this machine first, then asks every reachable Fleet machine about the references it did not find here. `--machine <name>` looks only there.
- Only `thread context` accepts a prefix: at least 4 characters of a UUID-shaped id. It must still name one thread.
- More than one match fails with `thread_ref_ambiguous` (exit 5) and lists the candidates; pass `exact_reference` from one of them. No match fails with `thread_not_found` (exit 3), whose message names the machines searched and those that did not answer.
- A T3 thread's own Claude or Codex session matches as that T3 thread. Older sessions of the same T3 thread match as plain terminal sessions.

## `thread context`

Each entry has `machine_name`, `host` (`t3`, `worker`, or `terminal`), `provider`, `session`, `t3_thread`, `t3_link`, `title`, `state`, and the context fields:

- `tokens`: context size of the last request or compaction. `tokens_estimated` is `true` right after a compaction, until the next request.
- `context_window` and `percent`: Codex records the window; Claude does not, so both are `null` for Claude.
- `last_active`, `cache_warm` (whether the one-hour prompt cache is still warm, so compacting is cheap), and `compacted_at`.
- `state`: `running`, `idle`, `waiting` (a task still owes it events), `settled`, `archived`, or `unknown` (no usage recorded yet).
- `model`, `effort`, `project`, `branch`, `workspace`, `parent_title`, and `pull_request` when T3 records them.

Human output gives times in US Central.

### The children ledger

`thread context --children` lists every T3 thread that the calling T3 thread started with `delegate_task`, finished ones included, each with title, link, state, provider, model, and context. Add `--live` to leave out settled and archived children. After a compaction, use it instead of memory to find out which delegates exist and what state they are in.

## `thread compact`

Asks T3 to compact a T3 thread on whichever machine runs it. The result `outcome` is `started`, `not_found`, `refused` (T3 would not compact now, for example while a turn runs), or `unavailable`.

- Only T3 threads can be compacted. Any other thread fails with `compact_unsupported_host` (exit 5); tell that session to run `/compact` instead.
- A T3 thread with no session yet fails with `thread_not_found`.
- A peer that predates the command fails with a usage error; run `homebased update` there.

The daemon's own idle compaction of waiting threads (after about 55 minutes idle, above 200K tokens) is separate and unchanged.

## `thread watch`

```bash
homebased --json thread watch <ref>... --context-above 150k [--action compact] [--once]
homebased --json thread watch --children --context-above 70%
```

- `--context-above` takes tokens (`150000`, `150k`, `1.2m`) or a percent of the context window (`80%`). A percent needs a known window, so it is refused for Claude threads.
- `--children` also watches the calling thread's live T3 children at the time you run it. Children started later need another `thread watch`.
- The watch checks about once a minute. It sends `THREAD_CONTEXT_OVER` once when the context climbs over the limit while the thread is idle, waiting, or settled, never while a turn runs. It fires again only after the context drops back below the limit and climbs over it again.
- `--action compact` compacts the T3 thread when it fires and still sends the event, with the outcome in `compaction`. It is refused for threads outside T3.
- `--once` ends the watch after it fires.
- Watching the same thread again from the same caller updates the existing watch (`created: false`) instead of adding one.
- The watch ends with `THREAD_WATCH_ENDED` when it is removed, fired with `--once`, or its thread is archived, deleted, or its session is gone.
- The watch lives on the calling machine. A target on another machine is checked through that machine; while it does not answer, the watch waits.

Events go to the calling thread like task callbacks; [events.md](events.md#thread-watch-events) lists their keys. In a Claude thread inside T3, `homebased wait --next <watch-id>` takes them instead, as for tasks ([t3.md](t3.md#waiting-in-a-claude-thread)).

`thread watches` lists the calling thread's live watches; `--all` lists every caller's, ended ones included. `thread unwatch <id>` ends one, and repeating it is harmless.

## Orchestrator recipe

A T3 root that delegates long work:

1. Start children with `delegate_task`.
2. `homebased --json thread watch --children --context-above 150k --action compact`, so a child that grows large is compacted while it is idle.
3. After your own compaction, run `homebased --json thread context --children` to rebuild the list of delegates, their links, and their states.
4. When a child is done, `thread unwatch` its watch, or let archiving the child end it.
