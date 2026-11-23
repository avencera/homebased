# homebased

A long build, test suite, or agent review can leave your Codex conversation waiting. Starting the work is only part of the job. You still need to check when it finishes and bring the result back.

**Homebased runs the work in the background and sends the result back to the same Codex thread.** Your agent submits a task and ends its turn. You can continue the conversation or leave while the work runs. When the task finishes, your agent can report the result in that thread.

Use it to:

- Send a coding task or review to Codex, Claude, Grok, or OpenCode while you work on something else.
- Run a build, test suite, or CI watcher and get the result without repeated status checks.
- Keep a dev server, proxy, or emulator running for the agent, stopped once its thread is done.

Homebased keeps task status and logs. An optional web dashboard lets you inspect tasks and browse files. If a task stops producing output, Homebased asks the agent to check it and leaves the task running.

[Ask your agent to install it](#for-agents), or follow the [manual install steps](#install). Homebased runs on Linux and macOS.

## GPU priority queue

Run GPU commands and containers through the resource queue. Jobs use `high`, `medium`, or `low` priority. A run is preempted for higher-priority work at any age, unless its job sets `hold`, which keeps the GPU once the run is 30 minutes old; a job can yield at checkpoints instead of being killed. Each resource runs one attempt at a time. A job holds its GPU from first step to last, so keep it to GPU work: run builds, data prep, CPU evaluation, reports, and uploads as CPU tasks chained with `--after`. A job submitted `--after` a prep task is held under its id until the prep succeeds, and cancelled without taking a GPU if it fails; a task submitted `--after` a job starts at `JOB_SUCCEEDED`. When a running job's GPU sits near idle for 5 minutes, its thread gets a `JOB_GPU_IDLE` alert from Netdata or `nvidia-smi` samples; the owner can snooze it or turn it off with a reason (`resource job gpu-idle`), and any agent may cancel a job whose GPU sits near 0% for 20 minutes or more with no snooze or off switch. Final job callbacks carry a GPU summary of each attempt: average utilization and power, peak memory, idle share, and time to first busy. Submit refuses a step that itself runs a build tool such as `cargo build`, `build-ptx.sh`, or `pip install` unless given `--allow-build`, and the lease guard yields, requeues, or cancels a job whose processes hold no CUDA context on its GPU for 5 minutes. `resource job preempt`, or Preempt on the dashboard, stops a running job, even one whose spec holds the GPU, for waiting higher-priority work and requeues it at the front of its level. `--place-front`, `--place-before <job>`, or `--place-after <job>` on `resource job submit` choose where the job enters the queue, at submit or, with `--after`, when its prep succeeds. `--mem-peak <GiB>` holds a job with a free GPU until its host memory fits, as for [tasks](#wait-for-host-memory). See the [resource queue runbook](docs/resource-queue.md) for inspection, moves, preemption, cancellation, and cleanup.

## CI build queue

Run builds and tests through the CI queue to spread them across Fleet machines. A job names a class, such as `rust`, `android`, `xcode`, or `sim`, and each machine offers some classes with a number of slots (`homebased ci capacity rust 6`). `homebased ci run --class rust --git -- cargo test` waits on the submitting machine until some machine has a free slot of the class and its load and free disk admit the run, then runs it there at the pushed HEAD in that slot's own clone. Jobs can be pinned with `--machine`, moved while they wait (`ci move`), and are killed with their whole process group past `--kill-after` (default 1h), which fails them with the timeout in `JOB_FAILED`. `ci drain` stops a machine taking new CI jobs, and `ci stats` reports p50 and p90 queue wait and run time per machine and class. See the [CI queue runbook](docs/ci-queue.md).

## For agents

Tell an agent to install this:

```
Install homebased from https://github.com/avencera/homebased.

1. Binary: run the install script at
   https://github.com/avencera/homebased/releases/latest/download/install.sh
   curl -LSfs https://github.com/avencera/homebased/releases/latest/download/install.sh | sh
   Add ~/.local/bin to PATH if needed. Confirm with: homebased --json version

2. Daemon: homebased daemon install
   Confirm with: homebased --json daemon status
   The socket field must be "up". Use `homebased daemon stop` or `homebased daemon restart`, never raw systemctl or launchctl.

3. Skill: the operator contract is `.agents/skills/homebased` in that repository.
   Install it to ~/.agents/skills/homebased:
     mkdir -p ~/.agents/skills
     git clone --depth 1 git@github.com:avencera/homebased.git /tmp/homebased-src
     cp -R /tmp/homebased-src/.agents/skills/homebased ~/.agents/skills/homebased
   Then read ~/.agents/skills/homebased/SKILL.md and follow the route table there.
```

The install script puts the binary on disk and restarts a daemon that is already
running. It does not start a daemon on first install or install the skill.

## Install

Linux and macOS:

```sh
curl -LSfs https://github.com/avencera/homebased/releases/latest/download/install.sh | sh
```

The script downloads the latest GitHub release and installs `homebased` to `~/.local/bin`. If that directory is not on `PATH`, add it, then open a new shell.

Confirm the binary:

```sh
homebased --json version
```

Pass flags after `sh -s --`.

A specific release:

```sh
curl -LSfs https://github.com/avencera/homebased/releases/latest/download/install.sh | sh -s -- --tag v0.1.0
```

A different install directory:

```sh
curl -LSfs https://github.com/avencera/homebased/releases/latest/download/install.sh | sh -s -- --to /usr/local/bin
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--to` | `~/.local/bin` | Directory for the `homebased` binary |
| `--tag` | latest GitHub release | Release tag, for example `v0.1.0` |
| `--git` | `avencera/homebased` | GitHub repository that hosts the release |

The script overwrites an existing `homebased` in the install directory. If the
daemon is running, the script restarts it with the new binary.

It supports Linux (`x86_64` and `aarch64`) and macOS (Intel and Apple silicon).

### Daemon

The install script does not start the daemon on first install. If the daemon is
already running, the script restarts it with the new binary. After `homebased`
is on `PATH`:

```sh
homebased daemon install
homebased --json daemon status
```

`daemon status` must report `"socket": "up"`. Linux writes `~/.config/systemd/user/homebased.service`. macOS writes `~/Library/LaunchAgents/dev.praveen.homebased.plist`. Run install from a shell where `codex`, `claude`, `grok`, `opencode`, and the project toolchains are on `PATH`. The unit stores that `PATH` and the absolute agent paths, including `HOMEBASED_OPENCODE` when OpenCode is installed. The macOS plist also sets `CLAUDE_CONFIG_DIR`, from the installing shell or `~/.claude`, so Claude agents share the login file that SSH sessions use instead of a separate keychain copy.

Homebased reads `~/.config/homebased/config.toml` when the daemon starts. A missing file at this default path means that Fleet is disabled. To use another file, set `HOMEBASED_CONFIG` or pass `--config <path>`. `daemon install` validates an explicit config path and stores its absolute path in the host unit. This keeps the same config after logout or restart. Restart the daemon after you edit its config.

The HTTP listener is off by default. Fleet peers use this listener. The dashboard is also served here when its assets are built. There is no application token: any peer that can reach the listener can use Fleet routes and can read every regular file available to the daemon user through the dashboard file browser. Text, raster images, and HTML open on a separate content origin; other files download.

### Dashboard over HTTPS (Tailscale)

The dashboard listener speaks plain HTTP. To reach it over HTTPS from a
tailnet, put `tailscale serve` in front of it. File previews load raw bytes
from a second listener, the content origin, so that listener needs a proxied
port too. By default the OS picks the content port at each start, which a
proxy cannot be set up for ahead of time. Pin it in
`~/.config/homebased/config.toml`:

```toml
[dashboard]
# where the proxy serves this dashboard; Fleet peers link here from https pages
https_url = "https://<machine>.<tailnet>.ts.net:7678"
# plain-HTTP port of the content origin; omitted keeps an OS-chosen port
content_port = 7679
# port where an HTTPS proxy such as tailscale serve fronts content_port
content_https_port = 7680
```

Then proxy both listeners and restart the daemon:

```sh
tailscale serve --bg --https=7678 http://127.0.0.1:7677   # dashboard
tailscale serve --bg --https=7680 http://127.0.0.1:7679   # content origin
homebased daemon restart
```

Open `https://<machine>.<tailnet>.ts.net:7678/`. A page served over HTTPS
loads previews from `https://<machine>.<tailnet>.ts.net:7680/`; a page served
over plain HTTP keeps using `content_port` directly.

A machine shares its `https_url` with its Fleet peers. A dashboard opened over
HTTPS links each peer's tasks and queue at that peer's `https_url`, because
browsers upgrade or block a plain-HTTP link from a secure page. A peer without
`https_url` is linked at its plain-HTTP Fleet address, which only works from a
page opened over plain HTTP. `https_url` must be an `https://` base URL with no
path.

**Add to Home Screen** on an iPhone gives the dashboard a checklist icon named
"Homebased" that opens full screen, from `web/static/manifest.webmanifest` and
the `web/static/icon-*.png` files beside it. The browser tab shows the same
checklist from `web/static/favicon.svg`. Link previews in Messages, Slack, and
other apps show the `web/static/og.png` card. Once the page loads, it points its
`og:image` tags at the square icon, so Safari's share sheet shows the checklist
rather than a cropped card.

`content_https_port` must differ from `content_port`. With
`HOMEBASED_WEB_LISTEN=0.0.0.0:7677` the daemon holds its ports on every
interface, so the proxy cannot listen on the same port number on the
machine's tailnet address. The same goes for the dashboard: it is served on
`7678` rather than `7677`. `content_https_port` requires `content_port`, and
neither may be `0`. If `content_port` is busy, the daemon logs a warning and
runs without a content origin rather than moving to another port, so file
previews fail until you free the port and restart.

### Push notifications (ntfy)

Add this section to `~/.config/homebased/config.toml` on every machine that
owns agent threads:

```toml
[notify.ntfy]
topic = "homebased_k7q2xm4p"
# server = "https://ntfy.sh"
# token_file = "~/.config/homebased/ntfy-token"
```

Anyone who knows an ntfy topic can read its notifications. Choose a hard to
guess topic and set the config and token files to mode `600`. The daemon reads
the config at startup, so restart it after you edit this section. Send one test
push with:

```sh
homebased notify test
```

### Keep the host awake (macOS)

On a Mac that serves agents or tunnels, add this section to
`~/.config/homebased/config.toml`:

```toml
[power]
keep_awake = true
```

While the daemon runs, it holds a `PreventSystemSleep` power assertion, the
same one that `caffeinate -s` takes. On AC power, the system does not sleep,
and a Mac that is already asleep stays awake after its next dark wake. The
display can still sleep, and macOS ignores the assertion on battery power.
The weaker `caffeinate -i` assertion does not work here: it does not stop a
dark wake from going back to sleep. A normal user can create this
assertion, so it needs no `sudo pmset` change, and updates and reinstalls keep
it. When the daemon stops, macOS releases the assertion. Check it with:

```sh
pmset -g assertions | grep homebased
```

Sleep drops outbound connections, such as a T3 Connect tunnel, and no
incoming traffic wakes the Mac to restore them. SSH still works during sleep
because "Wake for network access" wakes the Mac for an incoming connection.
Linux does not support this setting; the daemon logs a warning and ignores it.

To let other Fleet machines reach this daemon, set `HOMEBASED_WEB_LISTEN` to a reachable host:port before install. The host unit stores this address, and a later install from a shell without the variable keeps it; set it to `off` to turn the listener off. Use a Tailscale or LAN address on a trusted network, or `0.0.0.0:7677` to listen on all interfaces. You can also set `--web-listen` when you run the daemon directly. A loopback address cannot receive requests from other machines.

Use `homebased daemon stop` and `homebased daemon restart`. Do not use raw `systemctl` or `launchctl`.

### T3 Code wake

Homebased can wake stopped Claude sessions and T3-owned Codex threads through
T3 Code's undocumented local API; if a Codex wake fails, it queues the event
with `codex queue`. It needs T3 Code 0.1.0 or later; an older server fails
`homebased t3 check` with a request to update T3 Code. T3 keeps a Claude
process between turns, so an event for a live Claude session that a T3 thread
owns also goes through T3, which shows it in the thread; the session socket is
the fallback. An archived T3 thread is unarchived first, so the turn runs and
you can see it. A thread that T3 imported from its older orchestrator continues
in a new provider session with the earlier context handed over. homebased
reads a T3 thread's context from the session T3 runs its next turn in, so an
imported thread reads its new session, and a thread that switches provider or
starts a new session at its next turn is not compacted first.

Claude Code caches a conversation for an hour. After that, the next turn
re-reads the whole context at full price, and every request in that turn
carries it. Interactive Claude Code compacts a long idle session before the
cache lapses, but T3 runs Claude through the Agent SDK, which does not. So
homebased compacts idle Claude sessions that T3 threads own.

Codex threads that T3 owns get the same treatment. OpenAI publishes no fixed
cache lifetime, but on one machine's Codex rollouts the first request after an
idle gap still read its context from the cache up to about 50 idle minutes,
mostly up to 75, and rarely after that, so homebased treats it as an hour too.
OpenAI charges no cache write premium, so compacting a cold Codex thread costs
about the read its next turn would pay anyway, plus the summary, and every
later request then reads the summary. T3 compacts a Codex thread through
Codex's own compaction.

The daemon checks this machine's T3 threads every minute. Once a thread's
session has been idle for 55 minutes with at least 175,000 context tokens,
homebased sends `/compact` to the thread. A thread that sleeps on work with
[`homebased wait --long`](#sleep-until-long-work-ends) is certain to run again
hours later, after its cache lapsed, so for it the limit is 60,000 tokens; below
that a cold wake-up costs little, and compacting again would lose detail. The cache is still warm, so the
compaction costs a fraction of a cold read. A thread qualifies when it is not
archived, runs no turn, and will likely run again: a homebased task or queue
job, a T3 subagent's report, or a child thread's report still owes it a wake,
or it is unsettled and not a T3 subagent whose task ended. That covers a root
waiting on its T3 subagents or child threads, a thread waiting on you, and an
idle interactive thread alike. A settled thread or a finished T3 subagent
rarely runs again, so compacting it would cost more than it saves. Any activity in the
thread moves the 55 minutes, and each idle period gets one attempt. A queue job
counts until its ending event reaches the thread, or for at most seven days, in
case its queue machine never reports one; a sleep's hold on a job counts toward
the 60,000-token limit for the same time. A terminal session has no T3 thread
to compact, so it is left alone.

An event or message for a session whose cache already lapsed, with at least
175,000 context tokens, sends `/compact` first and then the event. For a task
or job event to a sleeping thread the limit is 60,000 tokens; a direct message
keeps 175,000, since its sender waits for the answer. The event's
turn would write the whole context back to the one-hour cache, while Claude
Code's compaction request writes it at the cheaper five-minute rate, and every
later request then reads the summary instead of the full context. Nothing
compacts a cold thread until a message arrives, so a thread that is never
resumed costs nothing.

A task or queue job event that starts a compaction waits in the homebased inbox
until the transcript or rollout shows the compaction finished, then goes as a
normal turn.
The inbox survives a T3 or daemon restart, while T3 keeps its own queue only in
memory. If the compaction has not finished after 12 minutes, the event goes
anyway. A direct message cannot wait, because its sender needs an answer, so
it goes right after `/compact` and T3 holds it until the compaction finishes.

From 54 idle minutes on, an event for such a session explicitly waits for the
active run, because T3 refuses to steer into a running compaction. For the
same reason an event or message waits for the active run whenever T3's state
shows a `/compact` turn running in the thread, whatever the thread's size.

Homebased reads idle time and context size from the session transcript in
`~/.claude/projects/`, and for a Codex thread from its rollout in
`~/.codex/sessions/`. There the last `token_count` event's `total_tokens` is
the context size Codex shows itself; right after a compaction it is Codex's
estimate of the summary until the next request. Homebased does not follow a T3
Codex provider instance that sets its own `homePath`.

The dashboard's threads page lists open T3 Claude and Codex threads with at
least 175,000 context tokens, with their idle time, whether the cache is still
warm, and for Codex the share of the model's context window. It groups them as threads active in the last hour, threads settled in T3
in the last hour, and active threads idle over an hour, which have a cold
cache. A button reveals settled threads idle up to 48 hours; older settled
threads are not listed. Its Compact button sends `/compact` to one thread. A
second click in the same idle period is dropped. Homebased does not read
settled state from T3 Code 0.1.0, so every thread counts as active.

The daemon serves the same list as JSON at `GET /v1/threads`, on its socket and
on the dashboard port, for this machine's T3 threads. `all=true` lists every
open thread, and `include=<id>,<id>` adds those sessions below the size.
`thread=<prefix>` lists only the threads whose T3 thread id, Claude session id,
or Codex thread id starts with the prefix, at any size, so a client can look up
one thread; an ambiguous prefix lists every match. Each thread has `provider`
(`claude` or `codex`), `session`, `t3_thread`, `title`, `settled`, `tokens`,
`tokens_estimated`, `context_window` (Codex only, else null), `last_active`,
`cache_warm`, and `details`. A thread whose transcript or rollout has no
request yet is not listed. `POST /v1/threads/{session}/compact` is the Compact
button.

Every compaction request, from the idle scan, a cold event, or the dashboard,
is appended to `compactions.jsonl` in the homebased home directory. Each line
records the trigger, provider, session, T3 thread, context tokens, idle seconds,
whether the cache was warm, and T3's answer. The size after compaction is in the
session transcript's `compact_boundary` line, or in the `token_count` event
after the Codex rollout's `compacted` line.

If T3 may have taken an event but its reply was lost, every later attempt goes
only through T3, which drops a repeated send, so the event never arrives twice
through another route. When delivery finally gives up, homebased writes the
event to `callback-fallback.log` and sends a push notification.

Run `homebased t3 check` after T3 updates to check that the local API still
works. It reports the orchestration protocol it found and was verified
against T3 Code 0.1.0. The daemon runs the same check when it starts and whenever T3 restarts, and
sends a push notification only when two checks a minute apart find the same
incompatibility.

A Claude session with a transcript but no live process is waiting, not gone.
Homebased keeps the event in the task inbox and watches
`~/.claude/sessions/`. When a live process for that session id appears, it
sends the event, keeping the task's events in order. `homebased task show`
lists each waiting event with its reason and the time delivery gives up.
After 3 days the event is marked failed and is written to
`callback-fallback.log`.

### Wait for tasks and jobs in a T3 thread

These sections use a few terms for the agents that run work. A T3 subagent is
a T3 `delegate_task` child: short work, typically under an hour, that T3
reports done to its parent once a turn ends with no wait running, so it never
waits on long work. A child thread is a top-level T3 thread launched with
[`homebased t3 launch --child`](#launch-a-t3-thread-on-any-machine) to own a
phase, an experiment, or long work; it reports to its parent thread once, and
homebased then settles it. A worker is a headless agent that homebased runs as
a task, which parks with `waiting`. Long work is a task, job, or chain expected
to run or queue for over an hour; a thread sleeps on it with
`homebased wait --long`.

```sh
homebased wait [--next [--final-only]] <id> [<id> ...]
homebased wait --long <id> [<id> ...]
```

`homebased wait` takes task ids, GPU queue job ids, and thread watch ids alike
and works out which is which. It streams the `HOMEBASED_EVENT` line of each
callback event for 1 to 16 of this thread's tasks, GPU queue jobs, and watches
to stdout and exits 0 once every target has its final outcome. A task follows
`TASK_WAITING` to the continuation run. A job ends on `JOB_SUCCEEDED`,
`JOB_FAILED`, or `JOB_CANCELLED`; its other events are printed and the wait
goes on. A watch ends on `THREAD_WATCH_ENDED`, or on the
`THREAD_CONTEXT_OVER` or `THREAD_STALLED` that ends a `--once` watch. While it runs, the
daemon gives those events to it in place of normal delivery. It counts an
event delivered only once the command confirms it printed the line, so an
event the command never confirmed goes through normal delivery after about 5
seconds, possibly as a repeat. It reconnects silently across a daemon restart
for up to 5 minutes. With `--next`, it exits 0 about a second after it prints
the first event, together with any that arrived in that second. With
`--next --final-only`, interim events such as `TASK_REPORTED` and
`JOB_PREEMPTED` are printed and confirmed but do not end the command; it exits
after a task's or job's final event or any watch event. A T3 subagent (a
`delegate_task` child) uses it so that its interim events do not end its turn
and wake its parent. It is meant
for a background Bash command with `--next` in a Claude Code session in a T3
Code thread, which T3 shows as Waiting. A Claude Code `Monitor` is killed
after at most 30 minutes and wakes the thread each time, while a background
command wakes it only when it exits. Give that command Claude Code's 2-hour
maximum timeout (`7200000`) and let the wait pick its own limit: in a Claude
session that a T3 thread owns, the wait exits after 55 minutes with no event,
so each wake-up lands inside the session's hour-long prompt cache, while a
wake-up after the cache expires rewrites the whole context. On that exit it
prints, on stderr, the command that starts the wait again on the ids still
pending; with `--final-only`, interim events may already be on stdout.
`--timeout <duration>` sets the limit by hand; other callers have none unless
they pass it. A task of another thread or machine and a
job of another thread return `wait_unavailable`, and so do a task or job
submitted without a thread, a job the queue refused or has not confirmed, and a
queue run that ran
here, which has no callback route. An id that is not a task, job, or thread
watch of this machine, including a watch id that does not exist here, returns
`wait_target_not_found`. A watch of another caller returns `wait_unavailable`.
Commands that name a watch, such as `thread unwatch`, return `watch_not_found`
for one that does not exist.

A Codex thread cannot run a background command that wakes it, so each event starts a new turn, and
T3 shows the thread as idle between events.

#### Sleep until long work ends

For long work, expected to run or queue for over an hour, the thread sleeps:
run `homebased wait --long` as an ordinary command, not in the background, and
end the turn. It checks the ids
as `homebased wait` does, prints one result per target on stderr, and exits 0
at once:

- `held`: the daemon now holds the target for this thread
- `already_ended`: its final event has already settled, so nothing is held
- `followed_to <continuation>`: the task parked, and the hold is on the run
  that continues it
- `watched`: a thread watch, whose events all wake the thread anyway

The hold is saved in the daemon's database, so it survives a daemon restart.
While it lasts, the daemon sorts each event of the target:

- A final event wakes the thread through normal delivery and ends the hold.
  Its `HOMEBASED_EVENT` line gains `held_events`: the kind, `seq`, time, and a
  short summary of each event the hold kept
- An alert wakes the thread, and the hold stays: `JOB_ATTENTION`,
  `JOB_BLOCKED`, `TASK_CHECK_DUE`, `TASK_MEMORY_REVIEW`, `JOB_CHECK_DUE`, and the first
  `JOB_GPU_IDLE` of an idle episode. End the turn again after handling it,
  without starting another wait
- Progress settles without waking the thread: `TASK_REPORTED`,
  `JOB_PREEMPTED`, `JOB_GPU_IDLE` reminders, and `TASK_WAITING`, which moves
  the hold to the continuation run so a chain stays held to its end. `task
  show` reads `callback: held` for a run whose `TASK_WAITING` the hold kept

When a held worker parks, the tasks and jobs it names with `--on` are held
with its chain, provided their events go to the sleeping thread, as a
worker's own submissions do. The continuation reports on them, so their final
events count as progress of the chain too: those and their progress events
join the chain's `held_events`, and only their alerts wake the thread. The
thread then wakes on the chain's final event or an alert. `held_events`
entries name their `task` or `job`, and a held `TASK_WAITING` summarizes as
`parked on task <id>, job <id>; continues as task <id>`.

A connected `homebased wait` on the same target still takes its events first.
A sleeping thread compacts at 60,000 tokens instead of 175,000 (see
[T3 Code wake](#t3-code-wake)). A hold is open work for `thread report`. In a
T3 subagent, which T3 reports to its parent as soon as its turn ends, `--long`
cannot sleep: it waits up to 115 minutes like a plain wait and says on stderr
that long work belongs in a child thread. A homebased
worker gets a `usage` error (exit 2) and parks with
`homebased task report --outcome waiting --on <id>` instead.

### Thread context, messages, compaction, watches, and interrupts

```sh
homebased thread context [<ref>...] [--children [--live]] [--machine <name>]
homebased thread read [<ref>] [--before <pos>|--after <pos>|--oldest|--item <pos> [--offset <n>]] [--limit <n>] [--max-chars <n>] [--machine <name>]
homebased thread compact [<ref>] [--machine <name>]
homebased thread watch [<ref>...] --context-above <150k|80%> [--action compact] [--once] [--children]
homebased thread watch [<ref>...] --stalled-after <20m|1h> [--once] [--children]
homebased thread watches [--all]
homebased thread unwatch <watch-id>
homebased thread report --summary <text> [--file <path>|-] [--stop-children]
homebased thread interrupt <ref> [--stop] [--machine <name>]
homebased thread answer <ref> --request <id> --answer <text>... [--machine <name>]
```

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

`thread compact` sends `/compact` to a T3 thread on whichever machine runs it,
and records it in `compactions.jsonl` with trigger `manual`. A thread that is
running a turn, including the calling thread, answers `queued`: T3 compacts it
once that turn ends, and asking again during the same turn queues no second
compaction. Nothing wakes the thread after a queued compaction, so a thread
that should resume on its own afterwards keeps a `homebased wait` running or
watches itself with `--action compact --once` instead. A thread outside
T3 fails with `compact_unsupported_host` (exit 5), and a T3 thread with no
session to compact yet with `thread_not_found` (exit 3).

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

`thread answer` answers a question a T3 thread's agent is waiting on, on
whichever machine runs it (`runtime-request.respond`). Pass T3's request id
with `--request` and one `--answer` per question, in order; an answer that
names an offered option in another case picks that option. A request that is
not pending, a wrong number of answers, or an approval of a command or file
change fails with `thread_control_refused`. Approvals stay with a person in
T3 Code, since they gate what an agent may run.

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
[threads.md](.agents/skills/homebased/references/threads.md) has the payload.

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

On the socket, `POST /v1/threads/context` resolves references and
`GET /v1/threads/watches?caller=<thread>` (or `all=true`) lists watches; both
are also served on the dashboard port. `POST /v1/threads/read`,
`POST /v1/threads/compact`, `POST /v1/threads/watches`,
`POST /v1/threads/watches/{id}/unwatch`, `POST /v1/threads/report`, and
`POST /v1/threads/control` (interrupt, stop, and answer) are socket only.
Fleet peers answer `POST /v1/cluster/threads/context`,
`POST /v1/cluster/threads/read`, `POST /v1/cluster/threads/compact`, and
`POST /v1/cluster/threads/control`.

### Launch a T3 thread on any machine

```sh
homebased t3 projects --machine code
homebased t3 project add ~/code/app --machine code [--title app] \
  [--lucide audio-waveform --color violet | --emoji 🎙️ | --monogram AP --color teal]
homebased t3 project icon ~/code/app --machine code --lucide captions --color sky
homebased t3 launch --machine code --project ~/code/app \
  --message "Fix the flaky upload test" [--worktree main --branch fix/upload] \
  [--clone-if-missing [--clone-into ~/code/app]] \
  [--provider codex --model gpt-6.1-sol --model-option reasoningEffort=high] \
  [--reply-route | --child]
```

`t3 launch` starts an ordinary top-level T3 Code thread in the T3 on another
Fleet machine, or on this one without `--machine`, as if someone had started
it there. The daemon forwards the request to that machine's daemon, which
calls T3's `orchestration.launchThread`: T3 creates the thread, prepares its
workspace (the project root, a new worktree, or `--existing-worktree`), runs
the project setup script, and starts the first message.

- `--project` takes a T3 project id, the project's workspace root on that
  machine (absolute or `~/`), its exact title, or the repository it checks
  out, as `owner/repo` or a git remote URL. A repository matches the project
  whose `origin` remote names it, compared by host and path in lowercase
  without scheme, user, port, trailing slash, or `.git`, so the SSH and HTTPS
  forms of one repository match. `owner/repo` matches on any host; a URL or
  `host/owner/repo` names the host too. An exact title wins over a
  repository of the same name. `t3 projects` lists projects with the model a
  launch uses by default, and lists the provider instances that machine's
  running T3 has enabled under `providers`, which is absent when T3 is not
  running there.
- `--clone-if-missing` clones the repository `--project` names into
  `~/code/<repo>` on that machine, or `--clone-into <dir>`, adds it as a
  project, and launches there, when no project checks it out. `owner/repo`
  clones from GitHub, over SSH when a project there already reaches GitHub
  over SSH and over HTTPS otherwise; a URL clones as given. The clone goes to a
  staging directory beside the target and moves into place once complete, so
  a clone cut short leaves nothing there. A directory already there that is a
  clone of the repository is added as it is, which makes a retry safe; any
  other directory there, or a clone that fails, refuses the launch with
  `t3_launch_refused` and git's reason. Git runs without a terminal, so a
  credential prompt fails the clone instead of waiting.
- `t3 project add <path>` adds an existing directory on that machine as a T3
  project, through that machine's own `t3 project add`, so the running T3 shows
  it at once. A relative path resolves against the current directory, then
  under that machine's home like a `~/` path. A directory that already is a
  project reports that project, so repeating the command is safe. It refuses
  with `t3_project_refused` when the directory does not exist there.
- The icon flags set the icon T3 shows for the project in place of its
  favicon, on a project just added or one already there: `--lucide <name>` (a
  [Lucide](https://lucide.dev/icons) name) or `--monogram <text>`, each with
  `--color`, or `--emoji`. `--clear-icon` removes it. `t3 project icon <path>`
  changes only the icon and refuses a directory that is not a project yet. T3
  checks that a Lucide name is kebab-case but not that the icon exists, so an
  unknown name shows a blank icon. A machine whose homebased predates icons
  refuses the request instead of ignoring the icon.
- Without `--provider` and `--model`, the thread runs the project's default
  model from that machine's T3 settings. When neither the settings nor the
  project name one, the launch fails and asks for both. `--provider` is an
  instance id on that machine, such as `claude`, not a driver name. T3
  refuses an instance that is disabled or missing there, explicit or from the
  default, and the `t3_launch_refused` error adds the instances it has
  enabled.
- The thread runs `full-access` unless `--runtime-mode` says otherwise, since
  nobody on that machine is there to approve its actions. `--plan` starts it
  in plan mode.
- The new thread is a hand-off: nothing in it points back to the thread that
  launched it. `--reply-route` ends the first message with the calling thread
  and machine and the `homebased message send` command that reaches them, so
  the new agent can report back; `--source-thread <id>` names another thread.
- `--child` launches a child thread of the calling T3 thread instead of a
  hand-off, to own a phase, an experiment, or long work. Its first message
  ends with a route back to the parent's T3 thread, tells the child to sleep
  on its long work with `homebased wait --long`, and asks it to report
  with `homebased thread report`, and to report again as a follow-up if a later
  message reopens it for more work.
  homebased records the link on both machines, so the parent lists the child
  under `thread context --children` and shows `waiting` until it reports, and
  the child's daemon settles it after its reporting turn ends. A child can
  launch children of its own. `--child` needs `--message` or
  `--message-file`, must run in a T3 thread on the calling machine, and cannot
  be combined with `--source-thread`. A retried `--request-id` returns the
  same thread and link. A machine whose homebased predates child threads
  refuses a `--child` launch with `t3_incompatible`.
- The output has the thread id and a `t3code://threads/<environment>/<thread>`
  link that opens it in T3 Code.
- T3 launches once per `--request-id`, so repeating the command with the same
  id after `t3_launch_outcome_unknown` returns the same thread.
- With `--worktree`, the launching machine checks that the base resolves to a
  commit in the project's checkout before it asks T3, fetching its branch
  from origin first with `--from-origin`, as T3 then does. `--from-origin`
  fetches branches only, so a commit id or an expression such as `main~2`
  must already be there. A missing base fails the command with
  `t3_worktree_base_missing` (exit 3). Its `input.fetched` is `true` when the
  branch was fetched and origin has no such branch, and its
  `input.repository` is the project's `host/owner/repo` when its origin
  names one. A fetch that cannot run, such as one that times out, leaves the
  decision to T3. When the command runs in a checkout whose origin is that
  same repository, the calling machine looks the base up there and says what
  it found in `input.local.kind`: `unpushed_branch`, which needs a push and
  `--from-origin`; `unpushed_commit` (with `input.local.commit`), which needs
  a pushed branch at that commit to launch from with `--from-origin`; or
  `pushed`, a branch origin already has, which needs `--from-origin`. A
  project that is not a git repository refuses a worktree with
  `t3_launch_refused`. Other workspace failures, such as a branch name that
  already exists there, still fail in the new thread or T3's own check.

A machine whose homebased predates these commands answers `t3_incompatible` and
needs `homebased update`. A `~/` path, quoted or expanded by the shell,
resolves under the home of the machine that launches the thread.

### Fleet

Fleet is off by default. Add this config to each machine that should join the
same Fleet. Each `address` is the HTTP base address of another Homebased
listener. The port defaults to `7677` when it is not in the address.

```toml
[fleet]
enabled = true
machine_name = "code"

[fleet.discovery]
mdns = true
tailscale = true
tailscale_port = 7677

[[fleet.machines]]
address = "http://main:7677"

[[fleet.machines]]
address = "http://training:7677"
```

Use a different unique `machine_name` on each installation. `mdns` is on by
default. Tailscale discovery is optional. Fleet uses plain HTTP. Use it only on
a trusted LAN or tailnet. Fleet does not add authentication.

Validate the file, then restart the daemon so it loads the changes:

```sh
homebased --json config validate
homebased daemon restart
```

Use the Fleet commands to check discovery and manage addresses:

```sh
homebased --json fleet machines
homebased --json fleet discover
homebased --json fleet probe code
homebased --json fleet add http://training:7677
homebased --json fleet remove http://training:7677
```

`fleet add` saves an explicit address in Homebased state. It does not edit
`config.toml`. A configured address stays managed by the config file.
`fleet remove <machine-uuid>` forgets a known machine, but a configured address
will be probed again.

The full operator workflow is in [the Homebased skill](.agents/skills/homebased/SKILL.md).

### Skill

The operator skill is [`.agents/skills/homebased`](.agents/skills/homebased). Copy it into the agent skills directory:

```sh
mkdir -p ~/.agents/skills
cp -R .agents/skills/homebased ~/.agents/skills/homebased
```

From GitHub without a local clone:

```sh
npx skills add git@github.com:avencera/homebased.git --skill homebased -g -y
```

Codex and Cursor read `~/.agents/skills/homebased`. For Claude Code, also install into `~/.claude/skills/homebased`, or pass `-a claude-code` to `npx skills add`. After install, read `SKILL.md`. That file is the operator contract.

## Usage

Run a command with `task run`, or submit a JSON spec for an agent or container
workload. The local daemon can run the task on this machine or send it to a
Fleet machine. The submitting machine owns the Codex thread and returns events
to it. Tell the user the task id, and end the turn. Results arrive later as one
line: `HOMEBASED_EVENT {...}`. Do not poll a running task.

```sh
homebased --json daemon status                         # socket must be "up"
homebased task run --name "release build" -- cargo build --release
homebased --json task submit --spec "$spec" --dry-run  # validate, print child argv
homebased --json task submit --spec "$spec"            # submit to the local machine
```

`task run` builds a `task` spec from its flags: it runs the command after `--`
in the current directory (`--cwd` overrides), names it after the command unless
`--name` is given, and takes `--timeout`, `--machine`, `--after`,
`--wait-for-machine`, `--mem-peak`, `--priority`, `--gpu-visible`, `--no-envrc`,
`--dry-run`, and `--request-id`.

Always pass `--json` on data commands. Every JSON object carries `api_version: 1`. `--request-id` is an optional CLI flag for a stable retry identity; it is not a task field.

### Workloads

| Work | `workload.type` |
| --- | --- |
| Long commands: `cargo build`, test suites, CI watchers | `task` |
| A model must reason and produce a report | `agent` |
| Work in a pinned Docker image, such as checkpoint evaluation | `container` |

A `task` runs an argv array with no shell. A caller that needs shell syntax must request it, for example `["sh", "-lc", "..."]`. An `agent` runs Codex, Claude, Grok, or OpenCode with a prompt file. Prefer `task` unless a model must reason.

A `task` also gets the direnv environment of its `cwd`, the same variables a terminal opened there would have, such as key paths that a repository's `.envrc` exports for a release. Before the command starts, the executor runs the `direnv` found on the absolute entries of the task's `PATH` and loads only an `.envrc` (never a `.env`, even with direnv's `load_dotenv`) that direnv already allows for that directory; Homebased never runs `direnv allow`. The first line of `output.log` names the file and the variables it set, never their values. When an `.envrc` exists but does not load, because direnv has not allowed or has denied it, is not installed, or fails, the command runs without it and that line says why. Cancelling the task while the `.envrc` loads stops it and never starts the command. The `.envrc` runs with the task's GPU selection, so it sees only the devices the command may use. Variables Homebased sets, such as `HOMEBASED_TASK_ID`, take precedence. Set `"envrc": "ignore"` on the workload, or pass `task run --no-envrc`, to run without it. GPU queue job steps of type `task` load it too, except in a CI job with a `git` checkout, which runs without one: the clone holds the previous commit until the checkout wrapper runs. Agent and container workloads never load an `.envrc`: an agent keeps the environment it was configured with, since an `.envrc` could switch its account or API key, and a container gets only the variables its spec names. The program itself is resolved on the submit-time `PATH`, so a tool that only the `.envrc` adds to `PATH` needs an absolute path or `["sh", "-c", "..."]`.

A `container` runs an image pinned by digest, with typed fields for the entrypoint, arguments, GPUs, memory limit, user, working directory, mounts, and environment. Homebased builds the Docker calls itself: it creates the container, saves its ID, starts it, streams its logs to `output.log`, waits for it, and removes it. The task exit code is the container exit code. If the watching worker stops, the container keeps running and the daemon adopts it. Docker options that the fields do not model, such as privileged mode, are refused. See [submit.md](.agents/skills/homebased/references/submit.md) for the fields.

Claude agent workloads use streaming JSON output by default, so `output.log` records progress during a turn. Haiku workloads (`haiku` or a `claude-haiku-*` model ID) also use `--autocompact 100k` by default; an explicit `--autocompact` in `extra_args` replaces that default. A caller can select a different Claude output format with `extra_args`; exact spellings of Homebased-managed standalone switches are reserved tokens there, so Homebased treats every exact match as that switch, not as another option's value, and emits each at most once. Homebased also drops, with their values, options that the agent would reject because Homebased already sets them: Codex `-C`/`--cd` and `-s`/`--sandbox` (Codex always runs with full access), Grok `--cwd`, and Codex or Grok `-m`/`--model` when the workload sets `model`. Other extra arguments keep their order and spelling.

OpenCode agent workloads use `opencode run --standalone` with JSON events and the prompt feed on stdin. `model` is optional and is passed as one provider-qualified value, such as `zai-coding-plan/glm-5.3-flash` or `provider/model#variant`. OpenCode gets full work-tool permissions for that child only. Homebased sets the child `PWD` to `cwd`, adds a generated primary agent, and leaves persistent OpenCode configuration unchanged. An inherited `OPENCODE_CONFIG_CONTENT` value may contain JSONC; Homebased preserves its unrelated settings and rejects malformed content or a generated-agent name collision. OpenCode extra arguments cannot replace the agent, working directory, model, prompt, session, server, or standalone mode.

### Spec

Submit specs use `api_version: 1` and a `workload` object. The required top-level `name` is a short goal label for the dashboard and events. Name the work, not the agent or the CLI. `thread` is the Codex thread or Claude Code session UUID that should receive events. Submit fills in an omitted `thread`: a Homebased worker gets its parent task's thread, and anyone else gets the calling session from `CODEX_THREAD_ID`, `CODEX_SESSION_ID`, or `CLAUDE_CODE_SESSION_ID`. Submitted from a plain terminal, or with `"thread": null`, a `task` or `container` workload runs without a thread: its events are recorded and shown by `task show` and the dashboard, but no session is woken. `cwd` is the host directory the child runs in, and it must already exist on the machine that runs the task; for a `container` it is a host path, not a path inside the container. Omit `machine` to run on the local machine. Set `machine` to a Fleet machine name to run there. The remote machine checks `cwd` on its own file system. For remote agent work, `prompt_file` must be an absolute path on the submitting machine; the CLI reads the file and sends its text.

`timeout` is an output-inactivity timer (default `1h`, minimum `30m`). Homebased resets it when `output.log` receives bytes. If the live child produces no output for the full timeout, Homebased sends `TASK_CHECK_DUE` and leaves the child running. Only explicit cancel, a signal, or process exit stops the child.

Codex agent specs may set `workload.resume_thread` to resume a Codex thread
with the new prompt. Claude workers discard their session at exit unless the
spec sets `workload.persist_session: true`; a Claude spec's `resume_thread`
forks such a saved session into the new task's session. Use `task followup` for
a finished Codex task, or a Claude task that saved its session, so Homebased
uses its recorded worker thread and keeps the task settings.

Agent example:

```json
{
  "api_version": 1,
  "name": "implement file browser",
  "cwd": "/path/to/project",
  "timeout": "30m",
  "workload": {
    "type": "agent",
    "agent": "claude",
    "model": "fable",
    "prompt_file": "/tmp/prompt.md"
  }
}
```

Task example:

```json
{
  "api_version": 1,
  "name": "cargo release build",
  "cwd": "/path/to/project",
  "timeout": "2h",
  "workload": {
    "type": "task",
    "command": ["cargo", "build", "--release"]
  }
}
```

To run the task on a Fleet machine, add `machine` at the top level of the
spec. This example is a remote task spec:

```json
{
  "api_version": 1,
  "name": "run cargo checks on code",
  "machine": "code",
  "cwd": "~/code/homebased",
  "timeout": "2h",
  "workload": {
    "type": "task",
    "command": ["cargo", "check"]
  }
}
```

Run a remote dry run to validate against the selected executor and see its
child argv. A dry run does not submit a task or store a request identity.

```sh
request_id="$(uuidgen)"
homebased --json task submit --spec remote.json --request-id "$request_id" --dry-run
homebased --json task submit --spec remote.json --request-id "$request_id"
```

Keep the same request UUID if the submit response is lost. Retry with that UUID;
do not make a new request UUID for the same intended task. Without
`--request-id`, the CLI creates a new one for each submit command. Local submits
use request UUIDs the same way, and the CLI retries a local submit whose outcome
is unknown, or that meets `daemon_busy`, for about 90 seconds.

### Start after other tasks

Set `after` to a list of task or GPU queue job UUIDs to start a task only when
those tasks and jobs succeed. The daemon that accepts the submit holds the task
and starts it itself, so the orchestrator does not have to be running when the
last dependency finishes.

```json
{
  "api_version": 1,
  "name": "phase b2: integrate",
  "cwd": "/path/to/project",
  "after": ["01a0b06f-306c-749e-aa9e-9e1a619ee915"],
  "workload": { "type": "task", "command": ["cargo", "test"] }
}
```

Each dependency must be a task or job submitted through the same daemon, local
or remote; held work can be a dependency too, so chains work. Success means
`TASK_SUCCEEDED`, or `JOB_SUCCEEDED` for a job; `JOB_FAILED` and
`JOB_CANCELLED` cancel the held task, and `JOB_PREEMPTED` does not. A job's
events come back to the machine that submitted it, so a GPU job on another
Fleet machine can feed a CPU task here. The submit returns `held` while a
dependency is pending, or `queued` when all of them already succeeded. When a
dependency ends any other way, the held task is cancelled before it starts and
its thread gets `TASK_CANCELLED` with a `cancel_reason`. A held task starts
with the environment and callback context saved at submit. `task cancel`
cancels a held task before it starts. Tasks held on a cancelled task are
cancelled too.

### Wait for an offline machine

A remote submit fails with `machine_unavailable` when its machine does not
answer. Set `wait_for_machine: true` to hold the task on the submitting daemon
instead, for a laptop that sleeps. Before each send, the daemon asks the
machine whether it takes the task now. The submit returns `queued` when the
machine takes it at once, and `held` otherwise; the daemon then asks again
each time the machine answers, and sends the task once it says yes.

A Mac with a battery says yes only after someone has used it for two minutes
without a break. A laptop also answers during a dark wake, a few seconds of
network with the lid closed, and would sleep again while the task ran; in a
dark wake, or with the display asleep from idleness, no one is using it, so
the task keeps waiting. Once someone uses the machine, the task starts within
about three minutes. A Mac without a battery, and any Linux host, takes the
task as soon as it answers. Set `power.waiting_tasks` to `"always"` or
`"when_in_use"` in the machine's config to choose; `homebased config
validate` shows the result:

```toml
[power]
waiting_tasks = "always"
```

An executor from a release without this question takes the task as soon as
it answers.

Set `coalesce` with `wait_for_machine` for work where only the newest request
matters, such as an update. Once a submit with a `coalesce` key is accepted,
every older task with the same key and machine that still waits on this daemon
is cancelled with `cancel_reason` `superseded`. A task whose launch already
began is left alone.

```json
{
  "api_version": 1,
  "name": "fleet update",
  "cwd": "~/",
  "machine": "mba",
  "wait_for_machine": true,
  "coalesce": "fleet-update",
  "workload": { "type": "task", "command": ["cmd", "fleet", "update", "--local"] }
}
```

`task list` on the submitting machine shows waiting tasks as `held`, and
`task show` adds `wait_for_machine` and `coalesce`. `task cancel` cancels a
waiting task before it starts. Only the submitting daemon holds the task, so
two machines that each submit with the same key both send theirs.

### GPUs are hidden from tasks

A `task` or `agent` runs with `CUDA_VISIBLE_DEVICES` set to empty, so a CPU
step that imports a CUDA library cannot take GPU memory from the GPU queue.
Set `"gpu": "visible"` in the spec, or pass `task run --gpu-visible`, to run
the command with `CUDA_VISIBLE_DEVICES` unset, so it sees every GPU. GPU queue
jobs are unchanged: each run sees the GPU its resource names.

### Wait for host memory

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
free. Work without `mem_peak` starts as before and is counted only through
memory in use.

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

### Long commands inside an agent worker

An agent worker must not wait in the foreground for a long command or detach
it: headless `claude -p` ends with its turn and kills what it started, so
Claude workers run with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`. The report
trailer tells every worker to submit the command as its own task, or as a GPU
queue job, instead, then report `waiting` with notes for later and exit:

```bash
homebased task report --outcome waiting --on <task-or-job-id> \
  --summary "<what is running and why>" --notes-file - <<'NOTES'
<what each outcome means and the next steps>
NOTES
```

The orchestrator gets `TASK_WAITING`, unless it sleeps on the worker (see
[Sleep until long work ends](#sleep-until-long-work-ends)). Homebased holds a
continuation until every named task ends and every named job sends its final
event (`JOB_SUCCEEDED`, `JOB_FAILED`, or `JOB_CANCELLED`), with any outcome,
then starts it with the notes: a
Codex continuation resumes the run's thread when one is known and receives
only the continuation block. Otherwise it starts a fresh session with the
original prompt plus the block. Other agents always start a fresh session
with the original prompt plus the block. The runs of one unit of work
form a chain of at most 20 runs. A task held `after` any run waits for the
chain's last run, and `task cancel` on any run cancels the run that owns the
work now. The worker, its origin, and every waited task must be on one
machine, and every waited job must be submitted from it; the job itself may
queue on another machine. Name a job by its job id, not by a run's task id.

`homebased task schema` prints the JSON Schema. The full field contract is in [`.agents/skills/homebased/references/submit.md`](.agents/skills/homebased/references/submit.md).

### Inspect and cancel

```sh
homebased --json task list --status running,queued
homebased --json task show <id>
homebased --json task followup <id> --message "The missing detail is X. Continue and report."
homebased task log <id> --tail 200
homebased --json task cancel <id>
```

`task show` and `task list` include `worker_thread`. A Claude task records its
session id when it starts running. A Codex task records the thread from its
session header when it ends, whether it succeeds, fails, is cancelled, or is
lost.
`task followup` accepts `--message` or `--message-file`, and resumes only a
terminal Codex task with a recorded worker thread or a terminal Claude task
submitted with `persist_session: true`. Only one follow-up can resume a Codex
thread at a time. `resume_thread_busy` names the active task; wait for its
event before trying again. Each Claude follow-up forks the saved session, so
several can branch from one task.

Run `task followup` on the task's origin or execution machine. A request from
another machine returns `followup_wrong_machine`. Use `--request-id <uuid>` to
retry a follow-up after a lost response. Use
`--allow-other-thread` only when a worker must send events to a thread other
than its parent task's thread.

Task ids are full UUIDs. Prefix matching does not exist. `task list` shows tasks
stored on the local machine, including tasks held here with status `held`. `task show`, `task log`, and `task cancel` use the
local daemon and can find or operate on a task in the known Fleet. A show result
includes `origin_machine`, `execution_machine`, `found_on`, and `availability`.
When the executor is offline, show can return cached origin state. Log data
comes from the executor and is unavailable while that machine is offline. When
the dashboard is enabled, `daemon status` reports its URL.

Errors with `--json` use the versioned error envelope. For example,
`submission_outcome_unknown` means the executor may have accepted the task. Use
the same `--request-id` to resolve or retry. Without a retry, the origin daemon
asks the executor again about once a minute and saves its answer, so the task
stops counting as open work once the executor answers. `cluster_lookup_incomplete` means
one or more known peers could not be checked. Retry the lookup; the daemon only
returns `task_not_found` after every machine in the current known Fleet gives a
definitive negative result.

### Retention

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

### Worker usage

Claude workers run without session persistence unless submitted with
`persist_session: true`, so their tokens usually never reach
`~/.claude/projects`. Homebased reads Claude Code's accounting from each
finished worker's `output.log` and keeps it per task:

```sh
homebased task usage --since 7d --by thread
homebased --json task usage --since 24h
```

The report covers workers that ran on this machine. The JSON has totals,
groups by model, day, and thread, and each task with its thread, cwd, task
directory, and tokens and list-price cost per model. A task's `thread` is the
Claude Code session or Codex thread that submitted it, so the report can be read
together with the session transcripts. A worker stopped before Claude Code's
final accounting is marked `complete: false`; its tokens and cost are lower
bounds. The terminal event of a Claude worker that ran on its submitting
machine carries the same `usage`, and the dashboard's usage page shows the
report. Tasks that ended before this was recorded get their usage when the
daemon starts.

### Events

Delivery is at-least-once. For new events, use `(task, seq)` to detect a
duplicate. Old events can lack `seq`; for those events, use `(task, event)`.
New event payloads also include `origin_machine` and `execution_machine`.
Only the origin machine sends `codex queue` to the original thread. It uses the
environment and directory captured when the task was submitted. The executor
runs the child and owns its process state, reports, and logs. A network outage
does not stop the child. The executor keeps unacknowledged events and retries
delivery. Callback delivery can repeat after a crash, so use the event identity
to ignore duplicates.

An event for a thread that is running a turn waits for that turn to end. Only
the events that need action before then steer into the running turn: the
alerts (`TASK_CHECK_DUE`, `TASK_MEMORY_REVIEW`, `JOB_ATTENTION`, `JOB_BLOCKED`,
`JOB_CHECK_DUE`, and the first `JOB_GPU_IDLE` of an idle episode), a
`--notify` report's `TASK_REPORTED`, `THREAD_CONTEXT_OVER`, `THREAD_STALLED`,
and `SERVICE_EXITED`.

| `event` | Meaning |
| --- | --- |
| `TASK_REPORTED` | Worker sent an interim report. The process is still running. |
| `TASK_CHECK_DUE` | No output for the full inactivity timeout. The child is still live. |
| `TASK_MEMORY_REVIEW` | For ten minutes, the part of this running task's `mem_peak` it has not allocated alone kept the first task waiting for memory from starting. Sent once per task. The child is still live and keeps its peak. |
| `TASK_WAITING` | An agent worker submitted long tasks or GPU queue jobs, reported `waiting` on them, and exited. Homebased continues the work in a new run, `continuation`, once every task and job in `waiting_on` ends. |
| `TASK_SUCCEEDED` | Exit 0 and last report was `succeeded`, or exit 0 with no report from a task, a container, or an agent without the report trailer. |
| `TASK_BLOCKED` | Last report was `blocked`. The process has exited. Answer and submit a new spec. There is no resume. |
| `TASK_FAILED` | Failed report, non-zero exit, signal, or spawn failure. Also an agent with the report trailer that exited 0 without reporting (`reason: "no_report"`). |
| `TASK_CANCELLED` | `task cancel` or `daemon stop --yes` ended it. With `cancel_reason`, a held task was cancelled before it started: a dependency ended, `task cancel` asked, or a newer task with the same `coalesce` key replaced it (`superseded`). |
| `TASK_LOST` | Worker disappeared without `exit.json`. |
| `THREAD_CONTEXT_OVER` | A watched thread's context passed the watch's limit while it ran no turn. Identified by `(watch, seq)`. With `compaction.outcome: "started"` on a thread other than the caller, only a running `homebased wait` gets it. `watch_ended` is true, with `end_reason: "once"`, when it ends a `--once` watch, which then sends no `THREAD_WATCH_ENDED`. |
| `THREAD_STALLED` | A thread watched with `--stalled-after` sat idle past the grace with nothing due to wake it and its work not done. Identified by `(watch, seq)`. It fires once per idle stretch. `watch_ended` is true, with `end_reason: "once"`, when it ends a `--once` watch. |
| `THREAD_WATCH_ENDED` | A watch's last event, unless a `--once` watch fired, with `reason`: `unwatched`, `archived`, `deleted`, `session_gone`, `no_context_window` (a percent watch on a thread that runs Claude Code), `finished` (the T3 thread stayed done and quiet for an hour), or `caller_gone` (the caller's T3 thread was archived, deleted, or stayed done and quiet for an hour). Only a running `homebased wait` gets `unwatched`, `finished`, and `caller_gone`. |
| `SERVICE_EXITED` | A service that was ready exited on its own, or its worker was lost. Identified by `service`; each service sends at most one. See [Services](#services). |

### Services

A service is a long-lived process that an agent starts and does not wait on,
such as a dev server, a binding proxy, an Android emulator, or a phone test
runner. Run it with `service start` instead of a background shell command, which
dies with the agent's shell and is never cleaned up:

```sh
homebased service start --name api --port 8787 -- pnpm wrangler dev --port 8787
homebased service start --name emulator --ready-line 'boot completed' --ready-timeout 5m -- emulator -avd Pixel_8
homebased --json service list                # this thread's live services and their state
homebased service log <id> --tail 100
homebased service stop <id>                  # idempotent
```

The daemon runs the service on this machine, detached from the caller's shell,
in the current directory or `--cwd`, with the caller's whole environment, as a
background shell command would have. A `task run` command gets the daemon's
environment with only the caller's `PATH` and `HOME`, because it may start
later or on another machine; a service starts now, here, on the caller's
behalf, so it gets the tools, versions, and tokens the caller sees. The environment goes to the service's worker in memory and is not stored.

`start` returns once the service is ready and prints its id. With `--port`, it
is ready when `127.0.0.1` or `::1` accepts a TCP connection on that port. With
`--ready-line`, it is ready when a line of its output matches the regular
expression. With neither, it is ready when it spawns. If it exits or is not
ready within `--ready-timeout` (default `2m`), Homebased stops it, prints its
last 20 output lines on stderr, and fails with `service_not_ready`.

A service belongs to the calling thread, found as for `task run`. It is
stopped, with its whole process group, by `service stop`, when that thread's T3
thread is archived, settled, finished with its delegated task, or deleted, or
when `--max` (default `12h`) elapses. None of these sends an event. If the
service exits on its own after it was ready, the thread gets one
`SERVICE_EXITED` event with its exit and last output lines, through normal
callback delivery. A service has no output-inactivity check. Services are kept
apart from tasks: `task list`, `homebased wait`, and task callbacks never see
them, and `daemon stop` or `restart` leaves them running.

### Direct messages

Send a message to a Claude Code session or Codex thread on a known machine.
`--thread` takes any thread reference that `homebased thread` takes, looked up
on that machine; a UUID the lookup cannot find is sent as given. The daemon
looks it up after it checks the message's saved binding, so a retry with the
same `--message-id` reaches the thread the first attempt chose:

```sh
homebased --json message send \
  --machine code \
  --thread <thread-ref> \
  --source-thread <source-thread-uuid> \
  --message "Please review this change."
```

The CLI can also select a Codex thread by receiver-side `--cwd`, or send to
the origin thread for a task with `--task <task-uuid>` alone. This is the
origin thread, not the task worker. A send to its own source thread returns
`message_to_self`; use `task followup` for a finished worker. A Claude
session with no live process gets the message as a new turn in the T3 Code
thread that owns it, and so does a live session that a T3 thread owns. A
Codex thread that T3 owns also gets it as a T3 turn, and any other Codex thread
gets it through `codex queue`. A message to a thread that is running a turn
waits for that turn to end; `--steer` delivers it into the running turn
instead, for a stop, a correction, or an answer the turn is blocked on. Pass the same
`--message-id <uuid>` to retry after a lost response. Delivery is synchronous
and at-least-once. A receiver crash before it stores the receipt can lead to a
duplicate message. Read [messages.md](.agents/skills/homebased/references/messages.md)
for source, reply, and retry rules.

Use `--worker <task-uuid>` alone to send new instructions to a running Claude
worker. The daemon sends to the task's execution machine and `worker_thread`.
A worker message always steers, and the worker reads it at its next turn boundary; if it finishes its final
turn first, the message is never read, and the sender cannot tell. A held or queued
task, or a running Codex worker, which records its thread only at exit, returns
`worker_message_unavailable` with `reason: no_worker_thread`. A finished task
returns `reason: terminal`.

The operator skill [`.agents/skills/homebased/SKILL.md`](.agents/skills/homebased/SKILL.md) is the contract for submit, events, inspect, errors, and worker sessions.

## Update

After the first install, replace the binary from GitHub and restart the daemon (the dashboard lives in the same process):

```sh
homebased --json update
```

A specific release:

```sh
homebased --json update --tag v0.2.0
```

`--dry-run` prints the tag, target, and destination without downloading or restarting. Running workers are separate processes; they keep running across the restart.

Update every Fleet machine to the same version. Machines must run the same homebased version to see held tasks across the Fleet: until an older machine updates, it reports a newer peer that has held tasks as unavailable.

### From this repository

```sh
just release local
```

The build installs `homebased` to `~/.local/bin` and restarts a running daemon.
