# Services

Read this before starting a process that keeps running and that nothing waits on to finish: a dev server, a binding or fixture proxy, an Android emulator, a phone test runner. Use `homebased service start` for these instead of a background shell command. A background command dies with the agent's shell or turn, keeps a T3 thread listed as running, and is never cleaned up when the thread is done. Use `homebased task run` instead for a command whose end you wait on, such as a test suite or a build.

## Start

```bash
homebased service start --name api --port 8787 -- pnpm wrangler dev --port 8787
homebased service start --name fixtures --ready-line 'listening on' -- python3 server.py 8765
homebased service start --name emulator --ready-line 'boot completed' --ready-timeout 5m -- emulator -avd Pixel_8
homebased service start --name watcher --max 2h -- sh -c 'cargo watch -x check'
```

- The command runs on this machine under the daemon, detached from your shell, in the current directory or `--cwd`. There is no shell: use `-- sh -c '...'` for pipes, `&&`, or redirection.
- It gets your shell's whole environment, as a command you would have run in the background would, so `PATH`, tool versions, and tokens are the same. The environment is passed to the service's worker in memory and is never written to disk. `PYTHONUNBUFFERED=1` is added unless set, so Python output reaches the log as it is written.
- `start` returns once the service is ready and prints its id: bare with `--quiet`, the whole service with `--json`, and a one-line summary otherwise.
  - `--port <n>`: ready once `127.0.0.1:<n>` or `[::1]:<n>` accepts a TCP connection. A port that already accepts connections before the service starts fails at once, since another process holds it.
  - `--ready-line <regex>`: ready once a line of its combined stdout and stderr matches. Color codes are stripped first, and a line still being written counts.
  - Neither: ready once it has run for a second, so a command that dies at once fails `start`.
- `--ready-timeout` (default `2m`) bounds the wait. If the service exits, fails to spawn, or is not ready in time, it is stopped with its process group, its last 20 output lines go to stderr, and `start` exits non-zero with `service_not_ready`. No `SERVICE_EXITED` follows.
- `--max` (default `12h`) is the longest it may run. It is then stopped, and no event is sent. Pick the shortest lifetime the work needs.
- A service has no output-inactivity check and sends no check-due reminders: it may stay silent for hours.

## Ownership and cleanup

A service belongs to the thread that started it, found as for `task run`: a worker's parent task thread, otherwise `CODEX_THREAD_ID` or `CLAUDE_CODE_SESSION_ID`. It ends when one of these happens first:

- `homebased service stop <id>`. It stops the whole process group, SIGTERM first and SIGKILL after 10 seconds, and returns once the service has ended. Stopping an ended service succeeds and changes nothing.
- The owner's T3 thread is archived, settled, finished with its delegated task, or deleted. The daemon checks about once a minute, and a thread it cannot find must stay missing for several checks in a row. An owner outside T3, such as a terminal session, is not followed: stop its services yourself.
- `--max` elapses.
- It exits on its own.

Only the last sends an event. Stop your services when the work that needed them is done rather than relying on cleanup.

A service started inside a homebased worker belongs to the parent task's thread, so it outlives the worker; stop it before the worker reports. `homebased daemon stop` and `restart` leave services running, and a restarted daemon picks them up again.

## Inspect

```bash
homebased --json service list                      # this thread's live services and their state
homebased --json service list --ended --all-threads
homebased service log <id> --tail 100
homebased --json service stop <id>
```

`state` is `starting`, `ready`, `stopping`, or `ended`. An ended service has `end`, `exit`, and `detail`. `end` is `stopped`, `owner_done`, `max_elapsed`, `not_ready`, `exited`, or `lost` (its worker vanished without recording how it ended). `service log` works while the service runs and after it ended.

## SERVICE_EXITED

When a service that was ready exits on its own, or its worker is lost, its owner gets one `HOMEBASED_EVENT` with `event: "SERVICE_EXITED"`. It goes through normal callback delivery: the T3 turn, the Claude Code messaging socket, or `codex queue`. No `homebased wait` takes a service id, so a running wait never prints it. Nothing is sent for a service stopped by `service stop`, by owner cleanup, or by `--max`, or one that never became ready. An owner whose T3 thread is archived or deleted is not woken.

| Key | Meaning |
| --- | --- |
| `service` | Service UUID. Use it for `service log` and de-duplication; each service sends at most one event. |
| `name` | The `--name` given. |
| `thread` | The owning thread. |
| `machine` | UUID of the machine that ran it. |
| `cwd`, `command` | Where and what it ran. |
| `end` | `exited` or `lost`. |
| `exit` | The process result as in a task's `process`: `{"kind":"exit","code":n}`, `{"kind":"signal","signal":n}`, or `null` when unknown. |
| `detail` | A sentence on how it ended, or `null`. |
| `started_at`, `ready_at`, `ended_at` | RFC 3339 times. |
| `output_tail` | Its last output lines, at most 20. |
| `next_action` | Suggested step. |

Read `output_tail`, or `homebased service log <service> --tail 200`. Restart the service with `service start` only if the work still needs it, and tell the user why it died if that matters to the task.
