# Services

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
