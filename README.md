# homebased

Supervise long-running agent CLIs and general task commands, then report them back to a Codex thread.

Homebasd runs two workload variants under the same detached lifecycle:

- `agent` — Codex, Claude, or Grok with a prompt and optional reporting trailer
- `task` — an arbitrary argv array such as `cargo build --release` or `gh pr checks --watch` (no shell)

Claude agent workloads use streaming JSON output by default, so `output.log` records progress during a turn. A caller can select a different Claude output format with `extra_args`.

`timeout` is an output-inactivity timer (default 4h, minimum 30m). Homebasd resets it when `output.log` receives bytes. If the live child produces no output for the full timeout, Homebasd sends `TASK_CHECK_DUE` and leaves the child running. Only explicit cancel, a signal, or process exit stops the child.

Submit specs use `api_version: 1` and a `workload` object. A required top-level `name` is the dashboard label. Tasks stored before this field was required keep a server-derived `display_name` from the workload. See `.agents/skills/homebased/references/submit.md` for the full contract.

The dashboard (`127.0.0.1:7677` by default) includes a device-wide read-only file browser. There is no application token: any peer that can reach the dashboard can read every regular file available to the daemon user. Use loopback locally, or bind a Tailscale address with `--web-listen` / `HOMEBASED_WEB_LISTEN` for remote access on a trusted network. Text, raster images, and HTML open on a separate content origin; other files download.

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

### Options

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

The script overwrites an existing `homebased` in the install directory.

It supports Linux (`x86_64` and `aarch64`) and macOS (Intel and Apple silicon).

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

### From this repository

```sh
just release local
```

That build installs `homebased` to `~/.local/bin`.
