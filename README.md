# homebased

Supervise long-running agent CLIs and general task commands, then report them back to a Codex thread.

Homebasd runs two workload variants under the same detached lifecycle:

- `agent` — Codex, Claude, or Grok with a prompt and optional reporting trailer
- `task` — an arbitrary argv array such as `cargo build --release` or `gh pr checks --watch` (no shell)

`timeout` is an attention timer (default 4h, minimum 2h). When it expires, Homebasd sends `TASK_CHECK_DUE` and leaves the child running. Only explicit cancel, a signal, or process exit stops the child.

Submit specs use `api_version: 1` and a `workload` object. See `.agents/skills/homebased/references/submit.md` for the full contract.

## Install

Linux and macOS:

```sh
curl -LSfs https://github.com/avencera/homebasd/releases/latest/download/install.sh | sh
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
curl -LSfs https://github.com/avencera/homebasd/releases/latest/download/install.sh | sh -s -- --tag v0.1.0
```

A different install directory:

```sh
curl -LSfs https://github.com/avencera/homebasd/releases/latest/download/install.sh | sh -s -- --to /usr/local/bin
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--to` | `~/.local/bin` | Directory for the `homebased` binary |
| `--tag` | latest GitHub release | Release tag, for example `v0.1.0` |
| `--git` | `avencera/homebasd` | GitHub repository that hosts the release |

The script overwrites an existing `homebased` in the install directory.

It supports Linux (`x86_64` and `aarch64`) and macOS (Intel and Apple silicon).

### From this repository

```sh
just release local
```

That build installs `homebased` to `~/.local/bin`.
