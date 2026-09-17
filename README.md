# homebased

Supervise long-running agent tasks and report them back to a Codex thread.

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
