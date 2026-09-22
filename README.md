# homebased

A long build, test suite, or agent review can leave your Codex conversation waiting. Starting the work is only part of the job. You still need to check when it finishes and bring the result back.

**Homebased runs the work in the background and sends the result back to the same Codex thread.** Your agent submits a task and ends its turn. You can continue the conversation or leave while the work runs. When the task finishes, your agent can report the result in that thread.

Use it to:

- Send a coding task or review to Codex, Claude, or Grok while you work on something else.
- Run a build, test suite, or CI watcher and get the result without repeated status checks.

Homebased keeps task status and logs. An optional web dashboard lets you inspect tasks and browse files. If a task stops producing output, Homebased asks the agent to check it and leaves the task running.

[Ask your agent to install it](#for-agents), or follow the [manual install steps](#install). Homebased runs on Linux and macOS.

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

The install script puts the binary on disk. It does not start the daemon or install the skill.

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

The script overwrites an existing `homebased` in the install directory.

It supports Linux (`x86_64` and `aarch64`) and macOS (Intel and Apple silicon).

### Daemon

The install script does not start the daemon. After `homebased` is on `PATH`:

```sh
homebased daemon install
homebased --json daemon status
```

`daemon status` must report `"socket": "up"`. Linux writes `~/.config/systemd/user/homebased.service`. macOS writes `~/Library/LaunchAgents/dev.praveen.homebased.plist`. Run install from a shell where `codex`, `claude`, `grok`, and the project toolchains are on `PATH`. The unit stores that `PATH` and the absolute agent paths.

The dashboard is off by default. It includes a device-wide read-only file browser. There is no application token: any peer that can reach the dashboard can read every regular file available to the daemon user. Text, raster images, and HTML open on a separate content origin; other files download.

To enable the dashboard, set `HOMEBASED_WEB_LISTEN` to a host:port before install. The host unit stores this address. Use a Tailscale or LAN address on a trusted network, or `0.0.0.0:7677` to listen on all interfaces. You can also set `--web-listen` when you run the daemon directly.

Use `homebased daemon stop` and `homebased daemon restart`. Do not use raw `systemctl` or `launchctl`.

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

Submit a JSON spec, tell the user the task id, and end the turn. Results arrive later as one line: `HOMEBASED_EVENT {...}`. Do not poll a running task.

```sh
homebased --json daemon status                         # socket must be "up"
homebased --json task submit --spec "$spec" --dry-run  # validate, print child argv
homebased --json task submit --spec "$spec"            # {"id": "<uuid>", "status": "queued"}
```

Always pass `--json` on data commands. Every JSON object carries `api_version: 1`. Submit only with `homebased task submit --spec <file|->`. There are no per-field submit flags.

### Workloads

| Work | `workload.type` |
| --- | --- |
| Long commands: `cargo build`, test suites, CI watchers | `task` |
| A model must reason and produce a report | `agent` |

A `task` runs an argv array with no shell. A caller that needs shell syntax must request it, for example `["sh", "-lc", "..."]`. An `agent` runs Codex, Claude, or Grok with a prompt file. Prefer `task` unless a model must reason.

Claude agent workloads use streaming JSON output by default, so `output.log` records progress during a turn. A caller can select a different Claude output format with `extra_args`; exact spellings of Homebased-managed standalone switches are reserved tokens there, so Homebased treats every exact match as that switch, not as another option's value, and emits each at most once. Other extra arguments keep their order and spelling.

### Spec

Submit specs use `api_version: 1` and a `workload` object. The required top-level `name` is a short goal label for the dashboard and events. Name the work, not the agent or the CLI. Tasks stored before this field was required keep a server-derived `display_name` from the workload. `thread` is the Codex thread UUID that should receive events. `cwd` is the directory the child runs in.

`timeout` is an output-inactivity timer (default `1h`, minimum `30m`). Homebased resets it when `output.log` receives bytes. If the live child produces no output for the full timeout, Homebased sends `TASK_CHECK_DUE` and leaves the child running. Only explicit cancel, a signal, or process exit stops the child.

Agent example:

```json
{
  "api_version": 1,
  "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
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
  "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
  "name": "cargo release build",
  "cwd": "/path/to/project",
  "timeout": "2h",
  "workload": {
    "type": "task",
    "command": ["cargo", "build", "--release"]
  }
}
```

`homebased task schema` prints the JSON Schema. The full field contract is in [`.agents/skills/homebased/references/submit.md`](.agents/skills/homebased/references/submit.md).

### Inspect and cancel

```sh
homebased --json task list --status running,queued
homebased --json task show <id>
homebased task log <id> --tail 200
homebased --json task cancel <id>
```

Task ids are full UUIDs. Prefix matching does not exist. When the dashboard is enabled, `daemon status` reports its URL and that page shows the same data.

### Events

Delivery is at-least-once. Treat a repeated event for the same task and event name as a duplicate.

| `event` | Meaning |
| --- | --- |
| `TASK_REPORTED` | Worker sent an interim report. The process is still running. |
| `TASK_CHECK_DUE` | No output for the full inactivity timeout. The child is still live. |
| `TASK_SUCCEEDED` | Exit 0, or last report was `succeeded`. |
| `TASK_BLOCKED` | Last report was `blocked`. The process has exited. Answer and submit a new spec. There is no resume. |
| `TASK_FAILED` | Failed report, non-zero exit, signal, or spawn failure. |
| `TASK_CANCELLED` | `task cancel` or `daemon stop --yes` ended it. |
| `TASK_LOST` | Worker disappeared without `exit.json`. |

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

### From this repository

```sh
just release local
```

That build installs `homebased` to `~/.local/bin`.
