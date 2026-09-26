# homebased

A long build, test suite, or agent review can leave your Codex conversation waiting. Starting the work is only part of the job. You still need to check when it finishes and bring the result back.

**Homebased runs the work in the background and sends the result back to the same Codex thread.** Your agent submits a task and ends its turn. You can continue the conversation or leave while the work runs. When the task finishes, your agent can report the result in that thread.

Use it to:

- Send a coding task or review to Codex, Claude, Grok, or OpenCode while you work on something else.
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

`daemon status` must report `"socket": "up"`. Linux writes `~/.config/systemd/user/homebased.service`. macOS writes `~/Library/LaunchAgents/dev.praveen.homebased.plist`. Run install from a shell where `codex`, `claude`, `grok`, `opencode`, and the project toolchains are on `PATH`. The unit stores that `PATH` and the absolute agent paths, including `HOMEBASED_OPENCODE` when OpenCode is installed.

Homebased reads `~/.config/homebased/config.toml` when the daemon starts. A missing file at this default path means that Fleet is disabled. To use another file, set `HOMEBASED_CONFIG` or pass `--config <path>`. `daemon install` validates an explicit config path and stores its absolute path in the host unit. This keeps the same config after logout or restart. Restart the daemon after you edit its config.

The HTTP listener is off by default. Fleet peers use this listener. The dashboard is also served here when its assets are built. There is no application token: any peer that can reach the listener can use Fleet routes and can read every regular file available to the daemon user through the dashboard file browser. Text, raster images, and HTML open on a separate content origin; other files download.

### Push notifications (ntfy)

Add this section to `~/.config/homebased/config.toml` on every machine that
owns agent threads:

```toml
[notify.ntfy]
topic = "praveen_homebased_9630420"
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

To let other Fleet machines reach this daemon, set `HOMEBASED_WEB_LISTEN` to a reachable host:port before install. The host unit stores this address. Use a Tailscale or LAN address on a trusted network, or `0.0.0.0:7677` to listen on all interfaces. You can also set `--web-listen` when you run the daemon directly. A loopback address cannot receive requests from other machines.

Use `homebased daemon stop` and `homebased daemon restart`. Do not use raw `systemctl` or `launchctl`.

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

Submit a JSON spec to the local daemon. It can run the task on this machine or
send it to a Fleet machine. The submitting machine owns the Codex thread and
returns events to it. Tell the user the task id, and end the turn. Results
arrive later as one line: `HOMEBASED_EVENT {...}`. Do not poll a running task.

```sh
homebased --json daemon status                         # socket must be "up"
homebased --json task submit --spec "$spec" --dry-run  # validate, print child argv
homebased --json task submit --spec "$spec"            # submit to the local machine
```

Always pass `--json` on data commands. Every JSON object carries `api_version: 1`. Put task fields in the JSON spec. `--request-id` is an optional CLI flag for a stable retry identity; it is not a task field.

### Workloads

| Work | `workload.type` |
| --- | --- |
| Long commands: `cargo build`, test suites, CI watchers | `task` |
| A model must reason and produce a report | `agent` |
| Work in a pinned Docker image, such as checkpoint evaluation | `container` |

A `task` runs an argv array with no shell. A caller that needs shell syntax must request it, for example `["sh", "-lc", "..."]`. An `agent` runs Codex, Claude, Grok, or OpenCode with a prompt file. Prefer `task` unless a model must reason.

A `container` runs an image pinned by digest, with typed fields for the entrypoint, arguments, GPUs, memory limit, user, working directory, mounts, and environment. Homebased builds the Docker calls itself: it creates the container, saves its ID, starts it, streams its logs to `output.log`, waits for it, and removes it. The task exit code is the container exit code. If the watching worker stops, the container keeps running and the daemon adopts it. Docker options that the fields do not model, such as privileged mode, are refused. See [submit.md](.agents/skills/homebased/references/submit.md) for the fields.

Claude agent workloads use streaming JSON output by default, so `output.log` records progress during a turn. A caller can select a different Claude output format with `extra_args`; exact spellings of Homebased-managed standalone switches are reserved tokens there, so Homebased treats every exact match as that switch, not as another option's value, and emits each at most once. Other extra arguments keep their order and spelling.

OpenCode agent workloads use `opencode run --standalone` with JSON events and the prompt feed on stdin. `model` is optional and is passed as one provider-qualified value, such as `zai-coding-plan/glm-5.3-flash` or `provider/model#variant`. OpenCode gets full work-tool permissions for that child only. Homebased sets the child `PWD` to `cwd`, adds a generated primary agent, and leaves persistent OpenCode configuration unchanged. An inherited `OPENCODE_CONFIG_CONTENT` value may contain JSONC; Homebased preserves its unrelated settings and rejects malformed content or a generated-agent name collision. OpenCode extra arguments cannot replace the agent, working directory, model, prompt, session, server, or standalone mode.

### Spec

Submit specs use `api_version: 1` and a `workload` object. The required top-level `name` is a short goal label for the dashboard and events. Name the work, not the agent or the CLI. Tasks stored before this field was required keep a server-derived `display_name` from the workload. `thread` is the Codex thread UUID that should receive events. `cwd` is the directory the child runs in. Omit `machine` to run on the local machine. Set `machine` to a Fleet machine name to run there. The remote machine checks `cwd` on its own file system. For remote agent work, `prompt_file` must be an absolute path on the submitting machine; the CLI reads the file and sends its text.

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

To run the task on a Fleet machine, add `machine` at the top level of the
spec. This example is a remote task spec:

```json
{
  "api_version": 1,
  "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
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
`--request-id`, the CLI creates a new one for each submit command.

`homebased task schema` prints the JSON Schema. The full field contract is in [`.agents/skills/homebased/references/submit.md`](.agents/skills/homebased/references/submit.md).

### Inspect and cancel

```sh
homebased --json task list --status running,queued
homebased --json task show <id>
homebased task log <id> --tail 200
homebased --json task cancel <id>
```

Task ids are full UUIDs. Prefix matching does not exist. `task list` shows tasks
stored on the local machine. `task show`, `task log`, and `task cancel` use the
local daemon and can find or operate on a task in the known Fleet. A show result
includes `origin_machine`, `execution_machine`, `found_on`, and `availability`.
When the executor is offline, show can return cached origin state. Log data
comes from the executor and is unavailable while that machine is offline. When
the dashboard is enabled, `daemon status` reports its URL.

Errors with `--json` use the versioned error envelope. For example,
`submission_outcome_unknown` means the executor may have accepted the task. Use
the same `--request-id` to resolve or retry. `cluster_lookup_incomplete` means
one or more known peers could not be checked. Retry the lookup; the daemon only
returns `task_not_found` after every machine in the current known Fleet gives a
definitive negative result.

### GPU resource loans

Use a registered resource for shared GPU work. The resource authority owns the
queue, active loan, and release proof. Read the
[GPU resource loan runbook](docs/resource-loans.md) for the real CLI commands,
JSON inputs, retry rules, and supervisor steps. Do not use the current live
training job to test this workflow.

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

| `event` | Meaning |
| --- | --- |
| `TASK_REPORTED` | Worker sent an interim report. The process is still running. |
| `TASK_CHECK_DUE` | No output for the full inactivity timeout. The child is still live. |
| `TASK_SUCCEEDED` | Exit 0, or last report was `succeeded`. |
| `TASK_BLOCKED` | Last report was `blocked`. The process has exited. Answer and submit a new spec. There is no resume. |
| `TASK_FAILED` | Failed report, non-zero exit, signal, or spawn failure. |
| `TASK_CANCELLED` | `task cancel` or `daemon stop --yes` ended it. |
| `TASK_LOST` | Worker disappeared without `exit.json`. |

### Direct messages

Send a message to an exact Codex thread on a known machine:

```sh
homebased --json message send \
  --machine code \
  --thread <thread-uuid> \
  --source-thread <source-thread-uuid> \
  --message "Please review this change."
```

The CLI can also select the destination by receiver-side `--cwd`, or send to
the origin thread for a task with `--task <task-uuid>` alone. Pass the same
`--message-id <uuid>` to retry after a lost response. Delivery is synchronous
and at-least-once. A receiver crash before it stores the receipt can lead to a
duplicate message. Read [messages.md](.agents/skills/homebased/references/messages.md)
for source, reply, and retry rules.

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

The build installs `homebased` to `~/.local/bin` and restarts a running daemon.
