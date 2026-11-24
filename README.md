# homebased

Homebased runs long work in the background and tells your coding agent when it's done.

Your agent hands a build, test suite, review, or GPU job to homebased and ends its turn. When the work finishes, homebased sends the result back to the same Codex, Claude Code, or T3 Code thread. Nobody has to keep checking on it.

It runs on Linux and macOS.

## What you can do with it

- Run a command, a container, or another agent (Codex, Claude, Grok, OpenCode) in the background and get the result in the thread that asked for it
- Chain work so a task starts only after the ones it depends on succeed
- Keep a dev server or emulator running for an agent, and stop it when the agent's thread is done
- Queue GPU jobs by priority on a shared machine
- Spread builds and tests across your machines
- Send work to other machines on your LAN or tailnet
- Check tasks and queues, and browse files, from a web dashboard

## Install

The easiest way is to paste this to your agent:

```
Install homebased from https://github.com/avencera/homebased.

1. Binary:
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

Or do it yourself:

```sh
curl -LSfs https://github.com/avencera/homebased/releases/latest/download/install.sh | sh
homebased daemon install
npx skills add git@github.com:avencera/homebased.git --skill homebased -g -y
```

Run `homebased daemon install` from a shell where your agents (`codex`, `claude`, and so on) and project toolchains are on `PATH`, since the daemon keeps that `PATH`.

The skill is how agents learn to use homebased. Claude Code needs it in `~/.claude/skills/homebased` too, or pass `-a claude-code` to `npx skills add`.

To update later, run `homebased update`.

## Quick start

Run a command in the background:

```sh
homebased task run --name "release build" -- cargo build --release
```

This prints a task id and returns. The agent ends its turn. When the build finishes, its thread gets a line like `HOMEBASED_EVENT {"event":"TASK_SUCCEEDED", ...}` and picks up from there.

Check on tasks:

```sh
homebased task list
homebased task log <id> --tail 200
homebased task cancel <id>
```

Start one task after another succeeds:

```sh
build="$(homebased --quiet task run -- cargo build --release)"
homebased task run --after "$build" -- ./deploy.sh
```

Run it on another machine with `--machine <name>` once [Fleet](docs/fleet.md) is set up.

Keep a dev server running for the agent:

```sh
homebased service start --name api --port 8787 -- pnpm dev
```

## Docs

| Topic | What's there |
| --- | --- |
| [Install and configure](docs/install.md) | Install options, the daemon, config file, dashboard over HTTPS, push notifications, keeping a Mac awake, updates |
| [Tasks](docs/tasks.md) | Task specs, agent and container workloads, chaining, events, inspecting and cancelling |
| [Services](docs/services.md) | Dev servers and other long-lived processes |
| [Host memory](docs/memory.md) | Holding tasks until there's enough RAM |
| [GPU queue](docs/resource-queue.md) | Priorities, preemption, idle alerts |
| [CI queue](docs/ci-queue.md) | Build and test slots across machines ([design](docs/ci-queue-design.md)) |
| [Temporary machines](docs/temporary-machines.md) | Pools of cloud machines that cloudlease creates for CI jobs and deletes when idle or past a deadline |
| [Fleet](docs/fleet.md) | Connecting machines |
| [T3 Code](docs/t3.md) | Waking threads, compaction, waiting and sleeping on work, launching threads on other machines |
| [Thread commands](docs/threads.md) | Reading, compacting, watching, and reporting from threads |
| [Messages](docs/messages.md) | Sending a message to another agent's thread |
| [Retention](docs/retention.md) | Cleaning up old task output |
| [Agent skill](.agents/skills/homebased/SKILL.md) | The full contract agents follow |

## Security

Fleet and the dashboard use plain HTTP with no login. Anyone who can reach the listener can run work and read any file the daemon's user can read. Keep it on a trusted LAN or tailnet. The listener is off until you turn it on; see [Install and configure](docs/install.md#listener-for-fleet-and-the-dashboard).
