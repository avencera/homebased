# Install and configure

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

## Agent skill

The operator skill is [`.agents/skills/homebased`](../.agents/skills/homebased). Copy it into the agent skills directory:

```sh
mkdir -p ~/.agents/skills
cp -R .agents/skills/homebased ~/.agents/skills/homebased
```

From GitHub without a local clone:

```sh
npx skills add git@github.com:avencera/homebased.git --skill homebased -g -y
```

Codex and Cursor read `~/.agents/skills/homebased`. For Claude Code, also install into `~/.claude/skills/homebased`, or pass `-a claude-code` to `npx skills add`. After install, read `SKILL.md`. That file is the operator contract.

## Daemon

The install script does not start the daemon on first install. If the daemon is
already running, the script restarts it with the new binary. After `homebased`
is on `PATH`:

```sh
homebased daemon install
homebased --json daemon status
```

`daemon status` must report `"socket": "up"`. Linux writes `~/.config/systemd/user/homebased.service`. macOS writes `~/Library/LaunchAgents/dev.praveen.homebased.plist`. Run install from a shell where `codex`, `claude`, `grok`, `opencode`, and the project toolchains are on `PATH`. The unit stores that `PATH` and the absolute agent paths, including `HOMEBASED_OPENCODE` when OpenCode is installed. The macOS plist also sets `CLAUDE_CONFIG_DIR`, from the installing shell or `~/.claude`, so Claude agents share the login file that SSH sessions use instead of a separate keychain copy.

Homebased reads `~/.config/homebased/config.toml` when the daemon starts. A missing file at this default path means that Fleet is disabled. To use another file, set `HOMEBASED_CONFIG` or pass `--config <path>`. `daemon install` validates an explicit config path and stores its absolute path in the host unit. This keeps the same config after logout or restart. Restart the daemon after you edit its config.

Use `homebased daemon stop` and `homebased daemon restart`. Do not use raw `systemctl` or `launchctl`.

## Listener for Fleet and the dashboard

To let other Fleet machines reach this daemon, set `HOMEBASED_WEB_LISTEN` to a reachable host:port before install. The host unit stores this address, and a later install from a shell without the variable keeps it; set it to `off` to turn the listener off. Use a Tailscale or LAN address on a trusted network, or `0.0.0.0:7677` to listen on all interfaces. You can also set `--web-listen` when you run the daemon directly. A loopback address cannot receive requests from other machines, except over the [iroh link](fleet.md#http-off), which hands its peers' requests to this listener.

The HTTP listener is off by default. Fleet peers use this listener. The dashboard is also served here when its assets are built. There is no application token: any peer that can reach the listener can use Fleet routes and can read every regular file available to the daemon user through the dashboard file browser. Text, raster images, and HTML open on a separate content origin; other files download.

## Dashboard over HTTPS (Tailscale)

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

## Push notifications (ntfy)

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

## Keep the host awake (macOS)

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

## Build from this repository

```sh
just release local
```

The build installs `homebased` to `~/.local/bin` and restarts a running daemon.
