# Fleet

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
default. Tailscale discovery is optional. Plain Fleet HTTP has no
authentication: use it only on a trusted LAN or tailnet. A peer reached over
the [iroh link](#iroh-link) is authenticated, and works off the LAN without
Tailscale.

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

## iroh link

A machine can also run an [iroh](https://www.iroh.computer) endpoint, the Fleet
link, next to its HTTP listener:

```toml
[fleet.iroh]
relays = "n0"   # or ["https://relay.example.com"], or "disabled"
port = 7679     # UDP port; 0 picks a free one
# peers = ["<endpoint id>"]   # more ids allowed the whole Fleet API
```

The link authenticates both ends by endpoint id. Each daemon keeps its key in
`iroh/secret.key` in its state directory, and `homebased fleet endpoint-id`
prints the id, making the key first if needed. It reads the state directory,
not the daemon, so you can collect every machine's id before you turn the link
on anywhere.

A daemon serves an endpoint id only what it granted that id. A Fleet peer gets
the whole Fleet API, the same as over HTTP; a pool machine gets only its member
routes. A pools coordinator also lets an unknown id call the pool join route,
and nothing else. Any other daemon closes connections from ids it has not
granted. Fleet calls over the link are plain HTTP/1, one exchange per QUIC
stream, handed to the daemon's own listener, so the Fleet API is the same over
both.

### Fleet machines over the link

Each `[[fleet.machines]]` entry can name a peer's endpoint id next to, or
instead of, its HTTP address, and the order to try them in:

```toml
[[fleet.machines]]
address = "http://main:7677"          # plain HTTP
iroh = "<main's endpoint id>"         # the Fleet link
transports = ["http", "iroh"]         # try in this order
# iroh_addrs = ["192.168.1.10:7679"]  # direct UDP addresses, optional
# iroh_relay = "https://relay.example.com"  # the peer's home relay, optional
```

| Key | Meaning |
| --- | --- |
| `address` | The peer's HTTP base address |
| `iroh` | The peer's endpoint id. Needs `[fleet.iroh]` here. This daemon grants the id the whole Fleet API and keeps a connection to it |
| `transports` | `"http"` and `"iroh"` in the order to try them. Each one listed needs its key, and each key needs its transport listed. Omitted, it is the transports the entry has, HTTP first |
| `iroh_addrs`, `iroh_relay` | Where to dial the peer. With `relays = "n0"` the id alone finds it; with custom relays, set one of them; with `"disabled"`, set `iroh_addrs` |

A call to a peer tries its addresses in order and moves on when one does not
answer: the entry's transports in their order, then addresses that mDNS,
Tailscale, or earlier probes found. An entry without `http` is link-only: the
peer is reached over the link and never at an HTTP address, even one that
answers with its machine UUID, because plain HTTP cannot prove who answered.
This takes hold once a link probe has confirmed the peer's machine at that
endpoint id, which the directory keeps across restarts; until then, an HTTP
address cached for the machine can still be tried. To turn an HTTP peer
link-only, first list it with both transports and check that `fleet machines`
shows its `iroh` id.

`[fleet.iroh] peers` allows ids this daemon does not dial, such as a machine
that lists this one but that this one does not list. To reach a peer over the
link and get its events back, each side lists the other.

Each pair of machines keeps one connection. A daemon dials a listed peer only
while it has no connection to it, backing off to one try a minute while the
peer stays unreachable or closes the link at once, as a peer that has not yet
allowed this daemon's id does. When both dial at once, both keep the connection the
smaller endpoint id opened.

`homebased --json fleet machines` shows the link: `local.iroh` has this
daemon's endpoint id, relays, and each allowed id with its grant and, when
connected, who dialed and since when. Each peer shows the endpoint id a link
probe confirmed (`iroh`), and a link address carries the endpoint id it
forwards to. A link address is a loopback port this daemon opens at each start;
it is not saved.

### HTTP off

Once every peer reaches a machine over the link, bind its HTTP listener to
loopback, so nothing unauthenticated is reachable from the network:

```sh
HOMEBASED_WEB_LISTEN=127.0.0.1:7677 homebased daemon install
homebased daemon restart
```

A systemd drop-in that sets `HOMEBASED_WEB_LISTEN` overrides the unit, so
change it there too.

Keep the listener on loopback rather than `off`: link streams from peers are
handed to it. The CLI uses the daemon's Unix socket and does not need it. The
dashboard stays reachable on the machine itself, and through `tailscale serve`
if you set that up (see [install](install.md#dashboard-over-https-tailscale)).
A loopback listener is not announced over mDNS, so peers find it only over the
link.

Cloud machines join the Fleet for a while as
[temporary machines](temporary-machines.md): they reach only their pool's
coordinator, over the link, and only the routes they need. Their boot data
writes their config, with `[fleet.iroh]` and `[temporary]` sections, so never
add a `[temporary]` section by hand.

The full operator workflow is in [the Homebased skill](../.agents/skills/homebased/SKILL.md).
