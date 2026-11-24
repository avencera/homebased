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
default. Tailscale discovery is optional. Fleet uses plain HTTP. Use it only on
a trusted LAN or tailnet. Fleet does not add authentication over HTTP; the
[iroh link](#iroh-link) does.

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
```

The link authenticates by endpoint id. Each daemon keeps its key in
`iroh/secret.key` in its state directory and serves an id only what that id
was granted: a peer gets the whole Fleet API, and a pool machine gets only
its member routes. A pools coordinator also lets an unknown id call the pool
join route, and nothing else; any other daemon closes connections from ids it
has not granted. Fleet calls over the link are plain HTTP/1, one exchange per
QUIC stream, so the Fleet API is the same over both. HTTP stays on alongside
it.

Today only pools use it. Cloud machines join the Fleet for a while as
[temporary machines](temporary-machines.md): they reach only their pool's
coordinator, over the link, and only the routes they need. Their boot data
writes their config, with `[fleet.iroh]` and `[temporary]` sections, so never
add a `[temporary]` section by hand.

The full operator workflow is in [the Homebased skill](../.agents/skills/homebased/SKILL.md).
