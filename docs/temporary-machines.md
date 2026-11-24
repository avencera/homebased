# Temporary machines

A pool is a CI class whose machines exist only while its jobs need them. The daemon that creates a pool, its coordinator, asks [cloudlease](#cloudlease-contract) for a cloud machine when a job of the class waits with no free slot. The machine installs Homebased at boot, joins the coordinator over an [iroh link](#link), offers the class like any Fleet machine, and runs jobs that the [CI queue](ci-queue.md) places on it. It renews its own lease only while it is useful. cloudlease deletes it when it releases itself, when it stops renewing, or at its hard deadline, and the coordinator drops it from the Fleet once cloudlease confirms it is gone.

cloudlease, not Homebased, decides whether a machine exists and how long it may live. It holds the provider token and enforces every deadline even when no Homebased daemon is running, so no agent can keep a machine alive by mistake. Homebased never holds a provider token.

## Create a pool and run jobs on it

The coordinator needs Fleet enabled, the [Fleet link](fleet.md#iroh-link), and a `[pools]` section:

```toml
[fleet.iroh]
# relays = "n0"                        # or ["https://relay.example.com"], or "disabled"
# port = 7679                          # UDP port of the coordinator's iroh endpoint

[pools]
cloudlease_url = "https://cloudlease.example.workers.dev"
cloudlease_token_file = "/home/me/.config/homebased/cloudlease-token"
# homebased_version = "0.37.0"         # release machines install; this daemon's by default
# reconcile_interval = "15s"
```

Machines use the coordinator's `relays` too; see [Link](#link). They need no inbound port and no tailnet.

```sh
homebased pool create afv --machine-class do-c16 --max 16 --deadline 6h \
  --env-file ~/.config/afv/pool.env
homebased ci run --class afv --git --kill-after 2h -- ./clean_scroll.sh PHerc-0172
homebased --json pool show afv
homebased pool release afv
```

| Flag | Meaning |
| --- | --- |
| `--machine-class` | cloudlease class, such as `do-c16`. Size, region, image, and volumes are cloudlease configuration |
| `--max` | Most machines alive at once, 1 to 64. cloudlease enforces it too |
| `--deadline` | No machine of the pool lives past this time from now, at most 24h. cloudlease enforces it |
| `--slots` | CI slots of the class on each machine (default 1) |
| `--idle-limit` | A machine with no work for this long releases itself (default 10m) |
| `--stall-after` | Running work with no output for this long stops keeping its machine (default 30m) |
| `--boot-timeout` | A machine that has not joined this long after its create is released (default 15m) |
| `--env-file` | `NAME=value` lines on the coordinator that every run on the pool's machines gets |

The pool's name is the CI class its jobs name. A job of an open pool's class is accepted even before any machine exists. Submit pool jobs on the coordinator: only it reaches pool machines, and it counts only its own waiting jobs when it asks for machines.

A job whose `--kill-after` would run past the pool's deadline is refused at submit with `pool_deadline_too_close`, and placement never sends a job to a machine that cloudlease will delete before the job's kill limit.

## Lifecycle

The coordinator moves each machine through these states, all from what cloudlease reports:

| State | Meaning |
| --- | --- |
| `requested` | Saved before the create call; the call may not have reached cloudlease |
| `booting` | cloudlease holds a lease; the machine has not joined |
| `joined` | In the Fleet, offering the pool's class; busy or idle by its slots |
| `releasing` | Its lease is ending; cloudlease has not confirmed it gone |
| `released` | cloudlease confirmed every resource of it gone, with a reason |

Each pass, about every 15 seconds, the coordinator:

1. creates the pool's cloudlease run on its first pass;
2. reads the run, moves each machine to its lease's state, and releases a machine that has not joined within the boot timeout;
3. asks for machines while ready jobs of the class outnumber the free slots on joined machines plus the slots of machines still booting, up to `--max`;
4. for each machine cloudlease reports `released`, removes it from the Fleet and ends every job it still held with `JOB_FAILED`;
5. at the deadline, or after `pool release`, releases the run, fails the pool's jobs that have not started with `JOB_FAILED` whose `held_ending` has code `pool_deadline` or `pool_released`, and marks the pool released once every machine is gone. A pool that cloudlease refuses, such as with `spend_cap_reached`, ends the same way with cloudlease's code.

## On the machine

The machine's boot data, written by the coordinator and passed through cloudlease, installs the static Homebased release for the machine's architecture, checks its `.sha256`, and starts `homebased daemon serve` on loopback with a `[temporary]` config section naming the coordinator's iroh endpoint. The daemon then:

1. reads `/etc/cloudlease/lease.json`, the lease and its lease-scoped token;
2. makes its iroh key, dials the coordinator, and joins with the single-use join token from its boot data, which binds its endpoint id to its lease; it gets the pool's settings and the `--env-file` variables in the answer;
3. writes those variables to a 0600 file in its state directory, which every task and job run on the machine gets, and offers the pool's class with `--slots` slots;
4. every minute decides whether to renew its lease:

| On the machine | Decision |
| --- | --- |
| A queued or running run wrote output within `--stall-after` | renew |
| Jobs wait in its queue and no run holds a slot | renew |
| Events it owes the coordinator are not delivered yet | renew |
| No work, idle less than `--idle-limit` | renew |
| Every run went silent for `--stall-after` | do not renew: cloudlease deletes the machine at the heartbeat deadline (about 10 minutes) unless output resumes |
| No work for `--idle-limit` | drain the CI class, check again that nothing arrived, then release the lease |

Its hard deadline applies whatever the machine decides.

## Outputs and lost machines

A job must finish its own uploads before it exits; the machine releases itself only once no run is left and every event reached the coordinator. When cloudlease deletes a machine under running work, at its hard deadline, after its heartbeat lapsed, or after `pool release`, the job's submitter gets `JOB_FAILED` with `machine_released`:

```json
"machine_released": {
  "pool": "afv", "machine": "afv-4fa9bd9a", "reason": "hard_deadline",
  "hard_deadline_at": "2026-11-25T02:00:00Z", "gone_at": "2026-11-25T02:00:41Z",
  "output_lost": true
}
```

Anything the job wrote on the machine and had not uploaded is gone; the task log on the machine is gone too.

## Secrets

Boot data is readable by anything on the machine through the provider's metadata service, so it carries no secret beyond the single-use join token and the lease token, which can only renew or release its own lease. The coordinator's endpoint id and addresses in it are public. Upload tokens and other secrets go in the pool's `--env-file` on the coordinator, which reads it at each join and hands the values to the machine over the link.

## Link

Temporary machines reach the coordinator over the [Fleet link](fleet.md#iroh-link), an [iroh](https://www.iroh.computer) endpoint (`iroh` 1.5.0) on each side, not over the LAN or tailnet HTTP that the rest of the Fleet uses. The coordinator's endpoint listens on UDP port `[fleet.iroh] port`, keyed by `iroh/secret.key` in its state directory so its id survives restarts. Each machine makes its own key at first start and keeps it, and opens one QUIC connection to the coordinator. Machines need no inbound port, no tailnet, and no `tailscaled`.

Each Fleet call between the two, in either direction, is one HTTP/1 exchange on its own QUIC stream of that connection, so the Fleet API is unchanged. Each side's Fleet knows the other at a loopback address: on the machine, `127.0.0.1:7678` (`link_listen`) reaches the coordinator; on the coordinator, one loopback port per machine reaches it. A forwarder turns each TCP connection there into a stream on the link, and the other side hands each stream to its own API.

Each side serves an endpoint id only what it granted that id:

| Who | Grant | What the link lets it do |
| --- | --- | --- |
| Endpoint not bound to a lease | none (enrollment) | Only `POST /v1/cluster/pool/join`. The single-use join token binds its endpoint id to the machine's lease; a failed join closes the connection |
| Machine of a joined lease | pool member | Join again, the Fleet probe (`/v1/cluster/machine`), and event delivery (`/v1/cluster/job-events`, `/v1/cluster/events`) for work it runs itself. No route that starts work, sends messages, or reads other machines' tasks |
| Coordinator, on the machine | peer | Every route of the machine's own API, over streams it opens on the machine's connection |

The coordinator grants an id when its join succeeds, and revokes it and closes its connection when the lease leaves `joined`. The lease stays bound to that one id: a join from another id for the same lease is refused. The machine grants only the coordinator's id, from its boot data.

`relays` picks the relays both ends use to find each other. A relay carries traffic until a direct path works, and whenever none does.

| `relays` | Use |
| --- | --- |
| `"n0"` (default) | n0's public relays and n0's DNS lookup, so a machine finds the coordinator by id alone. Rate-limited, and meant for light use |
| `["https://relay.example.com"]` | Self-hosted [`iroh-relay`](https://github.com/n0-computer/iroh/tree/main/iroh-relay) servers, and no lookup service. Switching is a config change |
| `"disabled"` | Direct addresses only, for one network or tests |

A connection with no packet for 12 seconds is dead; the machine reconnects every 2 seconds until it is back, and keeps renewing while it owes the coordinator events. A coordinator that shuts down closes the connections, so machines reconnect as soon as it is back.

## cloudlease contract

The full contract is the "Temporary machines contract" section of the cloudlease design note. Homebased calls, with `Authorization: Bearer <client token>`:

| Call | Use |
| --- | --- |
| `POST /v1/runs` | Create the pool's run with `max_machines` and `deadline_at`, idempotent by `request_id` |
| `GET /v1/runs/{run}` | Read every lease of the run; `released` means confirmed gone |
| `POST /v1/runs/{run}/machines` | Ask for a machine with `request_id` (the slot), `hostname`, and `boot` (`files`, `run`) |
| `POST /v1/leases/{lease}/release` | Release one machine |
| `POST /v1/runs/{run}/release` | Release the pool |

The machine calls, with its lease token, `POST /v1/leases/{lease}/renew` and `POST /v1/leases/{lease}/release` (`{"reason": "idle"}`). A `410 lease_ended` answer to a renew means the machine is going away.

## Not covered

- Prebuilt images. Each machine downloads Homebased at boot, about a minute.
- Plain tasks sent to a pool machine with `task run --machine <name>` run there, and their runs keep the machine alive, but the coordinator does not end them when the machine is gone. Use pool jobs.
- Draining a pool gently. `pool release` releases every machine at once; let the jobs finish first, then release.
- Pool jobs from other machines. Only the coordinator reaches pool machines, so submit pool jobs on the coordinator.
