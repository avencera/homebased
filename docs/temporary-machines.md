# Temporary machines

A pool is a CI class whose machines exist only while its jobs need them. The daemon that creates a pool, its coordinator, asks [cloudlease](#cloudlease-contract) for a cloud machine when a job of the class waits with no free slot. The machine installs Homebased at boot, joins the coordinator over the pool's [link](#link), offers the class like any Fleet machine, and runs jobs that the [CI queue](ci-queue.md) places on it. It renews its own lease only while it is useful. cloudlease deletes it when it releases itself, when it stops renewing, or at its hard deadline, and the coordinator drops it from the Fleet once cloudlease confirms it is gone.

cloudlease, not Homebased, decides whether a machine exists and how long it may live. It holds the provider token and enforces every deadline even when no Homebased daemon is running, so no agent can keep a machine alive by mistake. Homebased never holds a provider token.

## Create a pool and run jobs on it

The coordinator needs Fleet enabled, a listener machines can reach over the pool's [link](#link), and a `[pools]` section:

```toml
[pools]
cloudlease_url = "https://cloudlease.example.workers.dev"
cloudlease_token_file = "/home/me/.config/homebased/cloudlease-token"
# homebased_version = "0.37.0"         # release machines install; this daemon's by default
# reconcile_interval = "15s"

[pools.link]
kind = "tailnet"
join_address = "http://code:7677"      # where machines join, on the tailnet
```

`[pools.link]` is how machines reach the coordinator. `tailnet` is the only kind; see [Link](#link) for the open choice between Tailscale and iroh.

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

The pool's name is the CI class its jobs name. A job of an open pool's class is accepted even before any machine exists. Submit pool jobs on the coordinator: it counts only its own waiting jobs when it asks for machines.

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

The machine's boot data, written by the coordinator and passed through cloudlease, installs the static Homebased release for the machine's architecture, checks its `.sha256`, and starts `homebased daemon serve` on the address the link gives it (its Tailscale address for the tailnet link) with a `[temporary]` config section. The daemon then:

1. reads `/etc/cloudlease/lease.json`, the lease and its lease-scoped token;
2. joins the coordinator with the single-use join token from its boot data, and gets the pool's settings and the `--env-file` variables in the answer;
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

Boot data is readable by anything on the machine through the provider's metadata service, so it carries no secret beyond the single-use join token and the lease token, which can only renew or release its own lease. Upload tokens and other secrets go in the pool's `--env-file` on the coordinator, which reads it at each join and hands the values to the machine over the link. With the tailnet link, the machine's Tailscale key is minted by cloudlease, ephemeral, pre-authorized, single use, and expires in 15 minutes.

Fleet has no authentication, so a temporary machine can call any Fleet route of the coordinator, including ones that run commands there. Run only trusted code on pool machines until the link below limits members.

## Link

How machines reach the coordinator is still open: Tailscale ephemeral keys, or an iroh endpoint embedded in Homebased. Pools, leases, renewal, and release do not depend on it. `MemberLink` in `src/pool/link.rs` holds what boot does for the link (unit ordering, listen address, coordinator address), and Fleet calls go through `ClusterClient`, a hyper HTTP/1 client over a stream, so a second link is a new variant and a connector, not a change to pools.

Whichever link is chosen must authenticate each member and limit it to what it needs. A member calls the coordinator only to join (`/v1/cluster/pool/join`) and to deliver events (`/v1/cluster/job-events`, `/v1/cluster/events`). The coordinator calls the member for CI offers and placement, executions, cancels, and logs. Every other coordinator route, such as `executions`, `messages`, and `t3/launch`, must refuse a member.

| | Tailscale ephemeral keys | iroh endpoint in Homebased |
| --- | --- | --- |
| Identity | Tailscale node with `tag:homebased-temp`; Homebased sees only a source IP | `EndpointId` (an ed25519 public key) the machine generates at boot; the secret key never leaves it |
| Who issues it | cloudlease mints an ephemeral, pre-authorized, single-use, 15-minute key with a Tailscale OAuth client scoped to the tag | Nobody: the machine makes its key; the join token binds that `EndpointId` to the lease at join |
| Reaching only the hub | Tailnet ACL: `tag:homebased-temp` may reach only the coordinator's port 7677, and only the coordinator may reach the tag. Machines cannot reach each other or the rest of the tailnet | The hub accepts a join ALPN from any id and a member ALPN only from ids bound to live leases (`EndpointHooks::after_handshake` rejects after the TLS handshake), and drops an id when its lease is released |
| Authentication inside Homebased | None: any tagged machine is trusted for every route the ACL lets through, so Homebased still needs a per-member credential (from the join answer) and a route allowlist | Per connection, by key; the member ALPN serves only the member routes |
| On the machine | `tailscaled` installed and joined before Homebased boots | Nothing extra; Homebased listens on loopback and serves Fleet over its endpoint |
| Relays and paths | Tailscale DERP and direct paths, part of the Tailscale plan | n0's public relays are rate-limited and documented for development and testing. Production needs n0's paid relays or a self-hosted `iroh-relay` (TLS by ACME, ports 80/443 plus QUIC; it can allowlist endpoint ids). Direct paths by hole punching when they work |
| Discovery | Tailscale names | None needed: boot data carries the hub's `EndpointId` and relay URL (public); the hub dials back with the id from the join |
| Homebased code | None for the transport | iroh 1.5.0 (stable 1.x since June 2026, MSRV 1.91), its QUIC and rustls deps, an HTTP/1 adapter over bi-streams for the axum server and `ClusterClient` (no n0-maintained crate; third-party ones unvetted), key storage, and a connector beside TCP. Binary size cost is unmeasured |
| Outside Homebased | Tailscale OAuth client secret in cloudlease, tag and ACL policy, device churn | A relay to pay for or run |

Both close the hole only with work in Homebased: Tailscale limits which machines reach the coordinator but needs a member credential and route allowlist for what they may call; iroh gives per-connection identity and a natural place to limit routes, at the cost of an HTTP adapter and a relay. The contract's Tailscale key minting applies only if the tailnet link is chosen.

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
- Jobs submitted from another origin to a pool's class are placed on pool machines, but no machine is asked for on their behalf and they are not ended with a released machine.
