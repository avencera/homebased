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

# instead of homebased_version, for a build with no release: one archive pinned by its sha256
# [pools.homebased_archive]
# url = "https://example.com/homebased-x86_64-unknown-linux-musl.tar.gz"
# sha256 = "<64 hex digits>"
```

`homebased_archive` and `homebased_version` are exclusive. The archive is a `.tar.gz` holding a static Linux `homebased` for the machines' architecture; every machine downloads it from `url`, an `https` URL with no quotes or whitespace, and refuses it unless its sha256 matches. The URL is in the boot data, which anything on the machine can read, so a signed URL in it should expire soon after the pool's deadline.

Machines use the coordinator's `relays` too; see [Link](#link). They need no inbound port and no tailnet.

```sh
homebased pool create afv --machine-class do-c16 --max 16 --deadline 6h \
  --min-free-disk 20 --env-file ~/.config/afv/pool.env
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
| `--boot-timeout` | A machine that has not joined this long after its create is released. By default a minute before cloudlease's boot grace ends, or a quarter of the grace for one under 4 minutes; the coordinator reads the grace from each lease's first `heartbeat_deadline_at`, so DigitalOcean's 8 minutes gives 7m and phoenixNAP's 20 gives 19m. A machine renews only once it joined, so cloudlease deletes it at the end of the grace anyway; a value past the grace only changes the reason to `heartbeat_lapsed`. Must be shorter than `--deadline`; `pool create` refuses an equal or longer value with `usage`, because no machine could then join before the pool ends |
| `--min-free-disk` | Free disk, in GiB, below which a machine of the pool admits no CI run (default 1; a Fleet machine's `[ci] min_free_disk_gib` is 10). The boot data writes it as the machine's `[ci] min_free_disk_gib`. Set it to what one job needs, within what the class has free after boot; a machine below it takes no job, idles out, and a fresh one replaces it |
| `--env-file` | `NAME=value` lines on the coordinator that every run on the pool's machines gets |
| `--spend-cap` | Most the pool's cloudlease run may spend, in US dollars, such as `25` or `12.50`. cloudlease's own per-run cap applies without it, and refuses a larger value |

The pool's name is the CI class its jobs name. A job of an open pool's class is accepted even before any machine exists. Submit pool jobs on the coordinator: only it reaches pool machines, and it counts only its own waiting jobs when it asks for machines.

Placement never sends a job to a machine that cloudlease will delete before the job's kill limit, and every machine of the pool ends by its deadline, or sooner when the cloudlease class has a shorter lifetime. So the coordinator keeps each job within reach of some machine:

- At submit, a job is refused with `pool_deadline_too_close` unless a machine the pool has (booting or joined) outlives its `--kill-after` from now, or a machine asked for now would, once it joined: the boot timeout from now plus the kill limit must end by the pool's deadline, or by the end of the class's lifetime, which a lease that ends before the deadline shows. Before a lease of the pool has shown the grace, it uses `--boot-timeout` when set, and otherwise assumes cloudlease's default 8-minute grace; set `--boot-timeout` on a class with a longer grace, such as `19m` on phoenixNAP, so the first jobs are not admitted on the shorter estimate. The message names the longest `--kill-after` the pool takes.
- Each pass, a job that has not started and has fallen out of that reach, for example while its `--after` dependency ran, ends with `JOB_FAILED` whose `held_ending` has code `pool_deadline_too_close`. Time only takes reach away, so such a job could never run.
- The pool asks for machines only for ready jobs that a new machine could run; a job that only a machine it has could run waits for that machine's slot. A free or coming slot counts against a job only on a machine that outlives the job's kill limit, so a slot on a machine about to end does not stand in for a new one. Once a lease has shown the boot grace, a `--boot-timeout` past it counts as the grace, since cloudlease deletes an unjoined machine then.

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
3. ends waiting jobs out of reach (see above), then asks for machines while ready jobs of the class that a new machine could run outnumber the free slots on joined machines plus the slots of machines still booting, up to `--max`. A machine the provider could not create comes back from cloudlease already releasing with reason `create_failed`; the coordinator then waits 4 passes (a minute at the default interval) before it asks again, twice that after a second failure in a row, and stops the pool after a third;
4. for each machine cloudlease reports `released`, removes it from the Fleet and ends every job it still held with `JOB_FAILED`;
5. at the deadline, or after `pool release`, releases the run, fails the pool's jobs that have not started with `JOB_FAILED` whose `held_ending` has code `pool_deadline` or `pool_released`, and marks the pool released once every machine is gone. A pool whose run cloudlease no longer has ends the same way with `run_not_found`. The pool is marked released only once cloudlease took the run's release.
6. when it reads the run as `releasing` or `released` while the pool is open, cloudlease ended the run without the coordinator asking: at its spend cap, or by an operator's release. The pool ends the same way with code `run_ended` and a `cause`: `spend_cap` when a lease of the run was released with `spend_cap`, `released` otherwise (the run has no reason of its own). Its machines are already releasing, and each drops out of the Fleet once cloudlease confirms it gone; a job running on one ends with `machine_released`.

A pool stops when cloudlease refuses a machine for good, or after three `create_failed` machines in a row. A stopped pool asks for no more machines, takes no new job, and fails its waiting jobs with `JOB_FAILED` whose `held_ending` carries the code; jobs already running finish on their machines, which then release themselves when idle, and the pool is released once they are gone. `pool release` releases a stopped pool at once.

| Code | Why cloudlease refused |
| --- | --- |
| `run_ended` | The run takes no machines: its class was repriced or removed, or it hit its spend cap. A run cloudlease ended on its own releases the pool rather than stopping it (step 6 above) |
| `spend_cap_reached` | The machine would take the run past its spend cap, or cloudlease past its hourly cap |
| `create_failed` | The provider could not create three machines in a row |
| others, such as `invalid_boot` or `unauthorized` | A request or configuration error to fix before a new pool |

## On the machine

The machine's boot data, written by the coordinator and passed through cloudlease, installs the static Homebased release for the machine's architecture and checks it against the release's `.sha256`, or downloads the `homebased_archive` and checks it against the sha256 the coordinator pinned, and starts `homebased daemon serve` on loopback with a `[temporary]` config section naming the coordinator's iroh endpoint. The daemon then:

1. reads `/etc/cloudlease/lease.json`, the lease and its lease-scoped token;
2. makes its iroh key, dials the coordinator, and joins with the single-use join token from its boot data, which binds its endpoint id to its lease; it gets the pool's settings and the `--env-file` variables in the answer;
3. writes those variables to a 0600 file in its state directory, which every task and job run on the machine gets, and offers the pool's class with `--slots` slots;
4. every 30 seconds (`[temporary] renew_interval`), and never less often than a quarter of the heartbeat window `lease.json` gives as `heartbeat_ttl_secs`, decides whether to renew its lease:

| On the machine | Decision |
| --- | --- |
| A queued or running run wrote output within `--stall-after` | renew |
| Jobs wait in its queue and no run holds a slot | renew |
| Events it owes the coordinator are not delivered yet | renew |
| No work, idle less than `--idle-limit` | renew |
| Every run went silent for `--stall-after`, and it owes no events | do not renew: cloudlease deletes the machine when its heartbeat window ends (3 minutes by default) unless output resumes |
| No work for `--idle-limit` | stop offering the CI class, so no job can be queued, check again that nothing arrived, then release the lease |
| Renewal answered `410 lease_ended`: cloudlease ended the lease, such as at a spend cap | stop offering the CI class and stop renewing; running work goes on until cloudlease deletes the machine |

A renewal that fails, such as with no answer or `401 unauthorized`, is tried again every 5 seconds within the round, then the next round; cloudlease's heartbeat window, 3 minutes by default, spans several rounds. Its hard deadline applies whatever the machine decides.

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

The coordinator grants an id when its join succeeds, and revokes it and closes its connection when the lease leaves `joined`. The lease stays bound to that one id: a join from another id for the same lease is refused, and so is a join that names a machine id the coordinator already knows, its own or another Fleet or pool machine's. The machine grants only the coordinator's id, from its boot data.

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
| `POST /v1/runs` | Create the pool's run with `max_machines`, `deadline_at`, and `spend_cap_cents` from `--spend-cap`, idempotent by `request_id` |
| `GET /v1/runs/{run}` | Read every lease of the run; `released` means confirmed gone |
| `POST /v1/runs/{run}/machines` | Ask for a machine with `request_id` (the slot), `hostname`, and `boot` (`files`, `run`); a provider failure answers `201` with the lease already releasing, reason `create_failed`. A `503 busy` or no answer is sent again the next pass with the same `request_id` and body |
| `POST /v1/leases/{lease}/release` | Release one machine |
| `POST /v1/runs/{run}/release` | Release the pool |

The machine calls, with its lease token, `POST /v1/leases/{lease}/renew` and `POST /v1/leases/{lease}/release` (`{"reason": "idle"}`). A `410 lease_ended` answer to a renew means the machine is going away.

## Not covered

- Prebuilt images. Each machine downloads Homebased at boot, about a minute.
- Plain tasks sent to a pool machine with `task run --machine <name>` run there, and their runs keep the machine alive, but the coordinator does not end them when the machine is gone. Use pool jobs.
- Draining a pool gently by hand. `pool release` releases every machine at once; let the jobs finish first, then release. Only a pool that cloudlease stopped lets its running jobs finish.
- Pool jobs from other machines. Only the coordinator reaches pool machines, so submit pool jobs on the coordinator.
