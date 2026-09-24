# GPU resource loans

Use this runbook for an exclusive GPU registered with Homebased. The resource
authority is the machine that owns the GPU. It owns resource state, serving order,
loan state, and release proof. Version 1 runs resource work on that authority.
The assigned supervisor is one exact machine and thread. The submitting machine
owns the task callback route.

Do not use the current live training job as an example, test, or verification
target. Use a controlled run that is registered for this resource.

Example UUIDs, paths, and command arguments are placeholders. Replace them
before use. Do not run a code block unchanged.

Use the CLI, not private daemon routes or direct database edits.
`homebased resource schema` prints the registration, task, return-work,
trainer-attempt, and operator-attestation JSON schemas.

## Identities and retries

- Choose a resource UUID before registration. Keep it for every retry.
- Choose a request UUID before the first background or queued submit. Keep the
  same UUID and identical input after an unknown result. The submitting daemon
  saves the Homebased task UUID in its origin route before it sends the request
  to the authority.
- The authority creates loan and action UUIDs. Use the exact action UUID shown
  by `resource pending`.
- The authority also saves the release watcher's request and task UUIDs before
  launch. The supervisor uses the action UUID; it does not choose new watcher
  identities.
- A return launch needs distinct, preallocated request and task UUIDs. Keep both
  with the action UUID and identical return choice on retry.
- Cancellation and renotify each need an operation UUID. Reuse it after an
  unknown result.
- Operator attestation also needs its own operation UUID. Keep the complete
  saved document unchanged on retry, including the observation.
- Revision flags are compare-and-set checks. Read the current
  `resource.state_revision` from `resource show`; do not guess or increment it.

An unknown outcome means the authority may have committed the operation. It is
not a rejection. Retry with the same saved input and IDs. A definitive rejection
means the authority refused that operation. Do not change its content while
reusing the same ID.

## Register the resource and supervisor

Run registration on the GPU authority machine. The required input is:

```json
{
  "id": "11111111-1111-4111-8111-111111111111",
  "display_name": "shared GPU",
  "supervisor": {
    "machine": "22222222-2222-4222-8222-222222222222",
    "thread": "33333333-3333-4333-8333-333333333333"
  }
}
```

Save it as `resource.json`, then run:

```sh
homebased --json resource register --spec resource.json
homebased --json resource show 11111111-1111-4111-8111-111111111111
```

Registration sets the initial supervisor. To replace it, use the revision from
the latest `resource show` output:

```sh
homebased --json resource supervisor set 11111111-1111-4111-8111-111111111111 \
  --machine 44444444-4444-4444-8444-444444444444 \
  --thread 55555555-5555-4555-8555-555555555555 \
  --expected-revision <resource-state-revision>
```

Supervisor assignment names a machine UUID and exact thread UUID. Do not select
a supervisor from recent activity or a working directory. After an unknown
result, retry with the same supervisor and expected revision. If the authority
rejects a stale revision, read `resource show` before making a new assignment.

## Submit background training

`resource background submit` binds and starts the one task intended to hold the
GPU between loans. The resource owner registers it only after the task layer
confirms that it is running. Its `--spec` input is a task submit spec with no
`machine` field.
The required fields are `api_version`, `thread`, `name`, `cwd`, and
`workload`. `timeout` is optional and defaults to `1h`; it is an inactivity
timer, not a run deadline. `workload` must be a finite command:

```json
{
  "api_version": 1,
  "thread": "33333333-3333-4333-8333-333333333333",
  "name": "training background",
  "cwd": "/path/to/trainer",
  "timeout": "1h",
  "workload": {
    "type": "task",
    "command": [
      "python", "-m", "ops.run_segment", "run",
      "--task", "/path/to/task.json",
      "--input-root", "/path/to/inputs",
      "--runtime-root", "/path/to/runtime",
      "--image-digest", "sha256:<prepared-image-digest>"
    ]
  }
}
```

This shows the required direct-segment trainer shape. Replace every path and
value with those for a prepared, controlled run. The CLI rejects a machine
override. The current background implementation supports this maintained
trainer because the authority can verify its ownership lock after exit.

Run the command on the assigned supervisor machine and exact supervisor thread:

```sh
homebased --json resource background submit \
  11111111-1111-4111-8111-111111111111 \
  --request-id 66666666-6666-4666-8666-666666666666 \
  --spec trainer.json
```

This works both when the supervisor and authority are the same machine and
when they are different machines. For a remote supervisor, its machine saves
the callback route before sending the launch to the fixed authority. Do not run
the command on the authority machine when the supervisor is remote. The
authority accepts only the exact supervisor thread, no active loan, no queued
request ahead, and the supported trainer command. An exact retry reuses the
saved launch; changed content with the same request UUID conflicts.

## Bind the running trainer attempt

Wait until `resource show` reports the registered background task as `running`
and the trainer holds its ownership lock. Then save the six fields from that
trainer attempt's request as `attempt.json`:

```json
{
  "campaign_id": "campaign-01",
  "campaign_revision_id": "revision-01",
  "task_id": "trainer-task-01",
  "attempt_id": "attempt-01",
  "attempt_number": 1,
  "ownership_token": "token-01"
}
```

The `task_id` in this JSON is the trainer's string identity. It is not the
Homebased task UUID. Bind it to that exact Homebased task:

```sh
homebased --json resource background bind-attempt \
  11111111-1111-4111-8111-111111111111 \
  --task-id <homebased-trainer-task-uuid> \
  --attempt-spec attempt.json
```

Run this on the assigned supervisor machine and thread. The authority reads
the accepted task, reads the trainer attempt, and checks the held lock itself.
Do not invent the binding or call it before the task is confirmed running. An
exact saved binding can be retried. A different binding for that task conflicts.

## Mark a new resource idle

A queued request serves an unregistered resource only from saved evidence that
the GPU is free: a closed loan, a first background launch that never started a
process, or an operator attestation about an ended task. A new resource that
never had a background run has none of these. `resource show` then reports the
attention code for no idle evidence, and queued requests wait.

For such a resource, an operator can record one initial idle attestation. Use
it only when all of these are true:

- `resource show` shows no `registered_background_task`, no `loan`, and no
  `background_launch`.
- The operator inspected the GPU on the authority machine and found no work on
  it, for example with `nvidia-smi`.

Save the document with the exact resource, authority, and `state_revision` from
`resource show`, and a new operation UUID:

```json
{
  "operation_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
  "resource_id": "11111111-1111-4111-8111-111111111111",
  "authority_machine": "22222222-2222-4222-8222-222222222222",
  "expected_state_revision": 0,
  "observation": "Describe the authority GPU checks and why no work holds the GPU",
  "confirmation": "operator_confirmed_gpu_free"
}
```

Run it on the GPU authority machine:

```sh
homebased --json resource initial-idle --spec initial-idle.json
homebased --json resource show 11111111-1111-4111-8111-111111111111
```

The authority refuses the attestation if the resource has a registered task, a
loan, a first background launch, or an operator attestation, because that
history decides the idle state. It accepts one initial attestation per
resource. The receipt is a human confirmation, not proof that a process
exited. It counts as the idle boundary only until the first loan or background
launch; after that, the normal history applies. After an unknown result, retry
with the exact same file. The same operation UUID with changed content is a
conflict.

## Queue and cancel work

Background and queued command inputs use the same strict `ResourceTaskSubmitSpec`
shape shown below. The command is an argv array; it is not a shell string. The
spec has no `machine` field. Use a callback `thread` and a `cwd` available on
the authority machine. The queued command must run an inspectable native ELF or
Mach-O executable in the task's foreground process group. Scripts, shells,
interpreters, container clients, remote launchers, and detach tools are not
accepted. The authority cannot use process-group exit as release proof for
work that leaves that group. Replace the example command with a prepared native
executable before submitting.

```json
{
  "api_version": 1,
  "thread": "77777777-7777-4777-8777-777777777777",
  "name": "bounded GPU job",
  "cwd": "/path/to/job",
  "timeout": "1h",
  "workload": {
    "type": "task",
    "command": ["/path/to/prepared-command", "--input", "/path/to/input"]
  }
}
```

For work in a pinned Docker image, such as checkpoint evaluation, use a
`container` workload instead of a `docker` command. Its `gpus` field is
required for resource work:

```json
{
  "api_version": 1,
  "thread": "77777777-7777-4777-8777-777777777777",
  "name": "evaluate checkpoint",
  "cwd": "/path/to/job",
  "timeout": "1h",
  "workload": {
    "type": "container",
    "image": "registry.example/eval@sha256:<64-hex-digest>",
    "entrypoint": ["/usr/bin/python3", "-m", "eval"],
    "args": ["--checkpoint", "/data/checkpoint"],
    "gpus": "all",
    "memory": "24g",
    "mounts": [
      { "source": "/path/to/checkpoint", "target": "/data/checkpoint", "read_only": true },
      { "source": "/path/to/output", "target": "/out" }
    ]
  }
}
```

The image must already be on the authority; Homebased does not pull it. Each
mount source must exist on the authority. Docker and containerd sockets, and
directories that contain them, are refused. The container runs under
`dockerd`, outside the task's process group, so the witness is the container
itself. The authority releases the GPU only after Homebased saved the container
ID before the start, read the exited container's exit code, removed the
container, and saw that the same ID no longer exists. If the worker that
watches the container stops, the container keeps running, the task stays
`running`, and the daemon starts a worker that adopts the container. If Docker
is unavailable or the exit code cannot be read, the evidence stays
unconfirmed, the GPU stays reserved, and the supervisor gets an attention
notice. `resource background submit` does not accept a container.

Submit with a new, stable request UUID:

```sh
homebased --json resource request submit \
  11111111-1111-4111-8111-111111111111 \
  --request-id 88888888-8888-4888-8888-888888888888 \
  --spec request.json
```

The authority assigns each request an immutable acceptance identity. Queued
requests serve by queue rank, then acceptance identity. New requests join the
back. A request can wait or activate under a loan. Do not start a second task
with ordinary `task submit` to avoid this queue. Use
`resource requests <resource-uuid>` to read requests in serving order.

Cancel only a request that is still queued, before activation. Get the latest
state revision with `resource show`; use a new operation UUID once, then keep it
for retries:

```sh
homebased --json resource request cancel \
  11111111-1111-4111-8111-111111111111 \
  88888888-8888-4888-8888-888888888888 \
  --expected-revision <resource-state-revision> \
  --operation-id 99999999-9999-4999-8999-999999999999
```

Canceling the last request does not remove an active loan or its return
obligation.

Move only a request that is still queued. Read the latest state revision with
`resource show`, then choose exactly one placement: `--front`, `--back`,
`--before <request-uuid>`, or `--after <request-uuid>`. Use one new operation
UUID and keep the same input and ID after an unknown result:

```sh
homebased --json resource request move \
  11111111-1111-4111-8111-111111111111 \
  88888888-8888-4888-8888-888888888888 \
  --before 77777777-7777-4777-8777-777777777777 \
  --expected-revision <resource-state-revision> \
  --operation-id aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa
```

A successful move advances the resource state revision, even when the request
stays in the same place. The public socket action uses the same revision and
operation fields as other resource actions. Its request body for the example
above is:

```json
{
  "api_version": 1,
  "operation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "expected_revision": 3,
  "action": {
    "type": "move_queued",
    "request_id": "88888888-8888-4888-8888-888888888888",
    "placement": {
      "type": "before",
      "request_id": "77777777-7777-4777-8777-777777777777"
    }
  }
}
```

Send this body to `POST /v1/resources/<resource-uuid>/actions` on the authority.

## Check and act on supervisor actions

Run pending from the exact assigned supervisor machine and thread. Save the
whole JSON result unchanged before an action:

```sh
homebased --json resource pending \
  --machine <supervisor-machine-uuid> \
  --thread <supervisor-thread-uuid> > pending.json
```

Check `unavailable_authorities` first. If it is not empty, the result is
incomplete. An unavailable authority is not an empty action list. If it is
empty, use the exact `action_id`, `loan_id`, `state_revision`, phase, and return
context from this saved result. A stale action needs a fresh pending query.

The notice is separate from the action. A `delivered` notice means only that the
notice arrived. It does not complete the release or return decision. Check
`resource show` and a fresh `resource pending` result for the authority's current
phase. Do not infer completion from a delivered notice or a finished watcher.

### Release a background trainer

For a remote supervisor, start the watcher for the exact `release_required`
action:

```sh
homebased --json resource release-watch <action-uuid> \
  --pending-spec pending.json
```

For a co-located supervisor, the authority's `ResourceActor` starts the bound
watcher. Do not start a second watcher. For either case, the authority builds
release proof and `ResourceActor` reconciles it. There is no public
`resource released` command and no caller-supplied release receipt.

A newly published checkpoint is not proof that the GPU is free. The authority
must verify the exact registered task, attempt, checkpoint or final result,
confirmed process exit, and ownership-lock release. If the trainer completed
first, the return context is `already_completed`; do not resume it as the same
run.

If the trainer failed, was cancelled without this action's saved checkpoint
stop, or exited 0 with no result publication, the return context is
`ended_without_result`. It names the task and its outcome. The authority
releases the GPU for this context only when all of these are true:

- The exact registered task has a trainer-attempt association.
- The accepted spec and direct-segment command still match that association.
- The task is terminal, and its process-group exit is confirmed.
- The authority holds the exact saved `.segment.lock` until the transition
  commits.

If a queue exists, the next request in serving order runs next. The ended run cannot resume.
A checkpoint on disk does not make it resumable. A lost task, an unconfirmed
exit, or a held lock keeps the GPU reserved. A trainer with no trainer-attempt
association also keeps the GPU reserved. Its attention reason is
`TrainerAssociationMissing`, and only an operator can resolve it. A registered
trainer that ends before a loan opens gets a release action when a request
arrives. The same checks then apply.

### Resolve an unproven ended trainer

Use `operator-release` only when a trainer has ended or is lost, automatic
release proof is unavailable, and an operator has inspected the GPU authority
machine and confirmed that no work from that trainer remains. For example, a
trainer with no saved attempt association has no saved lock that the authority
can use as release proof. A trainer that ends before its confirmed start
registers it can never get an association. This command records the
operator's decision. It does not prove process exit, lock release, or a
reusable checkpoint. It cannot release a queued or running task.

Run this command on the GPU authority machine, not on a remote supervisor. Read
the current `resource show` result first. Use its exact resource authority and
`state_revision`, and choose the `state_binding` and `task_id` from this table:

| `resource show` state | `state_binding` | `task_id` |
| --- | --- | --- |
| No `loan`; `registered_background_task` has ended or is lost | `{"type":"no_loan"}` | `registered_background_task` |
| `loan` is `awaiting_release` | `{"type":"awaiting_release","loan_id":"<loan.id>","action_id":"<phase.action_id>"}` | `registered_background_task` |
| No `loan`; `background_launch.status` is `release_unproven` | `{"type":"first_background_launch","request_id":"<background_launch.request_id>"}` | `background_launch.task_id` |
| `loan` is `restoring`; `return_execution_mode` is `direct_segment_trainer` | `{"type":"restoring_return","loan_id":"<loan.id>","action_id":"<phase.action_id>"}` | `phase.resume_task_id` |
| `loan` is `restoring`; `return_execution_mode` is `native_foreground` or `container` | `{"type":"restoring_foreground_return","loan_id":"<loan.id>","action_id":"<phase.action_id>"}` | `phase.resume_task_id` |

Other loan phases cannot be resolved by this command. A native foreground
return task that succeeded with a confirmed process-group exit closes its loan
automatically. A container return task that exited with code 0 and whose
removal Homebased confirmed also closes its loan automatically. If either
failed with confirmed evidence, use `resource resolve`. Use
`restoring_foreground_return` only when that proof is missing, for example
when the task is lost, its process-group exit is not confirmed, or its
container evidence is not confirmed. The two
Restoring bindings are not interchangeable. Read `resource show` and use its
exact `return_execution_mode` value to choose the binding. If that field is
absent or unknown, stop; do not infer the mode from the return decision, task
status, command name, or command text, and do not submit either Restoring
binding. The authority refuses a binding that does not match the saved mode.
The `release_unproven` launch status has the attention code
`background_launch_release_unproven`. The dashboard shows it as
`Operator release required`, not `Available`, even when no request is queued.

Save the complete document before sending it:

```json
{
  "operation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
  "resource_id": "11111111-1111-4111-8111-111111111111",
  "authority_machine": "22222222-2222-4222-8222-222222222222",
  "task_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
  "expected_state_revision": 7,
  "state_binding": {
    "type": "awaiting_release",
    "loan_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    "action_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
  },
  "observation": "Describe the authority GPU checks and why no trainer work remains",
  "confirmation": "operator_confirmed_gpu_free"
}
```

Replace every example value with the exact observed value, and use the
`state_binding` from the table. The observation must describe what the operator
checked; do not copy the example text. Then run:

```sh
homebased --json resource operator-release --spec operator-release.json
homebased --json resource show 11111111-1111-4111-8111-111111111111
```

The authority saves the attestation, the task evidence it found, and the queue
or loan transition in one transaction:

- `awaiting_release`: with queued work, the next request in serving order serves. Otherwise
  the loan moves to `awaiting_return`, and the supervisor decides the return.
- `no_loan` and `first_background_launch`: the registration clears. With
  queued work, the next request in serving order serves. Otherwise the receipt is the idle
  boundary that a later request or first background launch uses.
- `restoring_return` and `restoring_foreground_return`: the Restoring loan
  closes with the attested end, and the registration clears. With queued work,
  the next request in serving order serves. Otherwise the closed loan and its receipt are the
  idle boundary. The task keeps its saved state. A lost task stays lost, and an
  unconfirmed process-group exit stays unconfirmed. The receipt records the
  operator's attestation, not a confirmed exit.

Until the receipt commits, the existing loan or launch keeps the resource
reserved. A task that has ended is not enough; only the saved receipt releases
it, and it stays released after a daemon restart.

After an unknown result, retry on the same authority with the exact same file
and operation UUID, even if the resource revision has changed. An exact retry
returns the saved receipt with `replayed: true`. The same operation UUID with
changed content is a conflict. A definite refusal writes nothing. It needs a
fresh state read and, if the operator still confirms release, a new attestation
with a new operation UUID.

### Choose what happens after the queue drains

When the phase is `return_required`, use the `return_context` from pending. The
return-work JSON is one of these exact tagged shapes:

```json
{"type":"same_run_resume","stopped_task":"<task-uuid-from-context>","recovery_ref":"<recovery-ref-from-context>"}
```

```json
{"type":"evaluation_or_next_epoch","completed_task":"<task-uuid-from-context>","spec":{"api_version":1,"thread":"33333333-3333-4333-8333-333333333333","name":"next training work","cwd":"/path/to/trainer","workload":{"type":"task","command":["/path/to/prepared-native-command"]}}}
```

```json
{"type":"new_background_work","spec":{"api_version":1,"thread":"33333333-3333-4333-8333-333333333333","name":"new training work","cwd":"/path/to/trainer","workload":{"type":"task","command":["/path/to/prepared-native-command"]}}}
```

```json
{"type":"after_ended_run","ended_task":"<task-uuid-from-context>","spec":{"api_version":1,"thread":"33333333-3333-4333-8333-333333333333","name":"new training work","cwd":"/path/to/trainer","workload":{"type":"task","command":["/path/to/prepared-native-command"]}}}
```

`same_run_resume` is for the matching `stopped` context. The authority derives
the command from saved run records. It will not infer a resume from a checkpoint
alone; the exact release proof, checkpoint publication, trainer association,
accepted task record, and immutable inputs must still match. Use
`evaluation_or_next_epoch` only with `already_completed`,
`new_background_work` only with `idle`, and `after_ended_run` only with
`ended_without_result`. `resource schema` prints all fields.
For these three choices, the work must be a native foreground executable, the
maintained direct-segment trainer command, or a `container` workload with
`gpus`. A Python evaluation script or a wrapper command is not accepted as a
command; run it in a pinned image as a `container` instead:

```json
{"type":"evaluation_or_next_epoch","completed_task":"<task-uuid-from-context>","spec":{"api_version":1,"thread":"33333333-3333-4333-8333-333333333333","name":"evaluate epoch","cwd":"/path/to/trainer","workload":{"type":"container","image":"registry.example/eval@sha256:<64-hex-digest>","args":["--checkpoint","/data/checkpoint"],"gpus":"all","memory":"24g","mounts":[{"source":"/path/to/checkpoint","target":"/data/checkpoint","read_only":true}]}}}
```

For a remote or co-located supervisor, submit one choice and stable launch identities:

```sh
homebased --json resource return <action-uuid> \
  --pending-spec pending.json \
  --resume-spec return.json \
  --request-id <new-request-uuid> \
  --task-id <new-task-uuid>
```

Or explicitly close without background work and record why:

```sh
homebased --json resource return <action-uuid> \
  --pending-spec pending.json \
  --no-resume "<decision reason>"
```

The authority saves how the accepted task holds the GPU. A same-run resume or a
maintained direct-segment trainer command closes the `restoring` loan when its
start is confirmed, and it becomes the registered training task. A native
foreground command never becomes the registered training task. Its `restoring`
loan keeps the GPU reserved while it runs, and new requests wait. When it exits
with code 0 and a confirmed process-group exit, the loan closes and the next
queued request in serving order runs. Any other end keeps the GPU reserved for `resolve`. A
container return task has the `container` execution mode and behaves the same
way, but its witness is the removed container: the loan closes only when the
container exits with code 0 and Homebased confirms that it is removed.

The co-located return path uses the authority's local decision owner. A remote
return keeps the callback route on the supervisor machine. Only the first
accepted launch can start a worker. After an unknown result, retry with the
same action, request, task, and work choice. A co-located `release-watch`
command is refused because the authority starts that watcher itself.

### Resolve an ended return task

Use `resolve` only when pending shows `restoring` with the exact loan and return
task, and the authority has confirmed that the task ended. This applies to a
direct-segment trainer that ended before its start was confirmed, and to a
native foreground or container task that ended without success. A native
foreground task needs confirmed process-group exit. A container task needs
confirmed container evidence: its exit code was read and its removal was
confirmed, or the container never started. A direct-segment trainer also needs
proof that its exact ownership lock is free:

```sh
homebased --json resource resolve <loan-uuid> \
  --pending-spec pending.json \
  --task-id <exact-return-task-uuid> \
  --reason "<durable resolution reason>"
```

The authority checks the task and release evidence. This is not a way to force
the GPU free while a task is running or uncertain. A lost task, a task whose
process-group exit is not confirmed, or a task whose identity changed stays
reserved. If a direct-segment return task has no lock proof, only an operator
can release it with the `restoring_return` binding. If a native foreground or
container return task is lost or has no confirmed exit evidence, only an
operator can release it with the `restoring_foreground_return` binding. See
[Resolve an unproven ended trainer](#resolve-an-unproven-ended-trainer).

### Retry a failed notice

Renotify only when the exact pending notice is `failed`:

```sh
homebased --json resource renotify <action-uuid> \
  --pending-spec pending.json \
  --operation-id <new-operation-uuid>
```

Keep that operation UUID after an unknown result. Renotify retries delivery; it
does not repeat or complete the action.

## Check status

Use these read commands when needed:

```sh
homebased --json resource show <resource-uuid>
homebased --json resource requests <resource-uuid>
homebased --json resource pending --machine <supervisor-machine-uuid> --thread <supervisor-thread-uuid>
```

Do not poll in a loop. Recheck after a notice, after compaction, before an
independent background launch, or when an operation has an unknown outcome.
Read the Homebased skill's [resource-loan procedure](../.agents/skills/homebased/references/resource-loans.md)
for the supervisor check points.
