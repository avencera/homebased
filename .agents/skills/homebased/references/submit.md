# Submit a task

## 1. Find the Codex thread id

The spec needs the UUID of the Codex thread that should receive the event. `homebased` accepts only a UUID, not a session name.

1. Use the thread id if the user or the harness already gave one.
2. Otherwise take the newest session file whose `cwd` matches this workspace and read `session_id` from its first line:

```bash
for f in $(ls -t ~/.codex/sessions/*/*/*/rollout-*.jsonl | head -20); do
  head -c 600 "$f" | grep -q "\"cwd\":\"$PWD\"" && { head -c 600 "$f" | grep -o '"session_id":"[^"]*"'; echo " $f"; break; }
done
```

Several sessions can share one cwd. Tell the user which id you chose. If you cannot find one, ask for it instead of guessing.

## 2. Write the prompt file

Write the prompt to a file, not inline JSON. `homebased` copies it into the task directory as `prompt.txt`, so a temp directory is fine:

```bash
dir=$(mktemp -d)
cat > "$dir/prompt.md" <<'PROMPT'
...
PROMPT
```

The prompt must stand alone. The worker has no access to this conversation.

- State the goal, the definition of done, the constraints, and the verification the worker must run.
- Give paths relative to `cwd` or absolute. Name the files the worker should read first.
- Say what to do if blocked: report `blocked` with the exact question. The worker cannot ask you mid-task.
- Do not add reporting instructions. `homebased` appends a fixed trailer that tells the worker how to call `homebased task report`. Leave `report_trailer` at its default `true`.

## 3. Write the spec

```json
{
  "api_version": 1,
  "agent": "claude",
  "model": "fable",
  "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
  "cwd": "/home/praveen/code/project",
  "prompt_file": "/tmp/tmp.abc/prompt.md",
  "timeout": "2h"
}
```

| Field | Required | Notes |
| --- | --- | --- |
| `api_version` | yes | Always `1`. |
| `agent` | yes | `codex`, `claude`, or `grok`. |
| `thread` | yes | Codex thread UUID from step 1. |
| `cwd` | yes | Existing directory. The worker runs there. |
| `prompt` or `prompt_file` | exactly one | Relative `prompt_file` resolves against `cwd`. Prefer `prompt_file`. |
| `model` | no | Passed through unchanged: `-m` for codex and grok, `--model` for claude. Omit to use the agent default. |
| `timeout` | no | Humantime string such as `30m`, `2h`. Default `4h`. Set it deliberately; expiry kills the worker and reports `TASK_FAILED` with `process.kind: "timeout"`. |
| `extra_args` | no | Array of strings appended after the unattended flags. |
| `report_trailer` | no | Default `true`. Set `false` only when the worker must not be told to report. |

Unknown fields fail with `invalid_spec` and a JSON pointer. Run `homebased task schema` to print the JSON Schema when in doubt.

The child argv per agent is fixed and unattended: codex runs `exec` with full access and no approvals, claude runs `-p --permission-mode auto --no-session-persistence`, grok runs `--always-approve --verbatim --prompt-file`. The worker inherits `PATH` and `HOME` from the shell that runs `task submit`, so submit from a shell where the agent binary and the project toolchain are on `PATH`.

## 4. Dry run, then submit

```bash
homebased --json task submit --spec "$dir/spec.json" --dry-run
homebased --json task submit --spec "$dir/spec.json"
```

The dry run validates the spec, resolves the binary and `cwd`, and prints the normalized spec plus the exact child argv. It spawns nothing and uses the same exit codes as a real submit. The real submit returns before the agent finishes:

```json
{"api_version": 1, "id": "01a0b06f-306c-749e-aa9e-9e1a619ee915", "status": "queued"}
```

## 5. End the turn

Tell the user the task id, the agent, the timeout, and that the result will arrive as a `HOMEBASED_EVENT` message. Do not wait, sleep, or poll. Several tasks may run at once; there is no concurrency bound, so keep the count sensible for the machine.

## Resubmitting after a blocked or failed task

There is no resume. Write a new prompt that includes the answer or the fix, name the previous task's `evidence` directory so the worker can read its `output.log`, and submit a new spec.
