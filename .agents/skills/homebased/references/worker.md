# You are a homebased worker

`HOMEBASED_TASK_ID` is set, so this session was started by `homebased` on behalf of an orchestrator in another Codex thread. `HOMEBASED_HOME` points at the state directory. Nobody is watching this session live, and nobody can answer a question mid-task.

For a GPU queue run, `HOMEBASED_TASK_ID` identifies one attempt. Use [resource-queue.md](resource-queue.md) for run variables, checkpoints, and exit 75. Run tasks cannot be used in `after` or waited on.

## Do the work

- Follow the prompt. Verify the work the way the prompt asks, or with the repository's normal checks if it says nothing.
- Do not run `codex queue`. Do not submit new homebased tasks unless the prompt asks for it, or to run a long command you then wait on (see [Long commands](#long-commands-submit-and-wait)).
- When you submit tasks, set the spec `thread` to your parent task's thread: `homebased --json task show "$HOMEBASED_TASK_ID"` prints it. Do not use `HOMEBASED_TASK_ID` itself. Another thread fails with `thread_mismatch` unless you pass `--allow-other-thread`. On a remote executor the parent's thread is not on this machine, so the submit fails with `unknown_thread`.
- A Claude worker can receive a `HOMEBASED_MESSAGE` line between turns from `message send --worker`. Treat it as an update to the prompt from the orchestrator, and say in your report how you applied it.
- Leave the working tree in the state the prompt asked for. Do not commit or push unless instructed.

## Long commands: submit and wait

Never wait in the foreground for a long command (a GPU run, a release build, a benchmark, a CI watch), and never detach one with `&`, `nohup`, `setsid`, or a background tool. Ending your turn ends this process and kills anything it started. A Claude worker runs with `CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1`, so the Bash and Agent tools have no `run_in_background`, and a foreground command that hits its timeout is killed instead of moved to the background. Detaching from the shell is still possible, but it is still wrong.

Hand the command to homebased and park instead. You do not need permission in the prompt for this.

1. Submit the command as a `task` workload (or the agent you need) with your parent task's `thread`, as above. Note the task id it returns. Submit as many as you need.
2. Report `waiting` on those tasks with notes for the next run, then exit 0 at once:

```bash
homebased task report --outcome waiting --on <task-id> [--on <task-id> ...] \
  --summary "<what is running and why>" --notes-file - <<'NOTES'
<what each outcome of each task means, and the exact next steps for each>
NOTES
```

The orchestrator gets `TASK_WAITING`. Homebased holds a continuation task until every task you named has ended, whether it succeeded, failed, or was cancelled, then starts it on its own. A Codex worker continues in the same thread and receives only the block below. Any other worker starts a fresh session whose prompt is your original prompt followed by the block:

```text
--- homebased continuation ---
You are continuing task <your task id>. You reported waiting on <task ids>.
Those tasks have now ended. Read each outcome with:
  homebased --json task show <task id>
  homebased task log <task id> --tail 200
Your notes from before you stopped:
<your notes>
Continue the task from these notes. Do not redo completed work.
```

- The notes are the only memory the next run has besides the repository. Write everything it needs into them: what is already done and verified, what each outcome means, and what to do next. Do not write a `RESUME.md` or any other handoff file into the repository.
- The continuation is a new task with your name plus ` (continued)`, the same `cwd`, agent, model, `extra_args`, and parent thread. It reports like any worker, and may park again.
- Name 1 to 16 tasks, each once. Each must be a task on this machine that was submitted here, not a GPU queue run, and not a task that already waits on you. Errors: `unknown_dependency`, `waiting_target_unsupported`, `waiting_remote_unsupported`, `waiting_cycle`. Only agent workers can park (`waiting_unsupported_workload`), and only on the machine that submitted them.
- One unit of work has at most 20 runs. On the last one, `waiting` fails with `too_many_continuations`; report `blocked` instead.
- The last report wins. If you report `waiting` and then decide to finish the work yourself, report `succeeded` or `failed` before you exit. A waiting report followed by a non-zero exit is `TASK_FAILED`, and nothing continues.

## Report when finished

Run exactly one of these after the work is complete and verified:

```bash
homebased task report --outcome succeeded --summary "<one paragraph: what changed, how it was verified>"
homebased task report --outcome failed --summary "<what failed, why, what you tried>"
homebased task report --outcome blocked --summary "<the exact decision or input you need>"
```

Always report. An agent that exits 0 without any report reads as `TASK_FAILED` with `reason: "no_report"`, because the orchestrator cannot tell whether the work finished.

- `--id` defaults to `HOMEBASED_TASK_ID`; do not pass it.
- Use `--summary-file <path>` or `--summary-file -` for a multi-line summary. The cap is 4 KiB; put detail in your normal output, which the orchestrator reads from `output.log`.
- The report writes SQLite directly. It works even while the daemon is stopped or restarting.
- The orchestrator receives one message when this process exits, carrying every report in order. Exit promptly after reporting.
- To wait for a long command, report `waiting` as described in [Long commands](#long-commands-submit-and-wait).

## Reporting more than once

Reports append; they never replace. The last outcome is the final one. If you report `blocked` and then find the answer yourself, report again with `succeeded` or `failed`.

Add `--notify` only when the orchestrator must act before you exit, for example a `blocked` report while you continue with a fallback. It sends one interim `TASK_REPORTED` message right away and is best effort. The exit message still follows. Default to no `--notify`.

Limits: at most 20 reports per task (`too_many_reports`, exit 5). A report after the task has been marked terminal fails with `task_terminal`, exit 5.

## If the report command fails

Print the error and your summary in your normal output, then exit non-zero when the work failed or is blocked. The orchestrator reads `output.log` when no report exists.
