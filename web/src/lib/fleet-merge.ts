// Merge of per-machine task reads into the dashboard's one fleet list

import type { FleetTask, TaskSummary } from './api.ts';

/** One machine's answer to a task read. */
export interface MachineTasks {
	/** Machine that answered. */
	machine: string;
	/** Newest first, each with the machine that runs it. */
	tasks: readonly FleetTask[];
	/** Whether the machine's `limit` left out older tasks. */
	hasMore: boolean;
}

/** The merged list and whether older tasks were left out. */
export interface MergedTasks {
	tasks: FleetTask[];
	hasMore: boolean;
}

/**
 * Merge per-machine answers into one entry per task, newest first, keeping the newest `limit`.
 *
 * A remote task has an origin record on the machine that sent it and an execution record on the
 * machine that runs it. The execution record wins because it owns the process state, and the
 * origin record stands in until the executor answers.
 *
 * A machine that left older tasks out may hold tasks older than its oldest listed one, so the list
 * ends there. With every machine read at `limit` that cut is never shorter than the limit, and it
 * keeps a machine still answering a smaller limit from leaving a gap mid-list.
 */
export function mergeFleetTasks(answers: readonly MachineTasks[], limit?: number): MergedTasks {
	const byId = new Map<string, { execution: boolean; entry: FleetTask }>();
	for (const answer of answers) {
		for (const entry of answer.tasks) {
			const execution = entry.machine === answer.machine;
			const kept = byId.get(entry.task.id);
			if (kept === undefined || (execution && !kept.execution)) {
				byId.set(entry.task.id, { execution, entry });
			}
		}
	}
	const sorted = [...byId.values()]
		.map(({ entry }) => entry)
		.sort((left, right) => newestFirst(left.task, right.task));
	const cuts = answers.flatMap((answer) =>
		answer.hasMore && answer.tasks.length > 0 ? [answer.tasks[answer.tasks.length - 1].task] : []
	);
	const complete = sorted.filter((entry) => cuts.every((cut) => newestFirst(entry.task, cut) <= 0));
	// each machine's own `has_more` covers tasks it left out before the merge saw them
	const hasMore =
		answers.some((answer) => answer.hasMore) || (limit !== undefined && complete.length > limit);
	return { tasks: limit === undefined ? complete : complete.slice(0, limit), hasMore };
}

// matches the daemon's order: creation time, then task UUID, both descending
function newestFirst(left: TaskSummary, right: TaskSummary): number {
	const byTime = Date.parse(right.created_at) - Date.parse(left.created_at);
	if (byTime !== 0) return byTime;
	return right.id < left.id ? -1 : right.id > left.id ? 1 : 0;
}
