import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { FleetTask, TaskSummary } from './api.ts';
import { mergeFleetTasks } from './fleet-merge.ts';

const MAIN = 'main-uuid';
const CODE = 'code-uuid';

// only the fields the merge reads; the rest of a summary does not affect it
function entry(machine: string, id: string, createdAt: string, status = 'running'): FleetTask {
	const task = { id, created_at: createdAt, status } as unknown as TaskSummary;
	return { machine, task };
}

test('the execution record wins over the origin record, whichever lands first', () => {
	const origin = { machine: MAIN, tasks: [entry(CODE, 't1', '2026-11-07T10:00:00Z', 'queued')] };
	const execution = { machine: CODE, tasks: [entry(CODE, 't1', '2026-11-07T10:00:00Z')] };

	for (const answers of [
		[origin, execution],
		[execution, origin]
	]) {
		const merged = mergeFleetTasks(answers.map((answer) => ({ ...answer, hasMore: false })));
		assert.equal(merged.tasks.length, 1);
		assert.equal(merged.tasks[0].task.status, 'running');
	}

	// until the executor answers, the origin record stands in
	const alone = mergeFleetTasks([{ ...origin, hasMore: false }]);
	assert.equal(alone.tasks[0].task.status, 'queued');
});

test('machines interleave newest first and the limit keeps the newest', () => {
	const merged = mergeFleetTasks(
		[
			{
				machine: MAIN,
				tasks: [
					entry(MAIN, 'm2', '2026-11-07T10:03:00Z'),
					entry(MAIN, 'm1', '2026-11-07T10:01:00Z')
				],
				hasMore: false
			},
			{ machine: CODE, tasks: [entry(CODE, 'c1', '2026-11-07T10:02:00Z')], hasMore: false }
		],
		2
	);
	assert.deepEqual(
		merged.tasks.map((task) => task.task.id),
		['m2', 'c1']
	);
	assert.equal(merged.hasMore, true);

	const whole = mergeFleetTasks([
		{ machine: CODE, tasks: [entry(CODE, 'c1', '2026-11-07T10:02:00Z')], hasMore: false }
	]);
	assert.equal(whole.hasMore, false);
	const machineHasMore = mergeFleetTasks([
		{ machine: CODE, tasks: [entry(CODE, 'c1', '2026-11-07T10:02:00Z')], hasMore: true }
	]);
	assert.equal(machineHasMore.hasMore, true);
});

test('a machine still answering a smaller page ends the list at its oldest task', () => {
	// after paging to 6, the local machine answered with all 4 of its tasks while the peer still
	// holds its page of 2
	const merged = mergeFleetTasks(
		[
			{
				machine: CODE,
				tasks: ['10:06', '10:04', '10:02', '10:00'].map((at) =>
					entry(CODE, `c${at}`, `2026-11-07T${at}:00Z`)
				),
				hasMore: false
			},
			{
				machine: MAIN,
				tasks: ['10:05', '10:03'].map((at) => entry(MAIN, `m${at}`, `2026-11-07T${at}:00Z`)),
				hasMore: true
			}
		],
		6
	);
	// the peer may hold tasks between 10:03 and 10:02, so the list stops at its 10:03
	assert.deepEqual(
		merged.tasks.map((task) => task.task.id),
		['c10:06', 'm10:05', 'c10:04', 'm10:03']
	);
	assert.equal(merged.hasMore, true);
});
