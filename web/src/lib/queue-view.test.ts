import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { JobRecord, Priority } from './api.ts';
import {
	dropPlacement,
	groupByLevel,
	newOperationId,
	nudgePlacement,
	stepProgress,
	waitingJobs
} from './queue-view.ts';

function job(
	id: string,
	priority: Priority,
	position: number,
	extra: Partial<JobRecord> = {}
): JobRecord {
	return {
		id,
		machine: 'm',
		origin: 'm',
		spec: {
			name: id,
			cwd: '/tmp',
			thread: 't',
			preempt: { mode: 'wait' },
			steps: [{ type: 'task', command: ['true'] }]
		},
		target: { type: 'any' },
		priority,
		position,
		state: { state: 'queued', resume: false },
		step: 0,
		runs: 0,
		created_at: '2026-10-03T00:00:00Z',
		updated_at: '2026-10-03T00:00:00Z',
		...extra
	};
}

test('jobs group by level in serving order, keeping empty levels', () => {
	const groups = groupByLevel([job('b', 'low', 2), job('a', 'low', 1), job('h', 'high', 1)]);
	assert.deepEqual(
		groups.map((group) => [group.priority, group.jobs.map((entry) => entry.id)]),
		[
			['high', ['h']],
			['medium', []],
			['low', ['a', 'b']]
		]
	);
});

test('waiting jobs skip the active ones and keep serving order', () => {
	const active = job('run', 'high', 1, { state: { state: 'active', resource: 'r0' } });
	const jobs = [job('low', 'low', 1), active, job('next', 'high', 2)];
	assert.deepEqual(
		waitingJobs(jobs).map((entry) => entry.id),
		['next', 'low']
	);
});

test('a nudge swaps inside a level and crosses to the near end of the next level', () => {
	const groups = groupByLevel([
		job('h1', 'high', 1),
		job('m1', 'medium', 1),
		job('m2', 'medium', 2),
		job('l1', 'low', 1)
	]);
	assert.deepEqual(nudgePlacement(groups, 'm2', 'up'), {
		type: 'relative',
		target: 'm1',
		side: 'before',
		expect: 'medium'
	});
	assert.deepEqual(nudgePlacement(groups, 'm1', 'up'), {
		type: 'edge',
		priority: 'high',
		end: 'back'
	});
	assert.deepEqual(nudgePlacement(groups, 'm2', 'down'), {
		type: 'edge',
		priority: 'low',
		end: 'front'
	});
	assert.equal(nudgePlacement(groups, 'h1', 'up'), null);
	assert.equal(nudgePlacement(groups, 'l1', 'down'), null);
	assert.equal(nudgePlacement(groups, 'missing', 'up'), null);
});

test('a nudge past an empty level lands in that level', () => {
	const groups = groupByLevel([job('h1', 'high', 1), job('l1', 'low', 1)]);
	assert.deepEqual(nudgePlacement(groups, 'l1', 'up'), {
		type: 'edge',
		priority: 'medium',
		end: 'back'
	});
});

test('a drop takes the target level and refuses the dragged job itself', () => {
	const target = job('t', 'high', 1);
	assert.deepEqual(dropPlacement('d', target, 'after'), {
		type: 'relative',
		target: 't',
		side: 'after',
		expect: 'high'
	});
	assert.equal(dropPlacement('t', target, 'before'), null);
});

test('operation IDs are version 4 UUIDs', () => {
	const id = newOperationId((bytes) => bytes.fill(0xff));
	assert.equal(id, 'ffffffff-ffff-4fff-bfff-ffffffffffff');
	assert.match(
		newOperationId(),
		/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
	);
	assert.notEqual(newOperationId(), newOperationId());
});

test('step progress follows the job and stops at a failed run', () => {
	const steps = Array.from({ length: 3 }, (_, index) => ({
		type: 'task' as const,
		command: ['run', String(index)]
	}));
	const base = job('s', 'low', 1, { spec: { ...job('s', 'low', 1).spec, steps } });
	const queued = { ...base, step: 1 };
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(queued, index, null)),
		['done', 'current', 'pending']
	);
	const failed = {
		...base,
		step: 1,
		position: null,
		state: { state: 'failed' as const, run: 'r' }
	};
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(failed, index, 1)),
		['done', 'failed', 'skipped']
	);
	const cancelled = {
		...base,
		step: 2,
		position: null,
		state: { state: 'cancelled' as const }
	};
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(cancelled, index, null)),
		['done', 'done', 'skipped']
	);
});
