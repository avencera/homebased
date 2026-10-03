import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { JobRecord, Priority, ResourceRecord } from './api.ts';
import {
	cleanupLabel,
	dropPlacement,
	groupByLevel,
	newOperationId,
	nudgePlacement,
	phaseLabel,
	phaseSince,
	preemptionLabel,
	queueHasWork,
	resourceState,
	stepLabel,
	stepProgress,
	stepText,
	targetLabel,
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
		state: { state: 'queued', next_step: 0, resume: false },
		next_step: 0,
		resume: false,
		runs: 0,
		created_at: '2026-10-03T00:00:00Z',
		updated_at: '2026-10-03T00:00:00Z',
		...extra
	};
}

const idle: ResourceRecord = {
	machine: 'm',
	resource: { id: 'r0', name: 'gpu0', device: null },
	run: null
};

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

test('resource state follows the run phase', () => {
	assert.equal(resourceState(idle), 'idle');
	const stopping: ResourceRecord = {
		...idle,
		run: {
			resource: 'r0',
			job: 'j',
			task: 'task',
			run_number: 1,
			step: 0,
			phase: {
				phase: 'stopping',
				started_at: null,
				cause: 'yield',
				requested_at: '2026-10-03T01:00:00Z'
			}
		}
	};
	assert.equal(resourceState(stopping), 'stopping');
	assert.equal(phaseLabel(stopping.run!.phase), 'stopping · yield');
	assert.equal(phaseSince(stopping.run!.phase), '2026-10-03T01:00:00Z');
	assert.equal(phaseLabel({ phase: 'cleaning', attempt: 2 }), 'cleaning · try 2');
	assert.equal(phaseSince({ phase: 'cleaning', attempt: 1 }), null);
});

test('the queue has work while a resource is held or a job remains', () => {
	assert.equal(queueHasWork([idle], []), false);
	assert.equal(queueHasWork([idle], [job('a', 'low', 1)]), true);
});

test('labels name the mode, target, step, and cleanup', () => {
	assert.equal(preemptionLabel({ mode: 'restart' }), 'restart');
	assert.equal(preemptionLabel({ mode: 'wait' }), 'wait');
	assert.equal(preemptionLabel({ mode: 'yield', restart_within: '5m' }), 'yield · restart 5m');
	const names = new Map([['r1', 'gpu1']]);
	assert.equal(targetLabel({ type: 'any' }, names), 'any');
	assert.equal(targetLabel({ type: 'pinned', resource: 'r1' }, names), 'gpu1');
	assert.equal(targetLabel({ type: 'pinned', resource: '0123456789' }, names), '01234567');
	const twoSteps = job('s', 'low', 1, {
		next_step: 1,
		spec: {
			...job('s', 'low', 1).spec,
			steps: [
				{ type: 'task', command: ['a'] },
				{ type: 'task', command: ['b'] }
			]
		}
	});
	assert.equal(stepLabel(twoSteps), 'step 2/2');
	assert.equal(stepLabel({ ...twoSteps, next_step: 2, state: { state: 'succeeded' } }), 'step 2/2');
	assert.equal(cleanupLabel(null), '—');
	assert.equal(cleanupLabel({ Ok: null }), 'clean');
	assert.equal(
		cleanupLabel({ Err: { kind: 'processes', failure: { kind: 'group_survived', pgid: 42 } } }),
		'process group 42 survived SIGKILL'
	);
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
	const queued = { ...base, next_step: 1 };
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(queued, index, null)),
		['done', 'current', 'pending']
	);
	const failed = {
		...base,
		next_step: 1,
		position: null,
		state: { state: 'failed' as const, run: 'r' }
	};
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(failed, index, 1)),
		['done', 'failed', 'skipped']
	);
	const cancelled = {
		...base,
		next_step: 2,
		position: null,
		state: { state: 'cancelled' as const }
	};
	assert.deepEqual(
		[0, 1, 2].map((index) => stepProgress(cancelled, index, null)),
		['done', 'done', 'skipped']
	);
	assert.equal(stepText(steps[0]), 'run 0');
	assert.equal(
		stepText({ type: 'container', image: 'busybox@sha256:abc' }),
		'container busybox@sha256:abc'
	);
});
