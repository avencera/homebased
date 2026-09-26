import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
	detailStatus,
	overviewStatus,
	queueMovePlacement,
	queueThreads,
	resourceQueue,
	returnPendingText
} from './resource-state.ts';
import type { ResourceDetail, ResourceOverviewItem } from './resources';

const resource: ResourceOverviewItem['resource'] = {
	id: '11111111-1111-4111-8111-111111111111',
	display_name: 'shared GPU',
	authority_machine: '22222222-2222-4222-8222-222222222222',
	supervisor: {
		machine: '22222222-2222-4222-8222-222222222222',
		thread: '33333333-3333-4333-8333-333333333333'
	},
	assignment_revision: 0,
	state_revision: 3,
	registered_background_task: null
};

const launch = (status: string) => ({
	request_id: '44444444-4444-4444-8444-444444444444',
	task_id: '55555555-5555-4555-8555-555555555555',
	status
});

const item = (overrides: Partial<ResourceOverviewItem>): ResourceOverviewItem => ({
	resource,
	loan: null,
	queued_count: 0,
	current_task: null,
	attention: null,
	...overrides
});

const queuedRequest = (
	requestId: string,
	acceptanceSequence: number
): ResourceDetail['requests'][number] => ({
	request_id: requestId,
	task_id: `task-${requestId}`,
	acceptance_sequence: acceptanceSequence,
	origin_machine: resource.authority_machine,
	display_name: requestId,
	state: { type: 'queued' }
});

test('an early-ended first launch with an empty queue is not available', () => {
	const status = overviewStatus(
		item({
			attention: {
				code: 'background_launch_release_unproven',
				message: 'GPU release is not proven'
			},
			background_launch: launch('release_unproven')
		}),
		[]
	);
	assert.equal(status.label, 'Operator release required');
	assert.equal(status.tone, 'amber');
});

test('the reservation stays visible without an attention view', () => {
	for (const status of ['release_unproven', 'queued', 'started_unregistered', 'newer_reason']) {
		const derived = overviewStatus(item({ background_launch: launch(status) }), []);
		assert.notEqual(derived.label, 'Available', status);
		assert.notEqual(derived.tone, 'green', status);
	}
});

test('detail status reads the same launch reservation', () => {
	const detail: ResourceDetail = {
		api_version: 1,
		resource,
		loan: null,
		requests: [],
		notices: [],
		current_task: null,
		background_task: null,
		attention: null,
		background_launch: launch('release_unproven')
	};
	assert.equal(detailStatus(detail).label, 'Operator release required');
	assert.equal(detailStatus({ ...detail, background_launch: undefined }).label, 'Available');
});

test('resource queues keep the server serving order', () => {
	const first = queuedRequest('first', 9);
	const second = queuedRequest('second', 2);
	const detail: ResourceDetail = {
		api_version: 1,
		resource,
		loan: null,
		requests: [first, second],
		notices: [],
		current_task: null,
		background_task: null,
		attention: null
	};

	assert.deepEqual(resourceQueue(detail).queue, [first, second]);
});

test('one-place queue moves use front and back at the ends', () => {
	const two = [queuedRequest('first', 1), queuedRequest('second', 2)];
	const four = [
		queuedRequest('first', 1),
		queuedRequest('second', 2),
		queuedRequest('third', 3),
		queuedRequest('fourth', 4)
	];

	assert.deepEqual(queueMovePlacement(two, 1, 'up'), { type: 'front' });
	assert.deepEqual(queueMovePlacement(two, 0, 'down'), { type: 'back' });
	assert.deepEqual(queueMovePlacement(four, 1, 'up'), { type: 'front' });
	assert.deepEqual(queueMovePlacement(four, 2, 'down'), { type: 'back' });
	assert.deepEqual(queueMovePlacement(four, 2, 'up'), { type: 'before', request_id: 'second' });
	assert.deepEqual(queueMovePlacement(four, 1, 'down'), { type: 'after', request_id: 'third' });
	assert.equal(queueMovePlacement(four, 0, 'up'), null);
	assert.equal(queueMovePlacement(four, 3, 'down'), null);
	assert.equal(queueMovePlacement(four, -1, 'down'), null);
});

const returnWindow = (deadline: string) => ({
	action_id: '66666666-6666-4666-8666-666666666666',
	opened_at: '2026-09-26T06:00:00Z',
	deadline_at: deadline
});

test('a pending return names when queued work may take the resource', () => {
	const now = Date.parse('2026-09-26T06:01:00Z');
	const open = returnWindow('2026-09-26T06:02:00Z');

	assert.equal(
		returnPendingText(2, open, now),
		'Queued work starts at 1:02:00 AM unless the supervisor decides first'
	);
	assert.equal(
		returnPendingText(0, open, now),
		'The supervisor has until 1:02:00 AM to decide before new queued work can start'
	);
	assert.equal(
		returnPendingText(1, returnWindow('2026-09-26T06:00:30Z'), now),
		'The decision time ended; queued work starts now'
	);
	assert.equal(returnPendingText(1, undefined, now), 'The queue drained and return is reserved');
});

test('a queue card explains a pending return only while the loan awaits one', () => {
	const detail: ResourceDetail = {
		api_version: 1,
		resource,
		loan: {
			id: '77777777-7777-4777-8777-777777777777',
			resource_id: resource.id,
			state: {
				type: 'active',
				phase: {
					type: 'awaiting_return',
					action_id: '66666666-6666-4666-8666-666666666666',
					return_context: { type: 'idle' }
				}
			}
		},
		requests: [queuedRequest('first', 1)],
		notices: [],
		current_task: null,
		background_task: null,
		attention: null,
		return_window: returnWindow('2999-01-01T00:00:00Z')
	};

	assert.match(resourceQueue(detail).returnPending ?? '', /^Queued work starts at /);
	assert.equal(resourceQueue({ ...detail, loan: null }).returnPending, null);
});

test('job threads resolve to the machine that runs each thread', () => {
	const peer = '88888888-8888-4888-8888-888888888888';
	const task = (id: string, thread: string | undefined, origin: string | null) => ({
		id,
		display_name: id,
		status: 'running',
		thread,
		origin_machine: origin,
		execution_machine: origin ? resource.authority_machine : null
	});
	const detail: ResourceDetail = {
		api_version: 1,
		resource,
		loan: null,
		requests: [
			{ ...queuedRequest('remote', 1), origin_machine: peer, thread: 'queued-thread' },
			queuedRequest('older-authority', 2)
		],
		notices: [],
		current_task: task('current', 'current-thread', peer),
		background_task: task('background', 'background-thread', null),
		attention: null
	};

	assert.deepEqual(queueThreads(resourceQueue(detail)), [
		{ machine: peer, thread: 'current-thread' },
		{ machine: resource.authority_machine, thread: 'background-thread' },
		{ machine: peer, thread: 'queued-thread' }
	]);
});
