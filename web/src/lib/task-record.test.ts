import assert from 'node:assert/strict';
import { afterEach, test } from 'node:test';

import { fetchTask, isTaskDetail } from './api.ts';

const originalFetch = globalThis.fetch;

afterEach(() => {
	globalThis.fetch = originalFetch;
});

function answer(body: unknown) {
	globalThis.fetch = async () => Response.json(body);
}

// held on its origin until a battery Mac accepts it: no task directory yet
const held = {
	api_version: 1,
	availability: 'held',
	callback: 'pending',
	cancel_requested_at: null,
	check_timeout: 'pending',
	created_at: '2026-10-22T21:12:01.054Z',
	cwd: '~/',
	execution_machine: '01a15b13-ec3e-7286-8a56-f5dd83d51a3a',
	exit_reason: null,
	failed_events: [],
	found_on: '01a0d11a-c5ad-71c2-8250-0cb4c7b5b135',
	id: '01a16574-e11e-71a3-916b-0e757619e174',
	last_accepted_seq: 0,
	last_event: null,
	last_settled_seq: 0,
	name: 'fleet update',
	origin_machine: '01a0d11a-c5ad-71c2-8250-0cb4c7b5b135',
	pid: null,
	reports: [],
	status: 'held',
	submission: { phase: { type: 'waiting' }, type: 'held' },
	thread: '00000000-0000-0000-0000-000000000000',
	timeout_secs: 3600,
	updated_at: '2026-10-22T21:12:01.054409Z',
	wait_for_machine: true,
	waiting_events: [],
	workload: { command: ['/bin/sh', '-c', 'cmd update'], type: 'task' }
};

test('a task held on its origin decodes without a log or task directory', async () => {
	answer(held);
	const record = await fetchTask(held.id);
	assert.ok(isTaskDetail(record));
	assert.equal(record.status, 'held');
	assert.equal(record.output_log, undefined);
});

test('a task known only by its route decodes as a route', async () => {
	answer({
		api_version: 1,
		availability: 'detail_unavailable',
		execution_machine: held.execution_machine,
		failed_events: [],
		found_on: held.origin_machine,
		id: held.id,
		last_accepted_seq: 0,
		last_settled_seq: 0,
		last_update: held.updated_at,
		origin_machine: held.origin_machine,
		status: 'held',
		submission: held.submission
	});
	const record = await fetchTask(held.id);
	assert.ok(!isTaskDetail(record));
	assert.equal(record.availability, 'detail_unavailable');
	assert.equal(record.execution_machine, held.execution_machine);
});
