import assert from 'node:assert/strict';
import { test } from 'node:test';

import { detailStatus, overviewStatus } from './resource-state.ts';
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
