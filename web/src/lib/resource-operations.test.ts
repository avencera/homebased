import assert from 'node:assert/strict';
import { test } from 'node:test';

import { ApiError, asApiError } from './api.ts';
import {
	classifyOperationFailure,
	operationStorageKey,
	ResourceOperationManager,
	type OperationStorage,
	type ResourceOperationEffects
} from './resource-operations.ts';
import type { BrowserResourceAction, ResourceDetail } from './resources.ts';

const resourceA = '11111111-1111-4111-8111-111111111111';
const resourceB = '22222222-2222-4222-8222-222222222222';
const cancelA: BrowserResourceAction = {
	type: 'cancel_queued',
	request_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
};
const cancelB: BrowserResourceAction = {
	type: 'cancel_queued',
	request_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
};

function apiError(code: string, httpStatus: number | null): ApiError {
	return new ApiError({ code, message: 'request failed', retryable: false, input: {} }, httpStatus);
}

function memoryStorage(): OperationStorage & { keys(): string[] } {
	const values = new Map<string, string>();
	return {
		getItem(key) {
			return values.get(key) ?? null;
		},
		setItem(key, value) {
			values.set(key, value);
		},
		removeItem(key) {
			values.delete(key);
		},
		keys() {
			return [...values.keys()];
		}
	};
}

function detail(resourceId: string, revision = 3): ResourceDetail {
	return {
		api_version: 1,
		resource: {
			id: resourceId,
			display_name: 'shared GPU',
			authority_machine: '33333333-3333-4333-8333-333333333333',
			supervisor: {
				machine: '33333333-3333-4333-8333-333333333333',
				thread: '44444444-4444-4444-8444-444444444444'
			},
			assignment_revision: 0,
			state_revision: revision,
			registered_background_task: null
		},
		loan: null,
		requests: [],
		notices: [],
		current_task: null,
		background_task: null,
		attention: null
	};
}

function effects(overrides: Partial<ResourceOperationEffects> = {}): ResourceOperationEffects {
	return {
		refresh: async () => {},
		readError: () => null,
		...overrides
	};
}

test('unknown outcomes keep the same operation id for retry', async () => {
	const calls: string[] = [];
	const manager = new ResourceOperationManager({
		storage: memoryStorage(),
		asApiError,
		uuid: () => 'op-1',
		submit: async (_id, _revision, operationId) => {
			calls.push(operationId);
			throw apiError('resource_outcome_unknown', 503);
		}
	});
	const view = effects();

	await manager.begin(resourceA, 4, cancelA, view);
	assert.equal(manager.stateFor(resourceA).operation?.operationId, 'op-1');
	assert.equal(classifyOperationFailure(apiError('resource_outcome_unknown', 503)).type, 'unknown');
	assert.equal(classifyOperationFailure(apiError('daemon_unavailable', null)).type, 'unknown');
	assert.equal(classifyOperationFailure(apiError('invalid_json', 200)).type, 'unknown');

	await manager.retry(resourceA, view);
	assert.deepEqual(calls, ['op-1', 'op-1']);
	assert.equal(manager.stateFor(resourceA).operation?.expectedRevision, 4);
});

test('stale revisions and refusals drop the operation so the next click uses a new revision', async () => {
	const manager = new ResourceOperationManager({
		storage: memoryStorage(),
		asApiError,
		uuid: () => 'op-stale',
		submit: async () => {
			throw apiError('resource_stale_revision', 409);
		}
	});

	await manager.begin(resourceA, 4, cancelA, effects());
	assert.equal(manager.stateFor(resourceA).operation, null);
	assert.match(manager.stateFor(resourceA).error ?? '', /revision changed/);
	assert.equal(
		classifyOperationFailure(apiError('resource_stale_revision', 409)).type,
		'stale_revision'
	);
	assert.equal(
		classifyOperationFailure(apiError('resource_not_allowed', 403)).type,
		'definitive_refusal'
	);
});

test('a pending operation on one resource does not block or retry another', async () => {
	let releaseA: (value: ResourceDetail) => void = () => {};
	const heldA = new Promise<ResourceDetail>((resolve) => {
		releaseA = resolve;
	});
	const calls: { resourceId: string; operationId: string }[] = [];
	let nextId = 0;
	const manager = new ResourceOperationManager({
		storage: memoryStorage(),
		asApiError,
		uuid: () => `op-${++nextId}`,
		submit: async (resourceId, _revision, operationId) => {
			calls.push({ resourceId, operationId });
			if (resourceId === resourceA) return heldA;
			throw apiError('resource_authority_unavailable', 503);
		}
	});

	const startedA = manager.begin(resourceA, 1, cancelA, effects());
	await manager.begin(resourceB, 2, cancelB, effects());

	assert.equal(manager.stateFor(resourceA).busy, true);
	assert.equal(manager.stateFor(resourceA).operation?.operationId, 'op-1');
	assert.equal(manager.stateFor(resourceB).busy, false);
	assert.equal(manager.stateFor(resourceB).operation?.operationId, 'op-2');

	await manager.retry(resourceB, effects());
	releaseA(detail(resourceA, 5));
	await startedA;

	assert.deepEqual(calls, [
		{ resourceId: resourceA, operationId: 'op-1' },
		{ resourceId: resourceB, operationId: 'op-2' },
		{ resourceId: resourceB, operationId: 'op-2' }
	]);
	assert.equal(manager.stateFor(resourceA).operation, null);
	assert.equal(manager.stateFor(resourceB).operation?.operationId, 'op-2');
});

test('a second click is ignored while the same resource operation is in flight', async () => {
	let release: (value: ResourceDetail) => void = () => {};
	const held = new Promise<ResourceDetail>((resolve) => {
		release = resolve;
	});
	let calls = 0;
	const manager = new ResourceOperationManager({
		storage: memoryStorage(),
		asApiError,
		uuid: () => 'op-once',
		submit: async () => {
			calls += 1;
			return held;
		}
	});

	const first = manager.begin(resourceA, 1, cancelA, effects());
	const second = manager.begin(resourceA, 1, cancelA, effects());
	release(detail(resourceA));
	await Promise.all([first, second]);

	assert.equal(calls, 1);
});

test('a failed resource read blocks a new operation and still allows retry of an unknown outcome', async () => {
	let blocked: ApiError | null = apiError('daemon_unavailable', null);
	let calls = 0;
	const manager = new ResourceOperationManager({
		storage: memoryStorage(),
		asApiError,
		uuid: () => 'op-retry',
		submit: async () => {
			calls += 1;
			throw apiError('resource_authority_unavailable', 503);
		}
	});

	await manager.begin(resourceA, 1, cancelA, effects({ readError: () => blocked }));
	assert.equal(calls, 0);
	assert.equal(manager.stateFor(resourceA).operation, null);

	blocked = null;
	await manager.begin(
		resourceA,
		1,
		cancelA,
		effects({
			readError: () => blocked,
			refresh: async () => {
				blocked = apiError('daemon_unavailable', null);
			}
		})
	);
	assert.equal(manager.stateFor(resourceA).operation?.operationId, 'op-retry');
	assert.match(manager.stateFor(resourceA).message ?? '', /latest resource read failed/);

	await manager.retry(resourceA, effects({ readError: () => blocked }));
	assert.equal(calls, 2);
	assert.equal(manager.stateFor(resourceA).operation?.operationId, 'op-retry');
});

test('a confirmed result is applied immediately and the saved operation is cleared', async () => {
	const storage = memoryStorage();
	let shownRevision: number | null = null;
	const manager = new ResourceOperationManager({
		storage,
		asApiError,
		uuid: () => 'op-ok',
		submit: async (id) => detail(id, 9)
	});

	await manager.begin(
		resourceA,
		3,
		cancelA,
		effects({
			showAuthoritative: (updated) => {
				shownRevision = updated.resource.state_revision;
			}
		})
	);

	assert.equal(shownRevision, 9);
	assert.equal(manager.stateFor(resourceA).operation, null);
	assert.equal(storage.getItem(operationStorageKey(resourceA)), null);
	assert.match(manager.stateFor(resourceA).message ?? '', /authoritative resource detail/);
});

test('saved operations are stored per resource and rejected when the resource id does not match', () => {
	const storage = memoryStorage();
	const saved = {
		resourceId: resourceA,
		operationId: 'op-a',
		expectedRevision: 3,
		action: cancelA
	};
	storage.setItem(operationStorageKey(resourceA), JSON.stringify(saved));
	storage.setItem(
		operationStorageKey(resourceB),
		JSON.stringify({ ...saved, resourceId: resourceA })
	);

	const manager = new ResourceOperationManager({
		storage,
		asApiError,
		uuid: () => 'unused',
		submit: async () => detail(resourceA)
	});
	manager.load(resourceA);
	manager.load(resourceB);

	assert.equal(manager.stateFor(resourceA).operation?.operationId, 'op-a');
	assert.equal(manager.stateFor(resourceB).operation, null);
	assert.equal(storage.getItem(operationStorageKey(resourceB)), null);
	assert.deepEqual(
		storage.keys().filter((key) => key.startsWith('homebased:resource-operation:')),
		[operationStorageKey(resourceA)]
	);
});
