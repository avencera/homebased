import { Schema } from 'effect';
import { ApiError, asApiError } from './api';
import { submitResourceAction, type BrowserResourceAction, type ResourceDetail } from './resources';

/** One operation kept until the daemon confirms its result. */
export interface PendingOperation {
	resourceId: string;
	operationId: string;
	expectedRevision: number;
	action: BrowserResourceAction;
}

/** How the dashboard should handle an operation error. */
export type OperationFailure =
	{ type: 'unknown' } | { type: 'stale_revision' } | { type: 'definitive_refusal' };

/** State shown for one resource operation. */
export interface ResourceOperationState {
	operation: PendingOperation | null;
	busy: boolean;
	message: string | null;
	error: string | null;
}

/** Refresh hooks for the page or panel that owns a resource view. */
export interface ResourceOperationEffects {
	refresh(): Promise<void>;
	readError(): ApiError | null;
	showAuthoritative?(detail: ResourceDetail): void;
	refreshAfterSuccess?(): Promise<void>;
}

const StoredOperationSchema = Schema.Struct({
	resourceId: Schema.String,
	operationId: Schema.String,
	expectedRevision: Schema.Finite,
	action: Schema.Union(
		Schema.Struct({ type: Schema.Literal('cancel_queued'), request_id: Schema.String }),
		Schema.Struct({
			type: Schema.Literal('move_queued'),
			request_id: Schema.String,
			placement: Schema.Union(
				Schema.Struct({ type: Schema.Literal('front') }),
				Schema.Struct({ type: Schema.Literal('back') }),
				Schema.Struct({ type: Schema.Literal('before'), request_id: Schema.String }),
				Schema.Struct({ type: Schema.Literal('after'), request_id: Schema.String })
			)
		}),
		Schema.Struct({ type: Schema.Literal('stop_active'), task_id: Schema.String }),
		Schema.Struct({ type: Schema.Literal('renotify'), notice_id: Schema.String })
	)
});

interface MutableResourceOperationState extends ResourceOperationState {
	loaded: boolean;
}

const EMPTY_STATE: ResourceOperationState = {
	operation: null,
	busy: false,
	message: null,
	error: null
};

/** Keep idempotent operation state and browser storage keyed by resource ID. */
export class ResourceOperationManager {
	#states = $state<Record<string, MutableResourceOperationState>>({});

	/** Return the current operation state for one resource. */
	stateFor(resourceId: string): ResourceOperationState {
		return resourceId ? (this.#states[resourceId] ?? EMPTY_STATE) : EMPTY_STATE;
	}

	/** Load a saved operation once for one resource. */
	load(resourceId: string): void {
		if (!resourceId) return;
		const state = this.#ensureState(resourceId);
		if (state.loaded) return;

		state.loaded = true;
		state.operation = readOperation(resourceId);
		state.message = state.operation
			? 'An earlier action has no confirmed result. Retry uses the same operation ID.'
			: null;
	}

	/** Start a new operation with the resource revision currently on screen. */
	async begin(
		resourceId: string,
		expectedRevision: number,
		action: BrowserResourceAction,
		effects: ResourceOperationEffects
	): Promise<void> {
		if (!resourceId) return;
		this.load(resourceId);
		const state = this.#ensureState(resourceId);
		if (state.operation || state.busy) return;

		const pending: PendingOperation = {
			resourceId,
			operationId: makeUuid(),
			expectedRevision,
			action
		};
		state.error = null;
		state.message = null;
		this.#saveOperation(state, pending);
		await this.#sendOperation(state, pending, effects);
	}

	/** Retry a saved operation without changing its operation ID or revision. */
	async retry(resourceId: string, effects: ResourceOperationEffects): Promise<void> {
		if (!resourceId) return;
		this.load(resourceId);
		const state = this.#ensureState(resourceId);
		if (!state.operation || state.busy) return;
		state.error = null;
		state.message = null;
		await this.#sendOperation(state, state.operation, effects);
	}

	/** Clear a dismissed error for one resource. */
	dismissError(resourceId: string): void {
		const state = this.#states[resourceId];
		if (state) state.error = null;
	}

	#ensureState(resourceId: string): MutableResourceOperationState {
		const current = this.#states[resourceId];
		if (current) return current;

		const state: MutableResourceOperationState = { ...EMPTY_STATE, loaded: false };
		this.#states[resourceId] = state;
		return state;
	}

	#saveOperation(state: MutableResourceOperationState, pending: PendingOperation): void {
		state.operation = pending;
		try {
			window.sessionStorage.setItem(
				operationStorageKey(pending.resourceId),
				JSON.stringify(pending)
			);
		} catch {
			state.message = 'The operation ID is held in this page but could not be saved for reload.';
		}
	}

	#clearOperation(state: MutableResourceOperationState, resourceId: string): void {
		state.operation = null;
		try {
			window.sessionStorage.removeItem(operationStorageKey(resourceId));
		} catch {
			// the server outcome is known, so unavailable browser storage does not block the UI
		}
	}

	async #sendOperation(
		state: MutableResourceOperationState,
		pending: PendingOperation,
		effects: ResourceOperationEffects
	): Promise<void> {
		state.busy = true;
		try {
			const updated = await submitResourceAction(
				pending.resourceId,
				pending.expectedRevision,
				pending.operationId,
				pending.action
			);
			effects.showAuthoritative?.(updated);
			this.#clearOperation(state, pending.resourceId);
			state.error = null;
			state.message = `${actionLabel(pending.action)} returned authoritative resource detail.`;
			await effects.refreshAfterSuccess?.();
		} catch (cause) {
			const error = asApiError(cause);
			const failure = classifyOperationFailure(error);
			if (failure.type === 'unknown') {
				state.message =
					'The outcome is unknown. Details are refreshing; retry will send the same request with the same operation ID.';
				state.error = null;
				await effects.refresh();
				if (effects.readError() !== null) {
					state.message = `${state.message} The latest resource read failed.`;
				}
				return;
			}

			this.#clearOperation(state, pending.resourceId);
			state.message = null;
			await effects.refresh();
			const recovery =
				failure.type === 'stale_revision'
					? 'The resource revision changed. Choose the action again to use the refreshed revision.'
					: 'The action was refused. Review the refreshed resource details before choosing an action again.';
			const readError = effects.readError();
			const refreshFailure = readError ? ` The detail refresh failed: ${readError.message}` : '';
			state.error = `${error.code}: ${error.message}. ${recovery}${refreshFailure}`;
		} finally {
			state.busy = false;
		}
	}
}

/** Classify an API error by whether the operation outcome is known. */
export function classifyOperationFailure(error: ApiError): OperationFailure {
	if (error.code === 'resource_stale_revision') return { type: 'stale_revision' };
	if (isUnknownOutcome(error)) return { type: 'unknown' };
	return { type: 'definitive_refusal' };
}

function isUnknownOutcome(error: ApiError): boolean {
	return (
		error.httpStatus === null ||
		error.httpStatus >= 500 ||
		error.code === 'invalid_response' ||
		error.code === 'invalid_json' ||
		[
			'resource_outcome_unknown',
			'resource_operation_unavailable',
			'resource_authority_unavailable'
		].includes(error.code)
	);
}

function actionLabel(action: BrowserResourceAction): string {
	switch (action.type) {
		case 'cancel_queued':
			return 'Cancel queued request';
		case 'move_queued':
			return 'Move queued request';
		case 'stop_active':
			return 'Stop active test';
		case 'renotify':
			return 'Retry failed notification';
	}
}

function readOperation(resourceId: string): PendingOperation | null {
	try {
		const raw = window.sessionStorage.getItem(operationStorageKey(resourceId));
		if (!raw) return null;
		const value: unknown = JSON.parse(raw);
		const saved = Schema.decodeUnknownSync(StoredOperationSchema)(value);
		if (saved.resourceId !== resourceId) {
			window.sessionStorage.removeItem(operationStorageKey(resourceId));
			return null;
		}
		return saved;
	} catch {
		try {
			window.sessionStorage.removeItem(operationStorageKey(resourceId));
		} catch {
			return null;
		}
		return null;
	}
}

function operationStorageKey(resourceId: string): string {
	return `homebased:resource-operation:${resourceId}`;
}

function makeUuid(): string {
	if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
		return crypto.randomUUID();
	}
	const bytes = new Uint8Array(16);
	if (typeof crypto !== 'undefined' && typeof crypto.getRandomValues === 'function') {
		crypto.getRandomValues(bytes);
	} else {
		for (let index = 0; index < bytes.length; index += 1) {
			bytes[index] = Math.floor(Math.random() * 256);
		}
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0'));
	return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex
		.slice(6, 8)
		.join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10).join('')}`;
}
