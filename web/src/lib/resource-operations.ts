import { Schema } from 'effect';
import { BrowserResourceActionSchema, type BrowserResourceAction } from './resource-actions.ts';
import type { ResourceDetail } from './resources';

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

/** Error shape used to classify an operation outcome. */
export interface OperationError {
	code: string;
	message: string;
	httpStatus: number | null;
}

/** Refresh hooks for the page or panel that owns a resource view. */
export interface ResourceOperationEffects {
	refresh(): Promise<void>;
	readError(): OperationError | null;
	showAuthoritative?(detail: ResourceDetail): void;
	refreshAfterSuccess?(): Promise<void>;
}

/** Browser storage used to keep an unconfirmed operation across reloads. */
export interface OperationStorage {
	getItem(key: string): string | null;
	setItem(key: string, value: string): void;
	removeItem(key: string): void;
}

/** Injectable I/O so tests can drive the manager without a browser or daemon. */
export interface ResourceOperationDeps {
	submit: (
		id: string,
		expectedRevision: number,
		operationId: string,
		action: BrowserResourceAction
	) => Promise<ResourceDetail>;
	asApiError: (cause: unknown) => OperationError;
	storage?: OperationStorage;
	uuid?: () => string;
	onChange?: () => void;
}

const StoredOperationSchema = Schema.Struct({
	resourceId: Schema.String,
	operationId: Schema.String,
	expectedRevision: Schema.Finite,
	action: BrowserResourceActionSchema
});

interface MutableResourceOperationState extends ResourceOperationState {
	loaded: boolean;
}

const EMPTY_STATE: ResourceOperationState = Object.freeze({
	operation: null,
	busy: false,
	message: null,
	error: null
});

/** Classify an API error by whether the operation outcome is known. */
export function classifyOperationFailure(error: OperationError): OperationFailure {
	if (error.code === 'resource_stale_revision') return { type: 'stale_revision' };
	if (isUnknownOutcome(error)) return { type: 'unknown' };
	return { type: 'definitive_refusal' };
}

/** Session key for one resource; the resource id is the whole suffix after the last colon. */
export function operationStorageKey(resourceId: string): string {
	return `homebased:resource-operation:${resourceId}`;
}

/** Keep idempotent operation state and browser storage keyed by resource ID. */
export class ResourceOperationManager {
	#states: Record<string, MutableResourceOperationState> = {};
	#storage: OperationStorage;
	#submit: ResourceOperationDeps['submit'];
	#asApiError: (cause: unknown) => OperationError;
	#uuid: () => string;
	#onChange: (() => void) | undefined;

	constructor(deps: ResourceOperationDeps) {
		this.#storage = deps.storage ?? defaultStorage();
		this.#submit = deps.submit;
		this.#asApiError = deps.asApiError;
		this.#uuid = deps.uuid ?? makeUuid;
		this.#onChange = deps.onChange;
	}

	/** Return a copy of the current operation state for one resource. */
	stateFor(resourceId: string): ResourceOperationState {
		const state = resourceId ? this.#states[resourceId] : undefined;
		if (!state) return { ...EMPTY_STATE };
		return {
			operation: state.operation,
			busy: state.busy,
			message: state.message,
			error: state.error
		};
	}

	/** Load a saved operation once for one resource. */
	load(resourceId: string): void {
		if (!resourceId) return;
		const state = this.#ensureState(resourceId);
		if (state.loaded) return;

		state.loaded = true;
		state.operation = readOperation(this.#storage, resourceId);
		state.message = state.operation
			? 'An earlier action has no confirmed result. Retry uses the same operation ID.'
			: null;
		this.#notify();
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
		if (state.operation || state.busy || effects.readError() !== null) return;

		const pending: PendingOperation = {
			resourceId,
			operationId: this.#uuid(),
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
		this.#notify();
		await this.#sendOperation(state, state.operation, effects);
	}

	/** Clear a dismissed error for one resource. */
	dismissError(resourceId: string): void {
		const state = this.#states[resourceId];
		if (!state || state.error === null) return;
		state.error = null;
		this.#notify();
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
			this.#storage.setItem(operationStorageKey(pending.resourceId), JSON.stringify(pending));
		} catch {
			state.message = 'The operation ID is held in this page but could not be saved for reload.';
		}
		this.#notify();
	}

	#clearOperation(state: MutableResourceOperationState, resourceId: string): void {
		state.operation = null;
		try {
			this.#storage.removeItem(operationStorageKey(resourceId));
		} catch {
			// the server outcome is known, so unavailable browser storage does not block the UI
		}
		this.#notify();
	}

	async #sendOperation(
		state: MutableResourceOperationState,
		pending: PendingOperation,
		effects: ResourceOperationEffects
	): Promise<void> {
		state.busy = true;
		this.#notify();
		try {
			let updated: ResourceDetail;
			try {
				updated = await this.#submit(
					pending.resourceId,
					pending.expectedRevision,
					pending.operationId,
					pending.action
				);
			} catch (cause) {
				await this.#handleFailure(state, pending, effects, cause);
				return;
			}

			effects.showAuthoritative?.(updated);
			this.#clearOperation(state, pending.resourceId);
			state.error = null;
			state.message = `${actionLabel(pending.action)} returned authoritative resource detail.`;
			this.#notify();
			await effects.refreshAfterSuccess?.();
		} finally {
			state.busy = false;
			this.#notify();
		}
	}

	async #handleFailure(
		state: MutableResourceOperationState,
		pending: PendingOperation,
		effects: ResourceOperationEffects,
		cause: unknown
	): Promise<void> {
		const error = this.#asApiError(cause);
		const failure = classifyOperationFailure(error);
		if (failure.type === 'unknown') {
			state.message =
				'The outcome is unknown. Details are refreshing; retry will send the same request with the same operation ID.';
			state.error = null;
			this.#notify();
			await effects.refresh();
			if (effects.readError() !== null) {
				state.message = `${state.message} The latest resource read failed.`;
				this.#notify();
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
		this.#notify();
	}

	#notify(): void {
		this.#onChange?.();
	}
}

function isUnknownOutcome(error: OperationError): boolean {
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

function readOperation(storage: OperationStorage, resourceId: string): PendingOperation | null {
	try {
		const raw = storage.getItem(operationStorageKey(resourceId));
		if (!raw) return null;
		const value: unknown = JSON.parse(raw);
		const saved = Schema.decodeUnknownSync(StoredOperationSchema)(value);
		if (saved.resourceId !== resourceId) {
			storage.removeItem(operationStorageKey(resourceId));
			return null;
		}
		return saved;
	} catch {
		try {
			storage.removeItem(operationStorageKey(resourceId));
		} catch {
			return null;
		}
		return null;
	}
}

function defaultStorage(): OperationStorage {
	if (typeof globalThis.sessionStorage !== 'undefined') return globalThis.sessionStorage;
	return memoryStorage();
}

function memoryStorage(): OperationStorage {
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
		}
	};
}

// randomUUID needs a secure context, and the LAN dashboard is served over plain HTTP
function makeUuid(): string {
	if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
	const bytes = crypto.getRandomValues(new Uint8Array(16));
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
