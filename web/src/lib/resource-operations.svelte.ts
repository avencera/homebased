import { asApiError } from './api';
import {
	ResourceOperationManager as ResourceOperationEngine,
	type ResourceOperationEffects,
	type ResourceOperationState
} from './resource-operations';
import { submitResourceAction, type BrowserResourceAction } from './resources';

export type {
	PendingOperation,
	ResourceOperationEffects,
	ResourceOperationState
} from './resource-operations';

/** Reactive view of per-resource operation state for Svelte callers. */
export class ResourceOperationManager {
	#generation = $state(0);
	#engine = new ResourceOperationEngine({
		submit: submitResourceAction,
		asApiError,
		onChange: () => {
			this.#generation += 1;
		}
	});

	/** Return the current operation state for one resource. */
	stateFor(resourceId: string): ResourceOperationState {
		// reading the generation makes each caller rerun when the plain engine changes
		return this.#snapshot(this.#generation, resourceId);
	}

	#snapshot(_generation: number, resourceId: string): ResourceOperationState {
		return this.#engine.stateFor(resourceId);
	}

	/** Load a saved operation once for one resource. */
	load(resourceId: string): void {
		this.#engine.load(resourceId);
	}

	/** Start a new operation with the resource revision currently on screen. */
	async begin(
		resourceId: string,
		expectedRevision: number,
		action: BrowserResourceAction,
		effects: ResourceOperationEffects
	): Promise<void> {
		await this.#engine.begin(resourceId, expectedRevision, action, effects);
	}

	/** Retry a saved operation without changing its operation ID or revision. */
	async retry(resourceId: string, effects: ResourceOperationEffects): Promise<void> {
		await this.#engine.retry(resourceId, effects);
	}

	/** Clear a dismissed error for one resource. */
	dismissError(resourceId: string): void {
		this.#engine.dismissError(resourceId);
	}
}
