import type {
	BackgroundLaunchReservation,
	ResourceDetail,
	ResourceOverviewItem,
	ResourceTaskSummary,
	QueuePlacement
} from './resources';

export type ResourceTone = 'green' | 'blue' | 'amber' | 'red' | 'neutral';

export interface ResourceStatus {
	label: string;
	message: string;
	tone: ResourceTone;
}

/** Return a safe object view for a schema-validated extensible tagged value. */
export function asRecord(value: unknown): Record<string, unknown> | null {
	if (typeof value !== 'object' || value === null || Array.isArray(value)) return null;
	return value as Record<string, unknown>;
}

/** Read a string field from an extensible tagged value. */
export function stringField(value: unknown, key: string): string | null {
	const field = asRecord(value)?.[key];
	return typeof field === 'string' ? field : null;
}

/** Read a finite number field from an extensible tagged value. */
export function numberField(value: unknown, key: string): number | null {
	const field = asRecord(value)?.[key];
	return typeof field === 'number' && Number.isFinite(field) ? field : null;
}

/** Read a tagged enum name without assuming the server knows every variant. */
export function tagOf(value: unknown): string | null {
	return stringField(value, 'type');
}

/** Return the action phase retained by an active or attention-required loan. */
export function loanPhase(loan: ResourceDetail['loan'] | ResourceOverviewItem['loan']): unknown {
	if (!loan) return null;
	const state = loan.state;
	const stateType = tagOf(state);
	if (stateType === 'active') return asRecord(state)?.phase ?? null;
	if (stateType === 'needs_attention') return asRecord(state)?.last_safe_phase ?? null;
	return null;
}

/** Current action identity, when the loan is waiting on a supervisor decision. */
export function loanActionId(loan: ResourceDetail['loan']): string | null {
	const state = loan?.state;
	return stringField(state, 'action_id') ?? stringField(loanPhase(loan), 'action_id');
}

/** Return context carried by serving, return-pending, or restoring loan phases. */
export function loanReturnContext(loan: ResourceDetail['loan']): unknown {
	return asRecord(loanPhase(loan))?.return_context ?? null;
}

/** Return saved release provenance for the serving phase, when present. */
export function loanReleaseProvenance(
	loan: ResourceDetail['loan'] | ResourceOverviewItem['loan']
): unknown {
	return asRecord(loanPhase(loan))?.release_provenance ?? null;
}

/** Request selected for the active serving phase. */
export function currentRequestId(loan: ResourceDetail['loan']): string | null {
	return stringField(loanPhase(loan), 'current_request_id');
}

/** Explain the resource's current derived state without treating missing data as idle. */
export function overviewStatus(
	item: ResourceOverviewItem,
	unavailableAuthorities: readonly Record<string, unknown>[]
): ResourceStatus {
	const associations = unavailableAuthorities.map((issue) =>
		authorityAssociation(item.resource, issue)
	);
	if (associations.some((association) => association === true)) {
		return {
			label: 'Authority unavailable',
			message: 'Resource state cannot be confirmed',
			tone: 'red'
		};
	}
	if (associations.some((association) => association === null)) {
		return {
			label: 'Unknown',
			message: 'An authority failure could not be mapped to this resource',
			tone: 'amber'
		};
	}
	return deriveStatus(
		item.resource,
		item.loan,
		item.queued_count,
		item.current_task,
		item.attention,
		item.background_launch
	);
}

/** Explain the resource's detail state from its loan, tasks, and queued requests. */
export function detailStatus(detail: ResourceDetail): ResourceStatus {
	const queued = detail.requests.filter((request) => tagOf(request.state) === 'queued').length;
	return deriveStatus(
		detail.resource,
		detail.loan,
		queued,
		detail.current_task ?? detail.background_task,
		detail.attention,
		detail.background_launch
	);
}

/** What holds one resource now and what waits for it. */
export interface ResourceQueue {
	resource: ResourceDetail['resource'];
	status: ResourceStatus;
	/** Task of the serving loan. */
	current: ResourceTaskSummary | null;
	/** Registered background task that holds the resource while no loan serves it. */
	background: ResourceTaskSummary | null;
	/** Queued requests in the server-provided serving order. */
	queue: ResourceDetail['requests'];
}

/** Reduce a resource detail to its holder and waiting queue. */
export function resourceQueue(detail: ResourceDetail): ResourceQueue {
	return {
		resource: detail.resource,
		status: detailStatus(detail),
		current: detail.current_task,
		background: detail.background_task,
		queue: detail.requests.filter((request) => tagOf(request.state) === 'queued')
	};
}

/** Return the placement for moving a queued request one place in serving order. */
export function queueMovePlacement(
	queue: readonly ResourceDetail['requests'][number][],
	index: number,
	direction: 'up' | 'down'
): QueuePlacement | null {
	if (!Number.isInteger(index) || index < 0 || index >= queue.length) return null;

	if (direction === 'up') {
		if (index === 0) return null;
		// the head request may leave the queue before the click lands
		if (index === 1) return { type: 'front' };
		const previous = queue[index - 1];
		return previous ? { type: 'before', request_id: previous.request_id } : null;
	}

	if (index === queue.length - 1) return null;
	// the tail request may leave the queue before the click lands
	if (index === queue.length - 2) return { type: 'back' };
	const next = queue[index + 1];
	return next ? { type: 'after', request_id: next.request_id } : null;
}

/** Whether a resource runs or waits on work, so it earns space next to the task list. */
export function isBusy(queue: ResourceQueue): boolean {
	return queue.current !== null || queue.background !== null || queue.queue.length > 0;
}

/** Explain why a first background launch still reserves the resource. */
export function backgroundLaunchText(
	launch: BackgroundLaunchReservation | undefined
): string | null {
	if (!launch) return null;
	switch (launch.status) {
		case 'queued':
			return 'The first background launch is queued';
		case 'started_unregistered':
			return 'The first background launch started and is not registered yet';
		case 'release_unproven':
			return 'The first background launch ended before registration. GPU release is not proven until an operator confirms that no trainer GPU work remains.';
		case 'identity_mismatch':
			return 'The first background launch records do not match its receipt';
		default:
			return `The first background launch reserves the GPU (${launch.status})`;
	}
}

/** Human-readable task lifecycle state, with unknown states kept explicit. */
export function taskStatusLabel(task: ResourceTaskSummary | null): string {
	if (!task) return 'Task state unavailable';
	if (['queued', 'running', 'succeeded', 'failed', 'cancelled', 'lost'].includes(task.status)) {
		return task.status;
	}
	return `Unknown task state: ${task.status}`;
}

function deriveStatus(
	resource: ResourceOverviewItem['resource'],
	loan: ResourceDetail['loan'] | ResourceOverviewItem['loan'],
	queuedCount: number,
	currentTask: ResourceTaskSummary | null,
	attention: ResourceOverviewItem['attention'],
	backgroundLaunch: BackgroundLaunchReservation | undefined
): ResourceStatus {
	const stateType = tagOf(loan?.state);
	if (attention?.code === 'background_launch_release_unproven') {
		return {
			label: 'Operator release required',
			message: attention.message,
			tone: 'amber'
		};
	}
	if (attention || stateType === 'needs_attention') {
		return {
			label: 'Attention',
			message: attention?.message ?? 'Supervisor action is required',
			tone: 'amber'
		};
	}
	if (loan) {
		if (stateType !== 'active') {
			return {
				label: 'Unknown',
				message: `Unknown loan state: ${stateType ?? 'missing'}`,
				tone: 'amber'
			};
		}
		const phaseType = tagOf(loanPhase(loan));
		if (phaseType === 'awaiting_return') {
			return {
				label: 'Return pending',
				message: 'The queue drained and return is reserved',
				tone: 'amber'
			};
		}
		if (phaseType === 'awaiting_release') {
			if (currentTask?.status === 'lost') {
				return {
					label: 'Operator release required',
					message:
						'The trainer was lost with no reusable checkpoint or result. GPU release is not proven until an operator confirms that no trainer GPU work remains.',
					tone: 'amber'
				};
			}
			return {
				label: 'Active / reserved',
				message: 'Waiting for training to release the GPU',
				tone: 'blue'
			};
		}
		if (phaseType === 'serving') {
			return {
				label: 'Active / reserved',
				message: 'An optimization request holds the GPU',
				tone: 'blue'
			};
		}
		if (phaseType === 'restoring') {
			return { label: 'Active / reserved', message: 'The return task is starting', tone: 'blue' };
		}
		return {
			label: 'Unknown',
			message: `Unknown loan phase: ${phaseType ?? 'missing'}`,
			tone: 'amber'
		};
	}
	if (currentTask) {
		if (currentTask.status === 'lost') {
			return {
				label: 'Lost trainer',
				message: 'No reusable checkpoint or result is recorded. GPU release is not confirmed.',
				tone: 'amber'
			};
		}
		if (currentTask.status === 'running' || currentTask.status === 'queued') {
			return {
				label: 'Active / reserved',
				message: `${currentTask.display_name} is ${currentTask.status}`,
				tone: 'blue'
			};
		}
		return {
			label: 'Unknown',
			message: `Task ended with ${taskStatusLabel(currentTask)}; resource state is not clear`,
			tone: 'amber'
		};
	}
	if (resource.registered_background_task !== null) {
		return {
			label: 'Unknown',
			message: 'The registered training task is not available',
			tone: 'amber'
		};
	}
	// an unregistered launch still holds the GPU, so it is never shown as available
	const launchText = backgroundLaunchText(backgroundLaunch);
	if (launchText !== null) {
		return backgroundLaunch?.status === 'release_unproven'
			? { label: 'Operator release required', message: launchText, tone: 'amber' }
			: { label: 'Active / reserved', message: launchText, tone: 'blue' };
	}
	if (queuedCount > 0) {
		return {
			label: 'Queued',
			message: `${queuedCount} request${queuedCount === 1 ? '' : 's'} waiting`,
			tone: 'neutral'
		};
	}
	return {
		label: 'Available',
		message: 'No loan, queued request, or background task is registered',
		tone: 'green'
	};
}

function firstString(value: Record<string, unknown>, keys: readonly string[]): string | null {
	for (const key of keys) {
		const candidate = value[key];
		if (typeof candidate === 'string') return candidate;
	}
	return null;
}

function authorityAssociation(
	resource: ResourceOverviewItem['resource'],
	issue: Record<string, unknown>
): boolean | null {
	const resourceId = firstString(issue, ['resource_id', 'resource']);
	const machine = firstString(issue, ['authority_machine', 'machine', 'machine_id']);
	if (resourceId === resource.id || machine === resource.authority_machine) return true;
	if (resourceId !== null || machine !== null) return false;
	return null;
}
