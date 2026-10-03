// Pure shaping of the GPU queue routes for the dashboard. Type-only imports keep this module
// loadable by `node --test` without the SvelteKit resolver

import type {
	CleanupFailure,
	CleanupResult,
	JobRecord,
	JobTarget,
	Placement,
	Preemption,
	Priority,
	ResourceRecord,
	RunPhase,
	StepView,
	StopCause
} from './api';

/** Every level, in serving order: a higher level always runs first. */
export const PRIORITIES: readonly Priority[] = ['high', 'medium', 'low'];

/** A resource is idle without a run, otherwise in its run's phase. */
export type ResourceState = 'idle' | RunPhase['phase'];

/** One level of the machine queue and its jobs, in serving order. */
export interface LevelGroup {
	priority: Priority;
	jobs: readonly JobRecord[];
}

/** Direction of a one-step move in the serving order. */
export type Nudge = 'up' | 'down';

/** Placeholder for a value that is not known yet. */
const EM_DASH = '—';

/**
 * Every level in serving order, each with its jobs by position. Empty levels stay so a job can
 * move into them.
 */
export function groupByLevel(jobs: readonly JobRecord[]): LevelGroup[] {
	return PRIORITIES.map((priority) => ({
		priority,
		jobs: jobs
			.filter((job) => job.priority === priority)
			.toSorted((a, b) => (a.position ?? Infinity) - (b.position ?? Infinity))
	}));
}

/** Jobs still waiting for a resource, in serving order. */
export function waitingJobs(jobs: readonly JobRecord[]): JobRecord[] {
	return groupByLevel(jobs).flatMap((group) =>
		group.jobs.filter((job) => job.state.state === 'queued')
	);
}

/** State of one resource. */
export function resourceState(record: ResourceRecord): ResourceState {
	return record.run?.phase.phase ?? 'idle';
}

/** Whether the queue has anything to show: a held resource or a job that has not ended. */
export function queueHasWork(
	resources: readonly ResourceRecord[],
	jobs: readonly JobRecord[]
): boolean {
	return resources.some((record) => record.run !== null) || jobs.length > 0;
}

/** Start of the span a phase has lasted, for an elapsed timer; null when it has none. */
export function phaseSince(phase: RunPhase): string | null {
	switch (phase.phase) {
		case 'launching':
			return phase.reserved_at;
		case 'executing':
			return phase.started_at;
		case 'stopping':
			return phase.requested_at;
		case 'cleaning':
		case 'attention':
			return null;
	}
}

/** Short word for a run phase, naming the stop cause while a run stops. */
export function phaseLabel(phase: RunPhase): string {
	if (phase.phase === 'stopping') return `stopping · ${stopCauseLabel(phase.cause)}`;
	if (phase.phase === 'cleaning' && phase.attempt > 1) return `cleaning · try ${phase.attempt}`;
	return phase.phase;
}

/** Short word for a stop cause. */
export function stopCauseLabel(cause: StopCause): string {
	switch (cause) {
		case 'yield':
			return 'yield';
		case 'restart':
			return 'restart';
		case 'user_cancel':
			return 'cancel';
	}
}

/** Preemption mode and restart window, such as `yield · restart 5m`. */
export function preemptionLabel(preempt: Preemption): string {
	if (preempt.mode === 'restart' || !preempt.restart_within) return preempt.mode;
	return `${preempt.mode} · restart ${preempt.restart_within}`;
}

/** Resource name of a target, `any` for an unpinned job. */
export function targetLabel(target: JobTarget, names: ReadonlyMap<string, string>): string {
	if (target.type === 'any') return 'any';
	return names.get(target.resource) ?? target.resource.slice(0, 8);
}

/** Resource names by ID. */
export function resourceNames(resources: readonly ResourceRecord[]): Map<string, string> {
	return new Map(resources.map((record) => [record.resource.id, record.resource.name]));
}

/** Step progress: the step running or next to run, of all steps. */
export function stepLabel(job: JobRecord): string {
	const total = job.spec.steps.length;
	const current = job.state.state === 'succeeded' ? total : Math.min(job.step + 1, total);
	return `step ${current}/${total}`;
}

/** Where one step stands in its job. */
export type StepProgress = 'done' | 'current' | 'failed' | 'pending' | 'skipped';

/**
 * Progress of the step at `index`. A failed job stops at the step of its failed run, and a cancelled
 * job never runs the steps it had not reached.
 */
export function stepProgress(
	job: JobRecord,
	index: number,
	failedStep: number | null
): StepProgress {
	switch (job.state.state) {
		case 'succeeded':
			return 'done';
		case 'failed':
			if (index === failedStep) return 'failed';
			return failedStep !== null && index < failedStep ? 'done' : 'skipped';
		case 'cancelled':
			return index < job.step ? 'done' : 'skipped';
		case 'queued':
		case 'active':
			if (index < job.step) return 'done';
			return index === job.step ? 'current' : 'pending';
	}
}

/** One line for a step: the command, or the container image. */
export function stepText(step: StepView): string {
	return step.type === 'task' ? step.command.join(' ') : `container ${step.image}`;
}

/** Cleanup after one run: pending, clean, or why it could not finish. */
export function cleanupLabel(result: CleanupResult | null): string {
	if (result === null) return EM_DASH;
	return 'Ok' in result ? 'clean' : cleanupFailureText(result.Err);
}

/** One line per cleanup failure, matching the daemon's own wording. */
export function cleanupFailureText(failure: CleanupFailure): string {
	switch (failure.kind) {
		case 'process_group_unconfirmed':
			return 'process group exit not confirmed';
		case 'container_unconfirmed':
			return 'container exit and removal not confirmed';
		case 'processes':
			return processFailureText(failure.failure);
	}
}

function processFailureText(failure: { kind: string; [key: string]: unknown }): string {
	const pgid = typeof failure.pgid === 'number' ? failure.pgid : null;
	const count = (value: unknown) => (Array.isArray(value) ? value.length : 0);
	switch (failure.kind) {
		case 'enumeration_failed':
			return `could not list processes: ${String(failure.message ?? '')}`.trim();
		case 'targets_survived':
			return `${count(failure.survivors)} marked processes survived SIGKILL`;
		case 'protected_carries_marker':
			return `${count(failure.pids)} protected processes carry the run marker`;
		case 'protected_in_group':
			return `process group ${pgid ?? '?'} holds protected processes`;
		case 'group_unattributed':
			return `process group ${pgid ?? '?'} is not tied to the run`;
		case 'group_survived':
			return `process group ${pgid ?? '?'} survived SIGKILL`;
		default:
			return failure.kind.replaceAll('_', ' ');
	}
}

/**
 * Placement that moves a job one place in the serving order. Inside a level it swaps with its
 * neighbour; at the edge of a level it joins the near end of the next level. Null when the job is
 * already first or last in the whole queue, or not listed.
 */
export function nudgePlacement(
	groups: readonly LevelGroup[],
	jobId: string,
	direction: Nudge
): Placement | null {
	const levelIndex = groups.findIndex((group) => group.jobs.some((job) => job.id === jobId));
	if (levelIndex < 0) return null;
	const { jobs } = groups[levelIndex];
	const index = jobs.findIndex((job) => job.id === jobId);
	const step = direction === 'up' ? -1 : 1;
	const neighbour = jobs[index + step];
	if (neighbour) {
		return {
			type: 'relative',
			target: neighbour.id,
			side: direction === 'up' ? 'before' : 'after',
			expect: neighbour.priority
		};
	}
	const nextLevel = groups[levelIndex + step];
	if (!nextLevel) return null;
	return {
		type: 'edge',
		priority: nextLevel.priority,
		end: direction === 'up' ? 'back' : 'front'
	};
}

/**
 * Placement for dropping a dragged job beside another, taking that job's level. Null when it would
 * not move.
 */
export function dropPlacement(
	draggedId: string,
	target: JobRecord,
	side: 'before' | 'after'
): Placement | null {
	if (draggedId === target.id) return null;
	return { type: 'relative', target: target.id, side, expect: target.priority };
}

/**
 * Random version 4 UUID for one queue operation. `crypto.randomUUID` needs a secure context, and the
 * dashboard is often opened over plain http on the LAN, so this uses `getRandomValues`.
 */
export function newOperationId(
	fill: (bytes: Uint8Array<ArrayBuffer>) => void = (bytes) => crypto.getRandomValues(bytes)
): string {
	const bytes = new Uint8Array(16);
	fill(bytes);
	bytes[6] = (bytes[6] & 0x0f) | 0x40;
	bytes[8] = (bytes[8] & 0x3f) | 0x80;
	const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
