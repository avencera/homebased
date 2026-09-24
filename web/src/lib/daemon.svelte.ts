// Polling stores for the dashboard. Both must be constructed during component
// initialisation: they own `$effect`s and runed utilities.

import { IsDocumentVisible, useInterval } from 'runed';
import {
	asApiError,
	fetchLogTail,
	fetchFleetTasks,
	fetchStatus,
	fetchTask,
	isInFlight,
	type ApiError,
	type DaemonStatus,
	type FleetMachine,
	type FleetTask,
	type LogTail,
	type TaskDetail,
	type TaskQuery
} from './api';
import { resourceQueue, type ResourceQueue } from './resource-state';
import {
	fetchPendingActions,
	fetchResourceDetail,
	fetchResourceOverview,
	type PendingAction,
	type PendingActionResult,
	type ResourceDetail,
	type ResourceOverview
} from './resources';

/** Refresh cadence while the tab is visible. */
export const POLL_INTERVAL_MS = 2000;

/** Lines of `output.log` the detail page keeps on screen. */
export const LOG_TAIL_LINES = 200;

interface PollingOptions {
	/** Refetches immediately whenever this value changes. */
	key: () => string;
	/** Whether the timer should keep running. */
	active: () => boolean;
	/** One refresh. Must not read state it writes, or the key effect loops. */
	run: () => void;
}

function startPolling(options: PollingOptions): void {
	const poll = useInterval(() => POLL_INTERVAL_MS, {
		immediate: false,
		// so a tab that regains focus shows fresh data without waiting a tick
		immediateCallback: true,
		callback: options.run
	});
	let started = false;
	$effect(() => {
		options.key();
		// the first fetch comes from the initial `resume` below
		if (started) options.run();
		started = true;
	});
	$effect(() => {
		if (options.active()) poll.resume();
		else poll.pause();
	});
}

/** Dashboard list: daemon status plus the filtered fleet task table. */
export class DaemonStore {
	/** Last successful `GET /v1/status`. */
	status = $state<DaemonStatus | null>(null);
	/** This machine first, then every known peer and whether it answered. */
	machines = $state<readonly FleetMachine[]>([]);
	/** Filtered tasks from every machine that answered, newest first. */
	tasks = $state<readonly FleetTask[]>([]);
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#query: () => TaskQuery;
	#visible = new IsDocumentVisible();
	#generation = 0;

	constructor(query: () => TaskQuery = () => ({})) {
		this.#query = query;
		startPolling({
			key: () => queryKey(query()),
			active: () => this.#visible.current,
			run: () => void this.refresh()
		});
	}

	/** Whether the daemon answered the last request at all. */
	get online(): boolean {
		return this.error === null ? this.status !== null : this.error.reachable;
	}

	/** Fetch status and tasks together so the header and table agree. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		const query = this.#query();
		try {
			const [status, fleet] = await Promise.all([fetchStatus(), fetchFleetTasks(query)]);
			if (generation !== this.#generation) return;
			this.status = status;
			this.machines = fleet.machines;
			this.tasks = fleet.tasks;
			this.error = null;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}
}

/** Detail page: one task plus the tail of its output log. */
export class TaskStore {
	/** Last successful `GET /v1/tasks/{id}`. */
	task = $state<TaskDetail | null>(null);
	/** Last successful log tail. */
	logTail = $state<LogTail | null>(null);
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#id: () => string;
	#visible = new IsDocumentVisible();
	#generation = 0;
	/** Plain field: reading it must not make the fetch depend on its own result. */
	#loadedId = '';
	/** A terminal task cannot change again, so the timer stops. */
	#pollable = $derived(this.task === null || isInFlight(this.task.status));

	constructor(id: () => string) {
		this.#id = id;
		startPolling({
			key: id,
			active: () => this.#visible.current && this.#pollable,
			run: () => void this.refresh()
		});
	}

	/** Whether the daemon answered the last request at all. */
	get online(): boolean {
		return this.error === null ? this.task !== null : this.error.reachable;
	}

	/** Fetch the task and its log tail together. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		const id = this.#id();
		if (this.#loadedId !== id) {
			this.task = null;
			this.logTail = null;
		}
		try {
			const [task, logTail] = await Promise.all([fetchTask(id), fetchLogTail(id, LOG_TAIL_LINES)]);
			if (generation !== this.#generation) return;
			this.task = task;
			this.logTail = logTail;
			this.error = null;
			this.#loadedId = id;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}
}

/** Resource overview and authority availability. */
export class ResourceOverviewStore {
	/** Last validated resource list. */
	overview = $state<ResourceOverview | null>(null);
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#visible = new IsDocumentVisible();
	#generation = 0;

	constructor() {
		startPolling({
			key: () => 'resources',
			active: () => this.#visible.current,
			run: () => void this.refresh()
		});
	}

	/** Fetch the resource list and authority state. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		try {
			const overview = await fetchResourceOverview();
			if (generation !== this.#generation) return;
			this.overview = overview;
			this.error = null;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}
}

/** Holder and queue of every resource, for the task list's side panel. */
export class ResourceQueueStore {
	/** Resources in overview order. */
	queues = $state<readonly ResourceQueue[]>([]);
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);

	#visible = new IsDocumentVisible();
	#generation = 0;

	constructor() {
		startPolling({
			key: () => 'resource-queues',
			active: () => this.#visible.current,
			run: () => void this.refresh()
		});
	}

	/** Fetch the overview, then each detail, because only detail carries the queue. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		try {
			const overview = await fetchResourceOverview();
			const details = await Promise.all(
				overview.resources.map((item) => fetchResourceDetail(item.resource.id))
			);
			if (generation !== this.#generation) return;
			this.queues = details.map(resourceQueue);
			this.error = null;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
	}
}

/** One resource detail, including actions assigned to its exact supervisor. */
export class ResourceDetailStore {
	/** Last validated detail from the fixed resource authority. */
	detail = $state<ResourceDetail | null>(null);
	/** Pending actions for the assigned supervisor, or null until first success. */
	pendingActions = $state<PendingAction[] | null>(null);
	/** Authorities that could not answer the supervisor action query. */
	pendingUnavailableAuthorities = $state<readonly Record<string, unknown>[] | null>(null);
	/** Error from the resource detail request. */
	error = $state<ApiError | null>(null);
	/** Error from the pending-action query. */
	pendingError = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled detail attempt. */
	lastFetched = $state<number | null>(null);

	#id: () => string;
	#visible = new IsDocumentVisible();
	#generation = 0;
	#pendingGeneration = 0;
	#loadedId = '';

	constructor(id: () => string) {
		this.#id = id;
		startPolling({
			key: id,
			active: () => this.#visible.current,
			run: () => void this.refresh()
		});
	}

	/** Fetch detail, then refresh the supervisor's pending-action projection. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		const id = this.#id();
		if (this.#loadedId !== id) {
			this.detail = null;
			this.pendingActions = null;
			this.pendingUnavailableAuthorities = null;
		}

		try {
			const detail = await fetchResourceDetail(id);
			if (generation !== this.#generation) return;
			this.detail = detail;
			this.error = null;
			this.#loadedId = id;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
			this.lastFetched = Date.now();
			return;
		}

		await this.refreshPendingActions(generation);
		if (generation === this.#generation) this.lastFetched = Date.now();
	}

	/** Keep the exact detail returned by an acknowledged mutation on screen. */
	showAuthoritative(detail: ResourceDetail): void {
		if (detail.resource.id === this.#id()) {
			this.#generation += 1;
			this.#pendingGeneration += 1;
			this.detail = detail;
			this.pendingActions = null;
			this.pendingUnavailableAuthorities = null;
			this.pendingError = null;
			this.error = null;
			this.#loadedId = detail.resource.id;
			this.lastFetched = Date.now();
		}
	}

	/** Refresh the pending-action query without replacing resource detail. */
	async refreshPending(): Promise<void> {
		await this.refreshPendingActions(this.#generation);
	}

	async refreshPendingActions(generation = this.#generation): Promise<void> {
		const detail = this.detail;
		if (!detail) return;
		const pendingGeneration = ++this.#pendingGeneration;
		try {
			const result: PendingActionResult = await fetchPendingActions(
				detail.resource.supervisor.machine,
				detail.resource.supervisor.thread
			);
			if (generation !== this.#generation || pendingGeneration !== this.#pendingGeneration) return;
			this.pendingActions = result.actions.filter(
				(action) => action.resource_id === detail.resource.id
			);
			this.pendingUnavailableAuthorities = result.unavailable_authorities ?? [];
			this.pendingError = null;
		} catch (cause) {
			if (generation !== this.#generation || pendingGeneration !== this.#pendingGeneration) return;
			this.pendingError = asApiError(cause);
			this.pendingUnavailableAuthorities = null;
		}
	}
}

function queryKey(query: TaskQuery): string {
	return `${query.statuses?.join(',') ?? ''}|${query.thread ?? ''}`;
}
