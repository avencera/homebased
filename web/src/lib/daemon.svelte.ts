// Polling stores for the dashboard. Both must be constructed during component
// initialisation: they own `$effect`s and runed utilities.

import { IsDocumentVisible, useInterval } from 'runed';
import {
	asApiError,
	fetchLogTail,
	fetchStatus,
	fetchTask,
	fetchTasks,
	isInFlight,
	type ApiError,
	type DaemonStatus,
	type LogTail,
	type TaskDetail,
	type TaskQuery,
	type TaskSummary
} from './api';

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

/** Dashboard list: daemon status plus the filtered task table. */
export class DaemonStore {
	/** Last successful `GET /v1/status`. */
	status = $state<DaemonStatus | null>(null);
	/** Filtered tasks, newest first. */
	tasks = $state<TaskSummary[]>([]);
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
			const [status, tasks] = await Promise.all([fetchStatus(), fetchTasks(query)]);
			if (generation !== this.#generation) return;
			this.status = status;
			this.tasks = tasks;
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

function queryKey(query: TaskQuery): string {
	return `${query.statuses?.join(',') ?? ''}|${query.thread ?? ''}`;
}
