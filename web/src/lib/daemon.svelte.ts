// Polling stores for the dashboard. Both must be constructed during component
// initialisation: they own `$effect`s and runed utilities.

import { IsDocumentVisible, useInterval } from 'runed';
import { SvelteMap } from 'svelte/reactivity';
import {
	IN_FLIGHT_STATUSES,
	asApiError,
	fetchJob,
	fetchJobs,
	fetchLogTail,
	fetchFleetTasks,
	fetchResources,
	fetchStatus,
	fetchTask,
	isInFlight,
	type ApiError,
	type DaemonStatus,
	type FleetMachine,
	type FleetTask,
	type JobDetail,
	type JobRecord,
	type LogTail,
	type ResourceRecord,
	type TaskDetail,
	type TaskQuery
} from './api';
import { newOperationId } from './queue-view';

/** Every queued and running task, from every thread. */
const IN_FLIGHT_QUERY: TaskQuery = { statuses: IN_FLIGHT_STATUSES };

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
	/** Queued and running tasks on every machine that answered, whatever the filter. */
	inFlight = $state<readonly FleetTask[]>([]);
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
		// the default view is the in-flight list, so it needs no second fleet read
		const listsInFlight = queryKey(query) === queryKey(IN_FLIGHT_QUERY);
		try {
			const [status, fleet, inFlight] = await Promise.all([
				fetchStatus(),
				fetchFleetTasks(query),
				listsInFlight ? null : fetchFleetTasks(IN_FLIGHT_QUERY)
			]);
			if (generation !== this.#generation) return;
			this.status = status;
			this.machines = fleet.machines;
			this.tasks = fleet.tasks;
			this.inFlight = (inFlight ?? fleet).tasks;
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

/** This machine's GPU queue: its resources and every job that has not ended. */
export class QueueStore {
	/** By name. */
	resources = $state<readonly ResourceRecord[]>([]);
	/** Serving order. */
	jobs = $state<readonly JobRecord[]>([]);
	/**
	 * Names of ended jobs whose run still holds a resource while it cleans up or waits for a release.
	 * The queue list holds only jobs that have not ended, and a job's name never changes.
	 */
	endedJobNames = new SvelteMap<string, string>();
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#visible = new IsDocumentVisible();
	#generation = 0;

	constructor() {
		startPolling({
			key: () => '',
			active: () => this.#visible.current,
			run: () => void this.refresh()
		});
	}

	/** Fetch resources and jobs together so a run and its job agree. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		try {
			const [resources, jobs] = await Promise.all([fetchResources(), fetchJobs()]);
			if (generation !== this.#generation) return;
			this.resources = resources.resources;
			this.jobs = jobs.jobs;
			this.error = null;
			void this.#loadEndedJobNames(resources.machine);
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}

	/** Name of a job, from the queue or the ended-job lookup. */
	jobName(id: string): string | null {
		return this.jobs.find((job) => job.id === id)?.spec.name ?? this.endedJobNames.get(id) ?? null;
	}

	async #loadEndedJobNames(machine: string): Promise<void> {
		const missing = this.resources.flatMap((record) =>
			record.run && this.jobName(record.run.job) === null ? [record.run.job] : []
		);
		if (missing.length === 0) return;
		const found = await Promise.allSettled(missing.map((id) => fetchJob(id, machine)));
		for (const result of found) {
			if (result.status === 'fulfilled') {
				this.endedJobNames.set(result.value.job.id, result.value.job.spec.name);
			}
		}
	}
}

/** Job page: one job with every run, and the resources of the machine that runs it. */
export class JobStore {
	/** Last successful `GET /v1/resource/jobs/{job}`. */
	detail = $state<JobDetail | null>(null);
	/** Resources of the job's machine, for their names. */
	resources = $state<readonly ResourceRecord[]>([]);
	/** Machine that serves this dashboard. */
	localMachine = $state<string | null>(null);
	/** Error from the last attempt, cleared by the next success. */
	error = $state<ApiError | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#id: () => string;
	#visible = new IsDocumentVisible();
	#generation = 0;
	#loadedId = '';
	/** An ended job whose last cleanup finished cannot change again, so the timer stops. */
	#pollable = $derived(
		this.detail === null ||
			this.detail.job.position !== null ||
			this.detail.active_run !== null ||
			this.detail.runs.some((run) => run.cleanup === null)
	);

	constructor(id: () => string) {
		this.#id = id;
		startPolling({
			key: id,
			active: () => this.#visible.current && this.#pollable,
			run: () => void this.refresh()
		});
	}

	/** Whether the job's machine serves this dashboard, so run tasks have local pages. */
	get local(): boolean {
		return this.detail === null || this.detail.job.machine === this.localMachine;
	}

	/** Fetch the job, then its machine's resources. */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		const id = this.#id();
		if (this.#loadedId !== id) {
			this.detail = null;
			this.resources = [];
		}
		try {
			const [detail, local] = await Promise.all([fetchJob(id), fetchResources()]);
			const remote = detail.job.machine !== local.machine;
			const resources = remote ? await fetchResources(detail.job.machine) : local;
			if (generation !== this.#generation) return;
			this.detail = detail;
			this.resources = resources.resources;
			this.localMachine = local.machine;
			this.error = null;
			this.#loadedId = id;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}
}

/** Transport attempts for one queue operation, all with the same operation ID. */
const OPERATION_ATTEMPTS = 3;

/** Sends one queue control at a time and keeps the daemon's last refusal on screen. */
export class QueueControl {
	/** Key of the action in flight, such as a job or attention ID. */
	pending = $state<string | null>(null);
	/** The daemon's refusal or a transport failure from the last action. */
	error = $state<ApiError | null>(null);

	#refresh: () => Promise<void>;

	constructor(refresh: () => Promise<void>) {
		this.#refresh = refresh;
	}

	/**
	 * Send one operation under a fresh operation ID. A request that never reached the daemon is
	 * retried with the same ID, so the daemon applies it at most once.
	 */
	async run(key: string, send: (operationId: string) => Promise<unknown>): Promise<boolean> {
		if (this.pending !== null) return false;
		this.pending = key;
		this.error = null;
		const operationId = newOperationId();
		let succeeded = false;
		for (let attempt = 1; attempt <= OPERATION_ATTEMPTS; attempt += 1) {
			try {
				await send(operationId);
				succeeded = true;
				break;
			} catch (cause) {
				const error = asApiError(cause);
				this.error = error;
				if (error.reachable) break;
			}
		}
		if (succeeded) this.error = null;
		this.pending = null;
		await this.#refresh();
		return succeeded;
	}

	/** Clear the last error once the reader dismisses it. */
	dismiss(): void {
		this.error = null;
	}
}

function queryKey(query: TaskQuery): string {
	return `${query.statuses?.join(',') ?? ''}|${query.thread ?? ''}`;
}
