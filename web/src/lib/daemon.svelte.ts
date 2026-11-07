// Polling stores for the dashboard. Both must be constructed during component
// initialisation: they own `$effect`s and runed utilities.

import { untrack } from 'svelte';
import { IsDocumentVisible, useInterval } from 'runed';
import { SvelteMap } from 'svelte/reactivity';
import {
	IN_FLIGHT_STATUSES,
	asApiError,
	fetchFleetQueues,
	fetchJob,
	fetchLogTail,
	fetchFleetTasks,
	fetchResources,
	fetchRoster,
	fetchStatus,
	fetchTask,
	isInFlight,
	isRejectedRoute,
	isTaskDetail,
	type ApiError,
	type DaemonStatus,
	type FleetMachine,
	type FleetTask,
	type JobDetail,
	type LogTail,
	type MachineQueue,
	type MachineRead,
	type ResourceRecord,
	type TaskQuery,
	type TaskRecord,
	type ViewMachine
} from './api';
import { mergeFleetTasks, type MachineTasks } from './fleet-merge';
import { newOperationId } from './queue-view';

/** Whether the task has launched, so it has an output log to read. */
function hasLog(record: TaskRecord): boolean {
	return isTaskDetail(record) && record.output_log !== undefined;
}

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
	/** One refresh, run untracked. */
	run: () => Promise<void>;
}

function startPolling(options: PollingOptions): void {
	let running: Promise<void> | null = null;
	const start = () => {
		// a run started from an effect must not subscribe it to what the run reads or writes
		const run = untrack(options.run);
		running = run;
		void run.finally(() => {
			if (running === run) running = null;
		});
	};
	const poll = useInterval(() => POLL_INTERVAL_MS, {
		immediate: false,
		// so a tab that regains focus shows fresh data without waiting a tick
		immediateCallback: true,
		// a tick during a slow read waits for it; starting another would drop its answer
		callback: () => {
			if (running === null) start();
		}
	});
	let started = false;
	$effect(() => {
		options.key();
		// the first fetch comes from the initial `resume` below; a new key reads at once
		if (started) start();
		started = true;
	});
	$effect(() => {
		if (options.active()) poll.resume();
		else poll.pause();
	});
}

/** One machine's settled read: how it went, and its answer when it answered. */
interface MachineResult<T> {
	read: MachineRead;
	answer: T | null;
}

/**
 * Per-machine reads of one fleet-wide view.
 *
 * Each machine is read with its own request and lands when it answers, so a slow peer holds only
 * its own rows. A machine keeps at most one read in flight: a refresh while one is out skips that
 * machine instead of starting a second read that would race the first.
 */
class FleetReads<T> {
	/** Machines of the last refresh, this machine first. */
	roster = $state<readonly ViewMachine[]>([]);
	#answers = new SvelteMap<string, MachineResult<T>>();
	/** Whether a roster has arrived, so an empty roster is not mistaken for every machine settled. */
	#rostered = $state(false);
	/** Generation each in-flight read started in, by machine. */
	#pending = new Map<string, number>();
	#generation = 0;

	/** Every roster machine with how its read stands, `loading` until its first answer. */
	machines = $derived<readonly FleetMachine[]>(
		this.roster.map((machine) => ({
			...machine,
			read: this.#answers.get(machine.machine)?.read ?? { state: 'loading' }
		}))
	);

	/** Answers of the machines that answered, in roster order. */
	answers = $derived<readonly T[]>(
		this.roster.flatMap(({ machine }) => {
			const answer = this.#answers.get(machine)?.answer;
			return answer === null || answer === undefined ? [] : [answer];
		})
	);

	/** Whether every roster machine has settled a read since the last reset. */
	settled = $derived(
		this.#rostered && this.roster.every(({ machine }) => this.#answers.has(machine))
	);

	/** Forget every answer, for a query whose old answers no longer apply. */
	reset(): void {
		this.#generation += 1;
		this.#answers.clear();
		this.#pending.clear();
	}

	/** Start `read` for each machine of `roster` that has no read in flight. */
	refresh(
		roster: readonly ViewMachine[],
		read: (machine: string) => Promise<MachineResult<T>>
	): void {
		this.roster = roster;
		this.#rostered = true;
		for (const machine of [...this.#answers.keys()]) {
			if (!roster.some((entry) => entry.machine === machine)) this.#answers.delete(machine);
		}
		const generation = this.#generation;
		for (const { machine } of roster) {
			if (this.#pending.has(machine)) continue;
			this.#pending.set(machine, generation);
			void read(machine)
				// a failed request marks only this machine: the browser gives up on a peer that does not
				// answer before the daemon does, and that must not read as this daemon being down
				.catch((cause: unknown): MachineResult<T> => {
					const error = asApiError(cause);
					const message = error.reachable ? error.message : 'no answer in time';
					return { read: { state: 'unavailable', message }, answer: null };
				})
				.then((settled) => {
					if (this.#pending.get(machine) === generation) this.#pending.delete(machine);
					if (generation === this.#generation) this.#answers.set(machine, settled);
				});
		}
	}
}

/** How the daemon reports `machine` in a one-machine view, or unavailable when it left it out. */
function readOf(
	machines: readonly { machine: string; read: MachineRead }[],
	machine: string
): MachineRead {
	return (
		machines.find((entry) => entry.machine === machine)?.read ?? {
			state: 'unavailable',
			message: 'the daemon did not list this machine'
		}
	);
}

/** One machine's task read, tagged with the `limit` it was read with. */
interface TaskPage extends MachineTasks {
	limit: number | undefined;
}

/** Read one machine's tasks through the serving daemon. */
async function readTasks(machine: string, query: TaskQuery): Promise<MachineResult<TaskPage>> {
	const list = await fetchFleetTasks(machine, query);
	const read = readOf(list.machines, machine);
	const answer =
		read.state === 'online'
			? { machine, tasks: list.tasks, hasMore: list.has_more, limit: query.limit }
			: null;
	return { read, answer };
}

/** Dashboard list: daemon status plus the filtered fleet task table. */
export class DaemonStore {
	/** Last successful `GET /v1/status`. */
	status = $state<DaemonStatus | null>(null);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#query: () => TaskQuery;
	#visible = new IsDocumentVisible();
	#generation = 0;
	#statusError = $state<ApiError | null>(null);
	#rosterError = $state<ApiError | null>(null);
	#tasks = new FleetReads<TaskPage>();
	#inFlight = new FleetReads<TaskPage>();
	/** Status and thread filter the task answers were read with; the limit only pages. */
	#filter = '';
	#limit = $state<number | undefined>(undefined);
	#listsInFlight = $state(true);
	#merged = $derived(mergeFleetTasks(this.#tasks.answers, this.#limit));

	/** This machine first, then every known peer and how its read stands. */
	machines = $derived.by(() => this.#tasks.machines);
	/** Filtered tasks from every machine that answered, newest first. */
	tasks = $derived.by((): readonly FleetTask[] => this.#merged.tasks);
	/** Whether the query's `limit` left out older tasks. */
	hasMore = $derived.by(() => this.#merged.hasMore);
	/** Whether every machine has answered the current filter. */
	settled = $derived.by(() => this.#tasks.settled);
	/**
	 * The `limit` every machine's tasks were read with, so a reader asks for more only once it
	 * lands. Undefined while any machine is still reading.
	 */
	loadedLimit = $derived.by(() =>
		this.#tasks.settled && this.#tasks.answers.every((answer) => answer.limit === this.#limit)
			? this.#limit
			: undefined
	);
	/** Queued and running tasks on every machine that answered, whatever the filter. */
	inFlight = $derived.by((): readonly FleetTask[] =>
		this.#listsInFlight ? this.#merged.tasks : mergeFleetTasks(this.#inFlight.answers).tasks
	);
	/** This daemon's error from the last attempt, cleared by the next success. */
	error = $derived.by(() => this.#statusError ?? this.#rosterError);

	constructor(query: () => TaskQuery = () => ({})) {
		this.#query = query;
		startPolling({
			key: () => queryKey(query()),
			active: () => this.#visible.current,
			run: () => this.refresh()
		});
	}

	/** Whether the daemon answered the last request at all. */
	get online(): boolean {
		return this.error === null ? this.status !== null : this.error.reachable;
	}

	/**
	 * Read status, then each machine's tasks on its own. Resolves once status and the roster
	 * settle; each machine's tasks land when that machine answers.
	 */
	async refresh(): Promise<void> {
		const generation = ++this.#generation;
		const query = this.#query();
		const filter = filterKey(query);
		if (filter !== this.#filter) {
			this.#filter = filter;
			this.#tasks.reset();
		}
		this.#limit = query.limit;
		// the default view is the in-flight list, so it needs no second fleet read
		const listsInFlight = queryKey(query) === queryKey(IN_FLIGHT_QUERY);
		// answers from before the default view took over are stale once the second read resumes
		if (listsInFlight && !this.#listsInFlight) this.#inFlight.reset();
		this.#listsInFlight = listsInFlight;

		const status = fetchStatus().then(
			(status) => {
				this.status = status;
				this.#statusError = null;
			},
			(cause: unknown) => {
				this.#statusError = asApiError(cause);
			}
		);
		const reads = fetchRoster().then(
			(roster) => {
				// a newer refresh with another query owns the reads now
				if (generation !== this.#generation) return;
				this.#rosterError = null;
				this.#tasks.refresh(roster.machines, (machine) => readTasks(machine, query));
				if (!listsInFlight) {
					this.#inFlight.refresh(roster.machines, (machine) => readTasks(machine, IN_FLIGHT_QUERY));
				}
			},
			(cause: unknown) => {
				if (generation === this.#generation) this.#rosterError = asApiError(cause);
			}
		);
		await Promise.all([status, reads]);
		this.lastFetched = Date.now();
	}
}

/** Detail page: one task plus the tail of its output log. */
export class TaskStore {
	/** Last successful `GET /v1/tasks/{id}`. */
	task = $state<TaskRecord | null>(null);
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
	#pollable = $derived(
		this.task === null ||
			(isTaskDetail(this.task)
				? isInFlight(this.task.status)
				: !isRejectedRoute(this.task) &&
					(this.task.status === null || isInFlight(this.task.status)))
	);

	constructor(id: () => string) {
		this.#id = id;
		startPolling({
			key: id,
			active: () => this.#visible.current && this.#pollable,
			run: () => this.refresh()
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
			// both start together unless the last read showed the task not launched,
			// since a log read for a held task can wait out an asleep executor
			const known = this.#loadedId === id ? this.task : null;
			const log =
				known === null || hasLog(known)
					? fetchLogTail(id, LOG_TAIL_LINES).then(
							(tail) => ({ ok: true as const, tail }),
							(cause: unknown) => ({ ok: false as const, cause })
						)
					: null;
			const task = await fetchTask(id);
			const launched = hasLog(task);
			// a task that has not launched has no log to wait for
			const result = launched && log ? await log : { ok: false as const, cause: null };
			// the log request can lose a race with the launch; the next poll reads it
			if (launched && log && !result.ok && asApiError(result.cause).code !== 'task_not_started') {
				throw result.cause;
			}
			if (generation !== this.#generation) return;
			this.task = task;
			this.logTail = result.ok ? result.tail : null;
			this.error = null;
			this.#loadedId = id;
		} catch (cause) {
			if (generation !== this.#generation) return;
			this.error = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}
}

/** Queue page and panel: every machine's resources and queue. */
export class QueueStore {
	/** This machine first, then every known peer and how its read stands. */
	machines = $derived.by(() => this.#reads.machines);
	/** One per machine that answered, in `machines` order. */
	queues = $derived.by((): readonly MachineQueue[] => this.#reads.answers);
	/** Every machine's resources. */
	resources = $derived(this.queues.flatMap((queue) => queue.resources));
	/** Every machine's jobs, each machine's in serving order. */
	jobs = $derived(this.queues.flatMap((queue) => queue.jobs));
	/**
	 * Names of ended jobs whose run still holds a resource while it cleans up or waits for a release.
	 * The queue list holds only jobs that have not ended, and a job's name never changes.
	 */
	endedJobNames = new SvelteMap<string, string>();
	/** Error from the last attempt, cleared by the next success. */
	error = $derived.by(() => this.#rosterError);
	/** Epoch milliseconds of the last settled attempt. */
	lastFetched = $state<number | null>(null);

	#visible = new IsDocumentVisible();
	#reads = new FleetReads<MachineQueue>();
	#rosterError = $state<ApiError | null>(null);

	constructor() {
		startPolling({
			key: () => '',
			active: () => this.#visible.current,
			run: () => this.refresh()
		});
	}

	/** Read the roster, then each machine's queue on its own, which lands when it answers. */
	async refresh(): Promise<void> {
		try {
			const roster = await fetchRoster();
			this.#rosterError = null;
			this.#reads.refresh(roster.machines, async (machine) => {
				const fleet = await fetchFleetQueues(machine);
				const read = readOf(fleet.machines, machine);
				const answer = fleet.queues.find((queue) => queue.machine === machine) ?? null;
				return { read, answer };
			});
			void this.#loadEndedJobNames();
		} catch (cause) {
			this.#rosterError = asApiError(cause);
		}
		this.lastFetched = Date.now();
	}

	/** Name of a job, from the queue or the ended-job lookup. */
	jobName(id: string): string | null {
		return this.jobs.find((job) => job.id === id)?.spec.name ?? this.endedJobNames.get(id) ?? null;
	}

	async #loadEndedJobNames(): Promise<void> {
		const missing = this.resources.flatMap((record) =>
			record.run && this.jobName(record.run.job) === null
				? [{ job: record.run.job, machine: record.machine }]
				: []
		);
		if (missing.length === 0) return;
		const found = await Promise.allSettled(
			missing.map(({ job, machine }) => fetchJob(job, machine))
		);
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
	#machine: () => string | null;
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

	/** `machine` names the job's queue when the job page link knows it; null uses the saved route. */
	constructor(id: () => string, machine: () => string | null = () => null) {
		this.#id = id;
		this.#machine = machine;
		startPolling({
			key: () => `${id()}|${machine() ?? ''}`,
			active: () => this.#visible.current && this.#pollable,
			run: () => this.refresh()
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
		const machine = this.#machine();
		const key = `${id}|${machine ?? ''}`;
		if (this.#loadedId !== key) {
			this.detail = null;
			this.resources = [];
		}
		try {
			const [detail, local] = await Promise.all([fetchJob(id, machine), fetchResources()]);
			const remote = detail.job.machine !== local.machine;
			const resources = remote ? await fetchResources(detail.job.machine) : local;
			if (generation !== this.#generation) return;
			this.detail = detail;
			this.resources = resources.resources;
			this.localMachine = local.machine;
			this.error = null;
			this.#loadedId = key;
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
	return `${filterKey(query)}|${query.limit ?? ''}`;
}

/** The part of a query that decides which tasks match, without the page size. */
function filterKey(query: TaskQuery): string {
	return `${query.statuses?.join(',') ?? ''}|${query.thread ?? ''}`;
}
