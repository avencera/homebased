// Typed client for the daemon web API: reads plus the guarded queue controls. Each wire shape is
// one Effect schema, and its type derives from the schema. The shapes mirror the serde views in
// `src/daemon/api/views.rs` and the queue routes in `src/daemon/queue_api.rs`; keep both sides in
// step.

import { Schema } from 'effect';

/** Schema version this client is written against. */
export const API_VERSION = 1;

/** Every request uses the dashboard origin, so a slow answer means trouble. */
const REQUEST_TIMEOUT_MS = 5000;

const API_BASE = '/v1';

const ApiVersionSchema = Schema.Literal(API_VERSION);

/**
 * Task lifecycle, `TaskStatus` in the daemon: `held` waits on its origin for dependencies, and
 * `preempted` is a run stopped for higher-priority work whose queued job runs again later.
 */
const ProcessStatusSchema = Schema.Literal(
	'held',
	'queued',
	'running',
	'succeeded',
	'failed',
	'cancelled',
	'lost',
	'preempted'
);
export type ProcessStatus = typeof ProcessStatusSchema.Type;

/** Every status, in lifecycle order. */
export const PROCESS_STATUSES: readonly ProcessStatus[] = ProcessStatusSchema.literals;

/** Statuses of a task that can still change on its own. */
export const IN_FLIGHT_STATUSES: readonly ProcessStatus[] = ['held', 'queued', 'running'];

/** Narrow a URL or user supplied string to a known status. */
export function isProcessStatus(value: string): value is ProcessStatus {
	return (PROCESS_STATUSES as readonly string[]).includes(value);
}

/** Whether the worker can still change status on its own. */
export function isInFlight(status: ProcessStatus): boolean {
	return IN_FLIGHT_STATUSES.includes(status);
}

/** Agent CLI that runs the task. */
const AgentKindSchema = Schema.Literal('codex', 'claude', 'grok', 'opencode');
export type AgentKind = typeof AgentKindSchema.Type;

/** Callback delivery state. Outlives the process state. */
const CallbackStatusSchema = Schema.Literal('pending', 'sending', 'waiting', 'sent', 'failed');
export type CallbackStatus = typeof CallbackStatusSchema.Type;

/** Worker-authored outcome of one report. `waiting` parks an agent worker on other tasks. */
const ReportOutcomeSchema = Schema.Literal('succeeded', 'failed', 'blocked', 'waiting');
export type ReportOutcome = typeof ReportOutcomeSchema.Type;

/** Why the process ended, once known. Tagged by `kind` in JSON. */
const ExitReasonSchema = Schema.Union(
	Schema.Struct({ kind: Schema.Literal('exit'), code: Schema.Finite }),
	Schema.Struct({ kind: Schema.Literal('signal'), signal: Schema.Finite }),
	Schema.Struct({ kind: Schema.Literal('cancelled') }),
	Schema.Struct({ kind: Schema.Literal('spawn_failed'), message: Schema.String })
);
export type ExitReason = typeof ExitReasonSchema.Type;

/** Public workload view. Omits private prompt and extra-arg fields. */
const WorkloadSchema = Schema.Union(
	Schema.Struct({
		type: Schema.Literal('agent'),
		agent: AgentKindSchema,
		model: Schema.NullOr(Schema.String),
		/** Reasoning effort from the agent argv, when the caller set one. */
		reasoning: Schema.optional(Schema.NullOr(Schema.String))
	}),
	Schema.Struct({ type: Schema.Literal('task'), command: Schema.Array(Schema.String) }),
	Schema.Struct({
		type: Schema.Literal('container'),
		/** Image pinned by digest. */
		image: Schema.String,
		/** Argv head that replaces the image entrypoint, when set. */
		entrypoint: Schema.optional(Schema.Array(Schema.String)),
		/** Arguments after the image. */
		args: Schema.Array(Schema.String),
		/** "all" or device indices, when set. */
		gpus: Schema.optional(Schema.Union(Schema.Literal('all'), Schema.Array(Schema.Finite)))
	})
);
export type WorkloadView = typeof WorkloadSchema.Type;

/** Witness that a container task's container stopped and was removed. */
const ContainerExitEvidenceSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('unconfirmed') }),
	Schema.Struct({ type: Schema.Literal('never_started') }),
	Schema.Struct({
		type: Schema.Literal('confirmed'),
		container_id: Schema.String,
		exit_code: Schema.Finite
	})
);
export type ContainerExitEvidence = typeof ContainerExitEvidenceSchema.Type;

/** Container that a container task owns, as the task layer saved it. */
const ContainerDetailSchema = Schema.Struct({
	/** Fixed container name. */
	name: Schema.String,
	image: Schema.String,
	/** Container ID, once the worker saved it. */
	container_id: Schema.optional(Schema.String),
	/** When a worker first saw the container start. */
	started_at: Schema.optional(Schema.String),
	/** Unconfirmed until the task ends. */
	exit_evidence: ContainerExitEvidenceSchema
});
export type ContainerDetail = typeof ContainerDetailSchema.Type;

/** Inactivity-reminder state for the check timeout. */
const CheckTimeoutStatusSchema = Schema.Literal('pending', 'sent');
export type CheckTimeoutStatus = typeof CheckTimeoutStatusSchema.Type;

/** `GET /v1/status`. */
const DaemonStatusSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	/** Crate version of the running daemon. */
	version: Schema.String,
	pid: Schema.Finite,
	/** Unix socket path the CLI talks to. */
	socket: Schema.String,
	/** Dashboard base URL, or null when the TCP listener is off. */
	web: Schema.NullOr(Schema.String),
	/** Queued plus running tasks. */
	in_flight: Schema.Finite
});
export type DaemonStatus = typeof DaemonStatusSchema.Type;

/** Fields every chain state carries: the chain id, this task's run number, and the run count. */
const ChainRunFields = {
	/** Task id of the chain's first run. */
	id: Schema.String,
	/** 1-based position of this task in the chain. */
	run: Schema.Finite,
	/** Runs the chain has so far, including a held continuation. */
	runs: Schema.Finite
};

/**
 * Chain of an agent run that parked or continues parked work. `waiting` means the run in
 * `current` parked on the tasks in `on`, and `continuation` starts once they all ended.
 */
const ChainSchema = Schema.Union(
	Schema.Struct({ ...ChainRunFields, state: Schema.Literal('running'), current: Schema.String }),
	Schema.Struct({
		...ChainRunFields,
		state: Schema.Literal('waiting'),
		current: Schema.String,
		on: Schema.Array(Schema.String),
		continuation: Schema.String
	}),
	Schema.Struct({ ...ChainRunFields, state: Schema.Literal('ended'), outcome: Schema.String })
);
export type Chain = typeof ChainSchema.Type;

/** One row of `GET /v1/tasks`. */
const TaskSummarySchema = Schema.Struct({
	id: Schema.String,
	/** Submitted name. */
	name: Schema.String,
	status: ProcessStatusSchema,
	workload: WorkloadSchema,
	/** Submitting Codex thread or Claude Code session. */
	thread: Schema.String,
	/** Codex thread created by the task worker, when the worker printed one. */
	worker_thread: Schema.optional(Schema.String),
	cwd: Schema.String,
	/** Git worktree root that the executor found, when there is one. */
	project_root: Schema.optional(Schema.NullOr(Schema.String)),
	/** Machine that runs the submitting thread. Absent for a task that stays on one machine. */
	origin_machine: Schema.optional(Schema.String),
	/** Machine that runs the task. Present with `origin_machine`. */
	execution_machine: Schema.optional(Schema.String),
	/** Worker pid while running. */
	pid: Schema.NullOr(Schema.Finite),
	/** Terminal callback delivery, or null when this machine delivers none for the task. */
	callback: Schema.NullOr(CallbackStatusSchema),
	/** Output-inactivity timeout in seconds. */
	timeout_secs: Schema.Finite,
	/** Whether the inactivity reminder is pending or sent. */
	check_timeout: CheckTimeoutStatusSchema,
	exit_reason: Schema.NullOr(ExitReasonSchema),
	cancel_requested_at: Schema.NullOr(Schema.String),
	created_at: Schema.String,
	/** For a terminal task this is the finish time. */
	updated_at: Schema.String,
	/** Chain of a run of parked work. A parked run's own status reads `succeeded`. */
	chain: Schema.optional(ChainSchema)
});
export type TaskSummary = typeof TaskSummarySchema.Type;

/** Whether this task is the run that parked its chain, so its work is still waiting. */
export function isParked(task: TaskSummary): boolean {
	return task.chain?.state === 'waiting' && task.chain.current === task.id;
}

/** Where a browser opens one machine's dashboard. */
const MachineLocationSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('local') }),
	/** A peer at its best-ranked address, when the daemon knows one. */
	Schema.Struct({ type: Schema.Literal('peer'), address: Schema.NullOr(Schema.String) })
);
export type MachineLocation = typeof MachineLocationSchema.Type;

/** Result of reading one machine's tasks for the fleet list. */
const MachineReadSchema = Schema.Union(
	Schema.Struct({ state: Schema.Literal('online') }),
	Schema.Struct({ state: Schema.Literal('unavailable'), message: Schema.String })
);
export type MachineRead = typeof MachineReadSchema.Type;

/** One machine of `GET /v1/fleet/tasks`. */
const FleetMachineSchema = Schema.Struct({
	machine: Schema.String,
	name: Schema.String,
	/** Homebased version of the daemon, from the last probe for a peer. */
	version: Schema.String,
	location: MachineLocationSchema,
	read: MachineReadSchema
});
export type FleetMachine = typeof FleetMachineSchema.Type;

/** One task of `GET /v1/fleet/tasks` and the machine that runs it. */
const FleetTaskSchema = Schema.Struct({ machine: Schema.String, task: TaskSummarySchema });
export type FleetTask = typeof FleetTaskSchema.Type;

/** `GET /v1/fleet/tasks`. The serving machine comes first. */
const FleetTaskListSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	machines: Schema.Array(FleetMachineSchema),
	/** Newest first. */
	tasks: Schema.Array(FleetTaskSchema)
});
export type FleetTaskList = typeof FleetTaskListSchema.Type;

/** `POST /v1/fleet/thread-titles`. */
const ThreadTitlesSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	titles: Schema.Array(
		Schema.Struct({
			machine: Schema.optional(Schema.String),
			thread: Schema.String,
			title: Schema.NullOr(Schema.String)
		})
	)
});

/** Directory entry kind from `GET /v1/files/{token}`. */
const FileEntryKindSchema = Schema.Literal('directory', 'file', 'symlink', 'other');
export type FileEntryKind = typeof FileEntryKindSchema.Type;

/** One entry in a directory listing. */
const FileEntrySchema = Schema.Struct({
	name: Schema.String,
	kind: FileEntryKindSchema,
	target_kind: Schema.optional(Schema.NullOr(FileEntryKindSchema)),
	token: Schema.String,
	content_path: Schema.optional(Schema.NullOr(Schema.String)),
	size: Schema.optional(Schema.NullOr(Schema.Finite)),
	modified: Schema.optional(Schema.NullOr(Schema.String))
});
export type FileEntry = typeof FileEntrySchema.Type;

/** `POST /v1/files/resolve`. */
const ResolvedPathSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	requested: Schema.String,
	resolved: Schema.optional(Schema.NullOr(Schema.String)),
	kind: FileEntryKindSchema,
	token: Schema.String,
	content_path: Schema.optional(Schema.NullOr(Schema.String)),
	size: Schema.optional(Schema.NullOr(Schema.Finite)),
	modified: Schema.optional(Schema.NullOr(Schema.String))
});
export type ResolvedPath = typeof ResolvedPathSchema.Type;

/** `GET /v1/files/{token}`. */
const DirectoryListingSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	path: Schema.String,
	token: Schema.String,
	parent: Schema.optional(Schema.NullOr(Schema.String)),
	entries: Schema.Array(FileEntrySchema)
});
export type DirectoryListing = typeof DirectoryListingSchema.Type;

/** `GET /v1/files/origin`. */
const ContentOriginSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	port: Schema.Finite
});
export type ContentOrigin = typeof ContentOriginSchema.Type;

/** One append-only worker report. */
const TaskReportSchema = Schema.Struct({
	/** 1-based sequence. */
	seq: Schema.Finite,
	outcome: ReportOutcomeSchema,
	summary: Schema.String,
	reported_at: Schema.String,
	/** When an interim `--notify` send succeeded. */
	notified_at: Schema.optional(Schema.NullOr(Schema.String))
});
export type TaskReport = typeof TaskReportSchema.Type;

/**
 * Callback event already sent, or the one that will be sent. Rendered as JSON,
 * so only the discriminating `event` name is typed.
 */
const TaskEventSchema = Schema.Record({ key: Schema.String, value: Schema.Unknown }).pipe(
	Schema.filter(
		(value): value is Record<string, unknown> & { event: string } => typeof value.event === 'string'
	)
);
export type TaskEvent = typeof TaskEventSchema.Type;

/** `GET /v1/tasks/{id}`: the list fields plus everything on disk. */
const TaskDetailSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	...TaskSummarySchema.fields,
	reports: Schema.Array(TaskReportSchema),
	/** Combined stdout and stderr of the child. */
	output_log: Schema.String,
	/** Task directory. */
	evidence: Schema.String,
	last_event: Schema.NullOr(TaskEventSchema),
	/** Container and its witness. Present only for container tasks. */
	container: Schema.optional(ContainerDetailSchema)
});
export type TaskDetail = typeof TaskDetailSchema.Type;

/** `GET /v1/tasks/{id}/log`. */
const LogTailSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	id: Schema.String,
	/** Log text. Empty while the child has written nothing. */
	log: Schema.String,
	/** Whether earlier lines were dropped. */
	truncated: Schema.Boolean
});
export type LogTail = typeof LogTailSchema.Type;

/** Serving level of a queued job. */
const PrioritySchema = Schema.Literal('high', 'medium', 'low');
export type Priority = typeof PrioritySchema.Type;

/** How a running job gives up its resource to higher-priority work. */
const PreemptionSchema = Schema.Union(
	Schema.Struct({ mode: Schema.Literal('restart') }),
	Schema.Struct({
		mode: Schema.Literal('wait', 'yield'),
		restart_within: Schema.optional(Schema.String)
	})
);
export type Preemption = typeof PreemptionSchema.Type;

/** Resources a job may run on: any free one, or one pinned resource by ID. */
const JobTargetSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('any') }),
	Schema.Struct({ type: Schema.Literal('pinned'), resource: Schema.String })
);
export type JobTarget = typeof JobTargetSchema.Type;

/** Where a job is in its life. */
const JobStateSchema = Schema.Union(
	Schema.Struct({
		state: Schema.Literal('queued'),
		/** The next run resumes the step from a checkpoint. */
		resume: Schema.Boolean
	}),
	Schema.Struct({ state: Schema.Literal('active'), resource: Schema.String }),
	Schema.Struct({ state: Schema.Literal('succeeded', 'cancelled') }),
	Schema.Struct({ state: Schema.Literal('failed'), run: Schema.String })
);
export type JobState = typeof JobStateSchema.Type;

/** Why the queue asked an active run to stop. */
const StopCauseSchema = Schema.Literal('yield', 'restart', 'user_cancel');
export type StopCause = typeof StopCauseSchema.Type;

/**
 * Why cleanup after a run could not finish. `processes` carries the cleanup module's own failure,
 * tagged by `kind`.
 */
const CleanupFailureSchema = Schema.Union(
	Schema.Struct({ kind: Schema.Literal('process_group_unconfirmed', 'container_unconfirmed') }),
	Schema.Struct({
		kind: Schema.Literal('processes'),
		failure: Schema.Struct(
			{ kind: Schema.String },
			Schema.Record({ key: Schema.String, value: Schema.Unknown })
		)
	})
);
export type CleanupFailure = typeof CleanupFailureSchema.Type;

/** Lifecycle of a resource's active run. */
const RunPhaseSchema = Schema.Union(
	Schema.Struct({ phase: Schema.Literal('launching'), reserved_at: Schema.String }),
	Schema.Struct({ phase: Schema.Literal('executing'), started_at: Schema.String }),
	Schema.Struct({
		phase: Schema.Literal('stopping'),
		started_at: Schema.NullOr(Schema.String),
		cause: StopCauseSchema,
		requested_at: Schema.String
	}),
	Schema.Struct({ phase: Schema.Literal('cleaning'), attempt: Schema.Finite }),
	Schema.Struct({
		phase: Schema.Literal('attention'),
		id: Schema.String,
		failure: CleanupFailureSchema
	})
);
export type RunPhase = typeof RunPhaseSchema.Type;

/** The one run a resource holds. */
const ActiveRunSchema = Schema.Struct({
	resource: Schema.String,
	job: Schema.String,
	/** Run task, an ordinary task with its own page and logs. */
	task: Schema.String,
	run_number: Schema.Finite,
	/** 0-based step index. */
	step: Schema.Finite,
	/** Whether the run resumes its step from a checkpoint. */
	resume: Schema.Boolean,
	phase: RunPhaseSchema
});
export type ActiveRun = typeof ActiveRunSchema.Type;

/** One exclusive lane, normally one GPU. */
const ResourceSchema = Schema.Struct({
	id: Schema.String,
	name: Schema.String,
	/** GPU index exported to runs, if any. */
	device: Schema.NullOr(Schema.Finite)
});
export type Resource = typeof ResourceSchema.Type;

/** One row of `GET /v1/resources`. */
const ResourceRecordSchema = Schema.Struct({
	machine: Schema.String,
	resource: ResourceSchema,
	/** Null while idle. */
	run: Schema.NullOr(ActiveRunSchema)
});
export type ResourceRecord = typeof ResourceRecordSchema.Type;

/** `GET /v1/resources`. */
const ResourceListSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	machine: Schema.String,
	/** By name. */
	resources: Schema.Array(ResourceRecordSchema)
});
export type ResourceList = typeof ResourceListSchema.Type;

/** Workload of one job step, as accepted. Only the fields the dashboard shows are typed. */
const StepViewSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('task'), command: Schema.Array(Schema.String) }),
	Schema.Struct({
		type: Schema.Literal('container'),
		image: Schema.String,
		args: Schema.optional(Schema.Array(Schema.String))
	})
);
export type StepView = typeof StepViewSchema.Type;

/** The accepted job spec fields the dashboard shows. */
const JobSpecViewSchema = Schema.Struct({
	name: Schema.String,
	cwd: Schema.String,
	thread: Schema.String,
	preempt: PreemptionSchema,
	/** Resource name or ID the submitter pinned, if any. */
	resource: Schema.optional(Schema.NullOr(Schema.String)),
	steps: Schema.Array(StepViewSchema)
});
export type JobSpecView = typeof JobSpecViewSchema.Type;

/** One job of `GET /v1/resource/jobs`, and the head of a job detail. */
const JobRecordSchema = Schema.Struct({
	id: Schema.String,
	/** Machine whose queue holds the job. */
	machine: Schema.String,
	/** Machine that submitted it. */
	origin: Schema.String,
	spec: JobSpecViewSchema,
	target: JobTargetSchema,
	priority: PrioritySchema,
	/** 1-based position within the level; null once terminal. */
	position: Schema.NullOr(Schema.Finite),
	state: JobStateSchema,
	/** 0-based step the job is at: the step its next or current run executes, or the step it ended on. */
	step: Schema.Finite,
	/** Runs started so far. */
	runs: Schema.Finite,
	created_at: Schema.String,
	updated_at: Schema.String
});
export type JobRecord = typeof JobRecordSchema.Type;

/** `GET /v1/resource/jobs`: every non-terminal job, in serving order. */
const JobListSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	machine: Schema.String,
	jobs: Schema.Array(JobRecordSchema)
});
export type JobList = typeof JobListSchema.Type;

/** Attributable cleanup result after a run: `Ok` or the failure. */
const CleanupResultSchema = Schema.Union(
	Schema.Struct({ Ok: Schema.Null }),
	Schema.Struct({ Err: CleanupFailureSchema })
);
export type CleanupResult = typeof CleanupResultSchema.Type;

/** One attempt of one step. */
const JobRunSchema = Schema.Struct({
	task: Schema.String,
	run_number: Schema.Finite,
	step: Schema.Finite,
	status: ProcessStatusSchema,
	/** Present once the run task ended. */
	outcome: Schema.NullOr(ProcessStatusSchema),
	/** Unknown for runs stored before resource history was kept. */
	resource: Schema.NullOr(Schema.String),
	stop_cause: Schema.NullOr(StopCauseSchema),
	/** Present once cleanup finished. */
	cleanup: Schema.NullOr(CleanupResultSchema)
});
export type JobRun = typeof JobRunSchema.Type;

/** One stored job event. Rendered as data, so only its kind and time are typed. */
const JobEventViewSchema = Schema.Struct(
	{ seq: Schema.Finite, event: Schema.String, at: Schema.String },
	Schema.Record({ key: Schema.String, value: Schema.Unknown })
);
export type JobEventView = typeof JobEventViewSchema.Type;

/** `GET /v1/resource/jobs/{job}`. */
const JobDetailSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	job: JobRecordSchema,
	active_run: Schema.NullOr(ActiveRunSchema),
	/** Every attempt, oldest first. */
	runs: Schema.Array(JobRunSchema),
	last_stop_cause: Schema.NullOr(StopCauseSchema),
	events: Schema.Array(JobEventViewSchema)
});
export type JobDetail = typeof JobDetailSchema.Type;

/** Where a move puts a job; `priority: null` keeps its current level. */
export type Placement =
	| { type: 'edge'; priority: Priority | null; end: 'front' | 'back' }
	| { type: 'relative'; target: string; side: 'before' | 'after'; expect: Priority | null };

/** `POST /v1/resource/jobs/{job}/move`. */
const MoveResultSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	job: Schema.String,
	priority: PrioritySchema,
	position: Schema.Finite
});
export type MoveResult = typeof MoveResultSchema.Type;

/** `POST /v1/resource/jobs/{job}/cancel`. */
const CancelResultSchema = Schema.Union(
	Schema.Struct({ api_version: ApiVersionSchema, result: Schema.Literal('cancelled') }),
	Schema.Struct({
		api_version: ApiVersionSchema,
		result: Schema.Literal('stopping'),
		resource: Schema.String,
		task: Schema.String
	}),
	Schema.Struct({
		api_version: ApiVersionSchema,
		result: Schema.Literal('already_terminal'),
		state: Schema.String
	})
);
export type CancelResult = typeof CancelResultSchema.Type;

/** `POST /v1/resource/release`. */
const ReleaseResultSchema = Schema.Struct({
	api_version: ApiVersionSchema,
	resource: Schema.String,
	attention: Schema.String
});
export type ReleaseResult = typeof ReleaseResultSchema.Type;

/** Error envelope body: `{ error: { ... } }`. */
export interface ApiErrorBody {
	code: string;
	message: string;
	retryable: boolean;
	input: unknown;
}

const ErrorEnvelopeSchema = Schema.Struct({
	error: Schema.Struct({
		code: Schema.optional(Schema.String),
		message: Schema.optional(Schema.String),
		retryable: Schema.optional(Schema.Boolean),
		input: Schema.optional(Schema.Unknown)
	})
});

/** A daemon error envelope, or a transport failure shaped like one. */
export class ApiError extends Error {
	readonly code: string;
	readonly retryable: boolean;
	readonly input: unknown;
	/** HTTP status, or null when the request never reached the daemon. */
	readonly httpStatus: number | null;

	constructor(body: ApiErrorBody, httpStatus: number | null) {
		super(body.message);
		this.name = 'ApiError';
		this.code = body.code;
		this.retryable = body.retryable;
		this.input = body.input;
		this.httpStatus = httpStatus;
	}

	/** Whether the daemon answered at all. Drives the socket up/down marker. */
	get reachable(): boolean {
		return this.httpStatus !== null;
	}

	/** The request never produced an envelope: no listener, abort, or timeout. */
	static unreachable(cause: unknown): ApiError {
		return new ApiError(
			{
				code: 'daemon_unavailable',
				message: `daemon unreachable: ${describeCause(cause)}`,
				retryable: true,
				input: {}
			},
			null
		);
	}
}

/** Coerce a caught value into an `ApiError`. */
export function asApiError(cause: unknown): ApiError {
	return cause instanceof ApiError ? cause : ApiError.unreachable(cause);
}

/** `GET /v1/status`. */
export function fetchStatus(): Promise<DaemonStatus> {
	return getJson('/status', DaemonStatusSchema);
}

/** Query naming the machine whose queue a request reads or changes; omitted for this machine. */
function machineQuery(machine: string | null | undefined): string {
	return machine ? `?${new URLSearchParams({ machine })}` : '';
}

/** `GET /v1/resources`: one machine's resources and the run each holds. */
export function fetchResources(machine?: string | null): Promise<ResourceList> {
	return getJson(`/resources${machineQuery(machine)}`, ResourceListSchema);
}

/** `GET /v1/resource/jobs`: one machine's queue in serving order. */
export function fetchJobs(machine?: string | null): Promise<JobList> {
	return getJson(`/resource/jobs${machineQuery(machine)}`, JobListSchema);
}

/** `GET /v1/resource/jobs/{job}`, from the job's saved authority unless a machine is given. */
export function fetchJob(id: string, machine?: string | null): Promise<JobDetail> {
	return getJson(
		`/resource/jobs/${encodeURIComponent(id)}${machineQuery(machine)}`,
		JobDetailSchema
	);
}

/**
 * `POST /v1/resource/jobs/{job}/move`. The operation ID makes a retry of the same request return
 * the stored result instead of moving twice.
 */
export function moveJob(
	id: string,
	machine: string,
	operationId: string,
	placement: Placement
): Promise<MoveResult> {
	return postJson(
		`/resource/jobs/${encodeURIComponent(id)}/move${machineQuery(machine)}`,
		{ operation_id: operationId, placement },
		MoveResultSchema
	);
}

/** `POST /v1/resource/jobs/{job}/cancel`. */
export function cancelJob(id: string, machine: string, operationId: string): Promise<CancelResult> {
	return postJson(
		`/resource/jobs/${encodeURIComponent(id)}/cancel${machineQuery(machine)}`,
		{ operation_id: operationId },
		CancelResultSchema
	);
}

/** `POST /v1/resource/release`: leave one exact `Attention`, never a later one. */
export function releaseAttention(
	attention: string,
	machine: string,
	operationId: string
): Promise<ReleaseResult> {
	return postJson(
		`/resource/release${machineQuery(machine)}`,
		{ operation_id: operationId, attention },
		ReleaseResultSchema
	);
}

/** Filter for `GET /v1/fleet/tasks`. */
export interface TaskQuery {
	statuses?: readonly ProcessStatus[];
	thread?: string | null;
}

/** `GET /v1/fleet/tasks`: tasks from this machine and every reachable peer. */
export function fetchFleetTasks(query: TaskQuery = {}): Promise<FleetTaskList> {
	const params = new URLSearchParams();
	if (query.statuses?.length) params.set('status', query.statuses.join(','));
	if (query.thread) params.set('thread', query.thread);
	const search = params.size > 0 ? `?${params}` : '';
	return getJson(`/fleet/tasks${search}`, FleetTaskListSchema);
}

/**
 * Dashboard URL of a task that runs on a peer. Task detail and logs stay on the
 * executor, so the link opens the peer's own dashboard.
 */
export function peerTaskHref(machine: FleetMachine | undefined, id: string): string | null {
	if (machine?.location.type !== 'peer' || !machine.location.address) return null;
	return `${machine.location.address.replace(/\/+$/, '')}/tasks/${encodeURIComponent(id)}`;
}

/** One thread and the machine that runs it. */
export interface ThreadRef {
	/** Machine that runs the thread. Omitted for the machine that serves the dashboard. */
	machine?: string;
	thread: string;
}

/** Title of one requested thread, or null when no store names it. */
export interface ThreadTitle extends ThreadRef {
	title: string | null;
}

/** Submitting thread of a fleet task and the machine that runs it. */
export function taskThread(entry: FleetTask): ThreadRef {
	return { machine: entry.task.origin_machine ?? entry.machine, thread: entry.task.thread };
}

/** Most threads one title read may name. */
export const MAX_THREAD_TITLES = 200;

/**
 * `POST /v1/fleet/thread-titles`: T3 Code title, else the agent's own title, read
 * on the machine that runs each thread.
 */
export async function fetchThreadTitles(
	threads: readonly ThreadRef[]
): Promise<readonly ThreadTitle[]> {
	const body = await postJson('/fleet/thread-titles', { threads }, ThreadTitlesSchema);
	return body.titles;
}

/** `GET /v1/tasks/{id}`. */
export function fetchTask(id: string): Promise<TaskDetail> {
	return getJson(`/tasks/${encodeURIComponent(id)}`, TaskDetailSchema);
}

/** `GET /v1/tasks/{id}/log?tail=N`. */
export function fetchLogTail(id: string, tail: number): Promise<LogTail> {
	return getJson(`/tasks/${encodeURIComponent(id)}/log?tail=${tail}`, LogTailSchema);
}

/** `POST /v1/files/resolve`. */
export async function resolvePath(path: string): Promise<ResolvedPath> {
	return postJson('/files/resolve', { path }, ResolvedPathSchema);
}

/** `GET /v1/files/{token}`. */
export function fetchDirectory(token: string): Promise<DirectoryListing> {
	return getJson(`/files/${encodeURIComponent(token)}`, DirectoryListingSchema);
}

/** `GET /v1/files/origin`. */
export function fetchContentOrigin(): Promise<ContentOrigin> {
	return getJson('/files/origin', ContentOriginSchema);
}

/**
 * Absolute URL on the content origin for a UTF-8 filesystem path. Uses the
 * current browser hostname so loopback, LAN, and Tailscale clients match.
 */
export function contentUrlForPath(port: number, absolutePath: string): string {
	const trimmed = absolutePath.startsWith('/') ? absolutePath.slice(1) : absolutePath;
	const segments = trimmed.split('/').map(encodeURIComponent).join('/');
	return `${contentOriginBase(port)}/raw/${segments}`;
}

/** Absolute URL on the content origin for an opaque path token. */
export function contentUrlForToken(port: number, token: string): string {
	return `${contentOriginBase(port)}/by-token/${encodeURIComponent(token)}`;
}

function contentOriginBase(port: number): string {
	const { protocol, hostname } = window.location;
	const host = hostname.includes(':') ? `[${hostname}]` : hostname;
	return `${protocol}//${host}:${port}`;
}

async function getJson<S extends Schema.Schema.AnyNoContext>(
	path: string,
	schema: S
): Promise<Schema.Schema.Type<S>> {
	return decodeResponse(schema, await requestJson(path, { method: 'GET' }));
}

async function postJson<S extends Schema.Schema.AnyNoContext>(
	path: string,
	body: unknown,
	schema: S
): Promise<Schema.Schema.Type<S>> {
	return decodeResponse(
		schema,
		await requestJson(path, {
			method: 'POST',
			headers: { 'content-type': 'application/json' },
			body: JSON.stringify(body)
		})
	);
}

async function requestJson(path: string, init: RequestInit): Promise<unknown> {
	let response: Response;
	try {
		response = await fetch(`${API_BASE}${path}`, {
			...init,
			headers: { accept: 'application/json', ...init.headers },
			signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS)
		});
	} catch (cause) {
		throw ApiError.unreachable(cause);
	}
	if (!response.ok) throw await errorFromResponse(response);
	try {
		const body: unknown = await response.json();
		return body;
	} catch (cause) {
		throw new ApiError(
			{
				code: 'invalid_json',
				message: `daemon returned invalid JSON: ${describeCause(cause)}`,
				retryable: true,
				input: {}
			},
			response.status
		);
	}
}

function decodeResponse<S extends Schema.Schema.AnyNoContext>(
	schema: S,
	input: unknown
): Schema.Schema.Type<S> {
	try {
		return Schema.decodeUnknownSync(schema)(input);
	} catch (cause) {
		const message = cause instanceof Error ? cause.message : String(cause);
		throw new ApiError(
			{
				code: 'invalid_response',
				message: `daemon returned an invalid response: ${message}`,
				retryable: false,
				input: {}
			},
			200
		);
	}
}

async function errorFromResponse(response: Response): Promise<ApiError> {
	const fallback: ApiErrorBody = {
		code: 'http_error',
		message: `${response.status} ${response.statusText}`.trim(),
		retryable: response.status >= 500,
		input: {}
	};
	try {
		const parsed: unknown = await response.json();
		const body = Schema.decodeUnknownSync(ErrorEnvelopeSchema)(parsed);
		const error = body.error;
		if (!error?.message) return new ApiError(fallback, response.status);
		return new ApiError(
			{
				code: error.code ?? fallback.code,
				message: error.message,
				retryable: error.retryable ?? fallback.retryable,
				input: error.input ?? {}
			},
			response.status
		);
	} catch {
		return new ApiError(fallback, response.status);
	}
}

function describeCause(cause: unknown): string {
	if (cause instanceof DOMException && cause.name === 'TimeoutError') return 'request timed out';
	if (cause instanceof Error) return cause.message;
	return String(cause);
}
