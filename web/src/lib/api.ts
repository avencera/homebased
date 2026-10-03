// Typed client for the daemon web API: reads plus the guarded queue controls. The shapes
// mirror the serde views in `src/daemon/api/views.rs` and the queue routes in
// `src/daemon/queue_api.rs`; keep both sides in step.

import { Schema } from 'effect';

/** Schema version this client is written against. */
export const API_VERSION = 1;

/** Every request uses the dashboard origin, so a slow answer means trouble. */
const REQUEST_TIMEOUT_MS = 5000;

const API_BASE = '/v1';

/**
 * Task lifecycle, `TaskStatus` in the daemon: `held` waits on its origin for dependencies, and
 * `preempted` is a run stopped for higher-priority work whose queued job runs again later.
 */
export type ProcessStatus =
	'held' | 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'lost' | 'preempted';

/** Every status, in lifecycle order. */
export const PROCESS_STATUSES: readonly ProcessStatus[] = [
	'held',
	'queued',
	'running',
	'succeeded',
	'failed',
	'cancelled',
	'lost',
	'preempted'
];

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
export type AgentKind = 'codex' | 'claude' | 'grok' | 'opencode';

/** Callback delivery state. Outlives the process state. */
export type CallbackStatus = 'pending' | 'sending' | 'waiting' | 'sent' | 'failed';

/** Worker-authored outcome of one report. */
export type ReportOutcome = 'succeeded' | 'failed' | 'blocked';

/** Why the process ended, once known. Tagged by `kind` in JSON. */
export type ExitReason =
	| { kind: 'exit'; code: number }
	| { kind: 'signal'; signal: number }
	| { kind: 'cancelled' }
	| { kind: 'spawn_failed'; message: string };

/** Public workload view. Omits private prompt and extra-arg fields. */
export type WorkloadView =
	| {
			type: 'agent';
			agent: AgentKind;
			model: string | null;
			/** Reasoning effort from the agent argv, when the caller set one. */
			reasoning?: string | null;
	  }
	| { type: 'task'; command: readonly string[] }
	| {
			type: 'container';
			/** Image pinned by digest. */
			image: string;
			/** Argv head that replaces the image entrypoint, when set. */
			entrypoint?: readonly string[];
			/** Arguments after the image. */
			args: readonly string[];
			/** "all" or device indices, when set. */
			gpus?: 'all' | readonly number[];
	  };

/** Witness that a container task's container stopped and was removed. */
export type ContainerExitEvidence =
	| { type: 'unconfirmed' }
	| { type: 'never_started' }
	| { type: 'confirmed'; container_id: string; exit_code: number };

/** Container that a container task owns, as the task layer saved it. */
export interface ContainerDetail {
	/** Fixed container name. */
	name: string;
	image: string;
	/** Container ID, once the worker saved it. */
	container_id?: string;
	/** When a worker first saw the container start. */
	started_at?: string;
	/** Unconfirmed until the task ends. */
	exit_evidence: ContainerExitEvidence;
}

/** Inactivity-reminder state for the check timeout. */
export type CheckTimeoutStatus = 'pending' | 'sent';

/** `GET /v1/status`. */
export interface DaemonStatus {
	api_version: number;
	/** Crate version of the running daemon. */
	version: string;
	pid: number;
	/** Unix socket path the CLI talks to. */
	socket: string;
	/** Dashboard base URL, or null when the TCP listener is off. */
	web: string | null;
	/** Queued plus running tasks. */
	in_flight: number;
}

/** One row of `GET /v1/tasks`. */
export interface TaskSummary {
	id: string;
	/** Submitted name. */
	name: string;
	status: ProcessStatus;
	workload: WorkloadView;
	/** Submitting Codex thread or Claude Code session. */
	thread: string;
	/** Codex thread created by the task worker, when the worker printed one. */
	worker_thread?: string;
	cwd: string;
	/** Git worktree root that the executor found, when there is one. */
	project_root?: string | null;
	/** Machine that runs the submitting thread. Absent for a task that stays on one machine. */
	origin_machine?: string;
	/** Machine that runs the task. Present with `origin_machine`. */
	execution_machine?: string;
	/** Worker pid while running. */
	pid: number | null;
	/** Terminal callback delivery, or null when this machine delivers none for the task. */
	callback: CallbackStatus | null;
	/** Output-inactivity timeout in seconds. */
	timeout_secs: number;
	/** Whether the inactivity reminder is pending or sent. */
	check_timeout: CheckTimeoutStatus;
	exit_reason: ExitReason | null;
	cancel_requested_at: string | null;
	created_at: string;
	/** For a terminal task this is the finish time. */
	updated_at: string;
}

/** Where a browser opens one machine's dashboard. */
export type MachineLocation =
	| { type: 'local' }
	/** A peer at its best-ranked address, when the daemon knows one. */
	| { type: 'peer'; address: string | null };

/** Result of reading one machine's tasks for the fleet list. */
export type MachineRead = { state: 'online' } | { state: 'unavailable'; message: string };

/** One machine of `GET /v1/fleet/tasks`. */
export interface FleetMachine {
	machine: string;
	name: string;
	/** Homebased version of the daemon, from the last probe for a peer. */
	version: string;
	location: MachineLocation;
	read: MachineRead;
}

/** One task of `GET /v1/fleet/tasks` and the machine that runs it. */
export interface FleetTask {
	machine: string;
	task: TaskSummary;
}

/** `GET /v1/fleet/tasks`. The serving machine comes first. */
export interface FleetTaskList {
	api_version: number;
	machines: readonly FleetMachine[];
	/** Newest first. */
	tasks: readonly FleetTask[];
}

/** Directory entry kind from `GET /v1/files/{token}`. */
export type FileEntryKind = 'directory' | 'file' | 'symlink' | 'other';

/** One entry in a directory listing. */
export interface FileEntry {
	name: string;
	kind: FileEntryKind;
	target_kind?: FileEntryKind | null;
	token: string;
	content_path?: string | null;
	size?: number | null;
	modified?: string | null;
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

/** `POST /v1/files/resolve`. */
export interface ResolvedPath {
	api_version: number;
	requested: string;
	resolved?: string | null;
	kind: FileEntryKind;
	token: string;
	content_path?: string | null;
	size?: number | null;
	modified?: string | null;
}

/** `GET /v1/files/{token}`. */
export interface DirectoryListing {
	api_version: number;
	path: string;
	token: string;
	parent?: string | null;
	entries: readonly FileEntry[];
}

/** `GET /v1/files/origin`. */
export interface ContentOrigin {
	api_version: number;
	port: number;
}

/** One append-only worker report. */
export interface TaskReport {
	/** 1-based sequence. */
	seq: number;
	outcome: ReportOutcome;
	summary: string;
	reported_at: string;
	/** When an interim `--notify` send succeeded. */
	notified_at?: string | null;
}

/**
 * Callback event already sent, or the one that will be sent. Rendered as JSON,
 * so only the discriminating `event` name is typed.
 */
export interface TaskEvent {
	event: string;
	[key: string]: unknown;
}

/** `GET /v1/tasks/{id}`: the list fields plus everything on disk. */
export interface TaskDetail extends TaskSummary {
	api_version: number;
	reports: readonly TaskReport[];
	/** Combined stdout and stderr of the child. */
	output_log: string;
	/** Task directory. */
	evidence: string;
	last_event: TaskEvent | null;
	/** Container and its witness. Present only for container tasks. */
	container?: ContainerDetail;
}

/** Serving level of a queued job. */
export type Priority = 'high' | 'medium' | 'low';

/** How a running job gives up its resource to higher-priority work. */
export type Preemption =
	| { mode: 'restart' }
	| { mode: 'wait'; restart_within?: string }
	| { mode: 'yield'; restart_within?: string };

/** Resources a job may run on: any free one, or one pinned resource by ID. */
export type JobTarget = { type: 'any' } | { type: 'pinned'; resource: string };

/** Where a job is in its life. */
export type JobState =
	/** `resume`: the next run resumes the step from a checkpoint. */
	| { state: 'queued'; resume: boolean }
	| { state: 'active'; resource: string }
	| { state: 'succeeded' }
	| { state: 'failed'; run: string }
	| { state: 'cancelled' };

/** Why the queue asked an active run to stop. */
export type StopCause = 'yield' | 'restart' | 'user_cancel';

/**
 * Why cleanup after a run could not finish. `processes` carries the cleanup module's own failure,
 * tagged by `kind`.
 */
export type CleanupFailure =
	| { kind: 'process_group_unconfirmed' }
	| { kind: 'container_unconfirmed' }
	| { kind: 'processes'; failure: { kind: string; [key: string]: unknown } };

/** Lifecycle of a resource's active run. */
export type RunPhase =
	| { phase: 'launching'; reserved_at: string }
	| { phase: 'executing'; started_at: string }
	| { phase: 'stopping'; started_at: string | null; cause: StopCause; requested_at: string }
	| { phase: 'cleaning'; attempt: number }
	| { phase: 'attention'; id: string; failure: CleanupFailure };

/** The one run a resource holds. */
export interface ActiveRun {
	resource: string;
	job: string;
	/** Run task, an ordinary task with its own page and logs. */
	task: string;
	run_number: number;
	/** 0-based step index. */
	step: number;
	/** Whether the run resumes its step from a checkpoint. */
	resume: boolean;
	phase: RunPhase;
}

/** One exclusive lane, normally one GPU. */
export interface Resource {
	id: string;
	name: string;
	/** GPU index exported to runs, if any. */
	device: number | null;
}

/** One row of `GET /v1/resources`. */
export interface ResourceRecord {
	machine: string;
	resource: Resource;
	/** Null while idle. */
	run: ActiveRun | null;
}

/** `GET /v1/resources`. */
export interface ResourceList {
	api_version: number;
	machine: string;
	/** By name. */
	resources: readonly ResourceRecord[];
}

/** Workload of one job step, as accepted. Only the fields the dashboard shows are typed. */
export type StepView =
	| { type: 'task'; command: readonly string[] }
	| { type: 'container'; image: string; args?: readonly string[] };

/** The accepted job spec fields the dashboard shows. */
export interface JobSpecView {
	name: string;
	cwd: string;
	thread: string;
	preempt: Preemption;
	/** Resource name or ID the submitter pinned, if any. */
	resource?: string | null;
	steps: readonly StepView[];
}

/** One job of `GET /v1/resource/jobs`, and the head of a job detail. */
export interface JobRecord {
	id: string;
	/** Machine whose queue holds the job. */
	machine: string;
	/** Machine that submitted it. */
	origin: string;
	spec: JobSpecView;
	target: JobTarget;
	priority: Priority;
	/** 1-based position within the level; null once terminal. */
	position: number | null;
	state: JobState;
	/** 0-based step the job is at: the step its next or current run executes, or the step it ended on. */
	step: number;
	/** Runs started so far. */
	runs: number;
	created_at: string;
	updated_at: string;
}

/** `GET /v1/resource/jobs`: every non-terminal job, in serving order. */
export interface JobList {
	api_version: number;
	machine: string;
	jobs: readonly JobRecord[];
}

/** Attributable cleanup result after a run: `Ok` or the failure. */
export type CleanupResult = { Ok: null } | { Err: CleanupFailure };

/** One attempt of one step. */
export interface JobRun {
	task: string;
	run_number: number;
	step: number;
	status: ProcessStatus;
	/** Present once the run task ended. */
	outcome: ProcessStatus | null;
	/** Unknown for runs stored before resource history was kept. */
	resource: string | null;
	stop_cause: StopCause | null;
	/** Present once cleanup finished. */
	cleanup: CleanupResult | null;
}

/** One stored job event. Rendered as data, so only its kind and time are typed. */
export interface JobEventView {
	seq: number;
	event: string;
	at: string;
	[key: string]: unknown;
}

/** `GET /v1/resource/jobs/{job}`. */
export interface JobDetail {
	api_version: number;
	job: JobRecord;
	active_run: ActiveRun | null;
	/** Every attempt, oldest first. */
	runs: readonly JobRun[];
	last_stop_cause: StopCause | null;
	events: readonly JobEventView[];
}

/** Where a move puts a job; `priority: null` keeps its current level. */
export type Placement =
	| { type: 'edge'; priority: Priority | null; end: 'front' | 'back' }
	| { type: 'relative'; target: string; side: 'before' | 'after'; expect: Priority | null };

/** `POST /v1/resource/jobs/{job}/move`. */
export interface MoveResult {
	api_version: number;
	job: string;
	priority: Priority;
	position: number;
}

/** `POST /v1/resource/jobs/{job}/cancel`. */
export type CancelResult = { api_version: number } & (
	| { result: 'cancelled' }
	| { result: 'stopping'; resource: string; task: string }
	| { result: 'already_terminal'; state: string }
);

/** `POST /v1/resource/release`. */
export interface ReleaseResult {
	api_version: number;
	resource: string;
	attention: string;
}

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
const AgentKindSchema = Schema.Literal('codex', 'claude', 'grok', 'opencode');
const CallbackStatusSchema = Schema.Literal('pending', 'sending', 'waiting', 'sent', 'failed');
const CheckTimeoutStatusSchema = Schema.Literal('pending', 'sent');
const ReportOutcomeSchema = Schema.Literal('succeeded', 'failed', 'blocked');
const ExitReasonSchema = Schema.Union(
	Schema.Struct({ kind: Schema.Literal('exit'), code: Schema.Finite }),
	Schema.Struct({ kind: Schema.Literal('signal'), signal: Schema.Finite }),
	Schema.Struct({ kind: Schema.Literal('cancelled') }),
	Schema.Struct({ kind: Schema.Literal('spawn_failed'), message: Schema.String })
);
const WorkloadSchema = Schema.Union(
	Schema.Struct({
		type: Schema.Literal('agent'),
		agent: AgentKindSchema,
		model: Schema.NullOr(Schema.String),
		reasoning: Schema.optional(Schema.NullOr(Schema.String))
	}),
	Schema.Struct({ type: Schema.Literal('task'), command: Schema.Array(Schema.String) }),
	Schema.Struct({
		type: Schema.Literal('container'),
		image: Schema.String,
		entrypoint: Schema.optional(Schema.Array(Schema.String)),
		args: Schema.Array(Schema.String),
		gpus: Schema.optional(Schema.Union(Schema.Literal('all'), Schema.Array(Schema.Finite)))
	})
);
const ContainerExitEvidenceSchema = Schema.Union(
	Schema.Struct({ type: Schema.Literal('unconfirmed') }),
	Schema.Struct({ type: Schema.Literal('never_started') }),
	Schema.Struct({
		type: Schema.Literal('confirmed'),
		container_id: Schema.String,
		exit_code: Schema.Finite
	})
);
const ContainerDetailSchema = Schema.Struct({
	name: Schema.String,
	image: Schema.String,
	container_id: Schema.optional(Schema.String),
	started_at: Schema.optional(Schema.String),
	exit_evidence: ContainerExitEvidenceSchema
});
const TaskSummarySchema = Schema.Struct({
	id: Schema.String,
	name: Schema.String,
	status: ProcessStatusSchema,
	workload: WorkloadSchema,
	thread: Schema.String,
	worker_thread: Schema.optional(Schema.String),
	cwd: Schema.String,
	project_root: Schema.optional(Schema.NullOr(Schema.String)),
	origin_machine: Schema.optional(Schema.String),
	execution_machine: Schema.optional(Schema.String),
	pid: Schema.NullOr(Schema.Finite),
	callback: Schema.NullOr(CallbackStatusSchema),
	timeout_secs: Schema.Finite,
	check_timeout: CheckTimeoutStatusSchema,
	exit_reason: Schema.NullOr(ExitReasonSchema),
	cancel_requested_at: Schema.NullOr(Schema.String),
	created_at: Schema.String,
	updated_at: Schema.String
});
const TaskReportSchema = Schema.Struct({
	seq: Schema.Finite,
	outcome: ReportOutcomeSchema,
	summary: Schema.String,
	reported_at: Schema.String,
	notified_at: Schema.optional(Schema.NullOr(Schema.String))
});
const TaskEventSchema = Schema.Record({ key: Schema.String, value: Schema.Unknown }).pipe(
	Schema.filter(
		(value): value is Record<string, unknown> & { event: string } => typeof value.event === 'string'
	)
);
const TaskDetailSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	id: Schema.String,
	name: Schema.String,
	status: ProcessStatusSchema,
	workload: WorkloadSchema,
	thread: Schema.String,
	worker_thread: Schema.optional(Schema.String),
	cwd: Schema.String,
	origin_machine: Schema.optional(Schema.String),
	execution_machine: Schema.optional(Schema.String),
	pid: Schema.NullOr(Schema.Finite),
	callback: Schema.NullOr(CallbackStatusSchema),
	timeout_secs: Schema.Finite,
	check_timeout: CheckTimeoutStatusSchema,
	exit_reason: Schema.NullOr(ExitReasonSchema),
	cancel_requested_at: Schema.NullOr(Schema.String),
	created_at: Schema.String,
	updated_at: Schema.String,
	reports: Schema.Array(TaskReportSchema),
	output_log: Schema.String,
	evidence: Schema.String,
	last_event: Schema.NullOr(TaskEventSchema),
	container: Schema.optional(ContainerDetailSchema)
});
const FleetTaskListSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	machines: Schema.Array(
		Schema.Struct({
			machine: Schema.String,
			name: Schema.String,
			version: Schema.String,
			location: Schema.Union(
				Schema.Struct({ type: Schema.Literal('local') }),
				Schema.Struct({ type: Schema.Literal('peer'), address: Schema.NullOr(Schema.String) })
			),
			read: Schema.Union(
				Schema.Struct({ state: Schema.Literal('online') }),
				Schema.Struct({ state: Schema.Literal('unavailable'), message: Schema.String })
			)
		})
	),
	tasks: Schema.Array(Schema.Struct({ machine: Schema.String, task: TaskSummarySchema }))
});
const ThreadTitlesSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	titles: Schema.Array(
		Schema.Struct({
			machine: Schema.optional(Schema.String),
			thread: Schema.String,
			title: Schema.NullOr(Schema.String)
		})
	)
});
const LogTailSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	id: Schema.String,
	log: Schema.String,
	truncated: Schema.Boolean
});

const FileEntryKindSchema = Schema.Literal('directory', 'file', 'symlink', 'other');
const FileEntrySchema = Schema.Struct({
	name: Schema.String,
	kind: FileEntryKindSchema,
	target_kind: Schema.optional(Schema.NullOr(FileEntryKindSchema)),
	token: Schema.String,
	content_path: Schema.optional(Schema.NullOr(Schema.String)),
	size: Schema.optional(Schema.NullOr(Schema.Finite)),
	modified: Schema.optional(Schema.NullOr(Schema.String))
});
const ResolvedPathSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	requested: Schema.String,
	resolved: Schema.optional(Schema.NullOr(Schema.String)),
	kind: FileEntryKindSchema,
	token: Schema.String,
	content_path: Schema.optional(Schema.NullOr(Schema.String)),
	size: Schema.optional(Schema.NullOr(Schema.Finite)),
	modified: Schema.optional(Schema.NullOr(Schema.String))
});
const DirectoryListingSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	path: Schema.String,
	token: Schema.String,
	parent: Schema.optional(Schema.NullOr(Schema.String)),
	entries: Schema.Array(FileEntrySchema)
});
const ContentOriginSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	port: Schema.Finite
});
const DaemonStatusSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	version: Schema.String,
	pid: Schema.Finite,
	socket: Schema.String,
	web: Schema.NullOr(Schema.String),
	in_flight: Schema.Finite
});

const PrioritySchema = Schema.Literal('high', 'medium', 'low');
const StopCauseSchema = Schema.Literal('yield', 'restart', 'user_cancel');
const PreemptionSchema = Schema.Union(
	Schema.Struct({ mode: Schema.Literal('restart') }),
	Schema.Struct({
		mode: Schema.Literal('wait', 'yield'),
		restart_within: Schema.optional(Schema.String)
	})
);
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
const ActiveRunSchema = Schema.Struct({
	resource: Schema.String,
	job: Schema.String,
	task: Schema.String,
	run_number: Schema.Finite,
	step: Schema.Finite,
	resume: Schema.Boolean,
	phase: RunPhaseSchema
});
const ResourceListSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	machine: Schema.String,
	resources: Schema.Array(
		Schema.Struct({
			machine: Schema.String,
			resource: Schema.Struct({
				id: Schema.String,
				name: Schema.String,
				device: Schema.NullOr(Schema.Finite)
			}),
			run: Schema.NullOr(ActiveRunSchema)
		})
	)
});
const JobStateSchema = Schema.Union(
	Schema.Struct({
		state: Schema.Literal('queued'),
		resume: Schema.Boolean
	}),
	Schema.Struct({ state: Schema.Literal('active'), resource: Schema.String }),
	Schema.Struct({ state: Schema.Literal('succeeded', 'cancelled') }),
	Schema.Struct({ state: Schema.Literal('failed'), run: Schema.String })
);
const JobRecordSchema = Schema.Struct({
	id: Schema.String,
	machine: Schema.String,
	origin: Schema.String,
	spec: Schema.Struct({
		name: Schema.String,
		cwd: Schema.String,
		thread: Schema.String,
		preempt: PreemptionSchema,
		resource: Schema.optional(Schema.NullOr(Schema.String)),
		steps: Schema.Array(
			Schema.Union(
				Schema.Struct({ type: Schema.Literal('task'), command: Schema.Array(Schema.String) }),
				Schema.Struct({
					type: Schema.Literal('container'),
					image: Schema.String,
					args: Schema.optional(Schema.Array(Schema.String))
				})
			)
		)
	}),
	target: Schema.Union(
		Schema.Struct({ type: Schema.Literal('any') }),
		Schema.Struct({ type: Schema.Literal('pinned'), resource: Schema.String })
	),
	priority: PrioritySchema,
	position: Schema.NullOr(Schema.Finite),
	state: JobStateSchema,
	step: Schema.Finite,
	runs: Schema.Finite,
	created_at: Schema.String,
	updated_at: Schema.String
});
const JobListSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	machine: Schema.String,
	jobs: Schema.Array(JobRecordSchema)
});
const JobDetailSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	job: JobRecordSchema,
	active_run: Schema.NullOr(ActiveRunSchema),
	runs: Schema.Array(
		Schema.Struct({
			task: Schema.String,
			run_number: Schema.Finite,
			step: Schema.Finite,
			status: ProcessStatusSchema,
			outcome: Schema.NullOr(ProcessStatusSchema),
			resource: Schema.NullOr(Schema.String),
			stop_cause: Schema.NullOr(StopCauseSchema),
			cleanup: Schema.NullOr(
				Schema.Union(
					Schema.Struct({ Ok: Schema.Null }),
					Schema.Struct({ Err: CleanupFailureSchema })
				)
			)
		})
	),
	last_stop_cause: Schema.NullOr(StopCauseSchema),
	events: Schema.Array(
		Schema.Struct(
			{ seq: Schema.Finite, event: Schema.String, at: Schema.String },
			Schema.Record({ key: Schema.String, value: Schema.Unknown })
		)
	)
});
const MoveResultSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	job: Schema.String,
	priority: PrioritySchema,
	position: Schema.Finite
});
const CancelResultSchema = Schema.Union(
	Schema.Struct({
		api_version: Schema.Literal(API_VERSION),
		result: Schema.Literal('cancelled')
	}),
	Schema.Struct({
		api_version: Schema.Literal(API_VERSION),
		result: Schema.Literal('stopping'),
		resource: Schema.String,
		task: Schema.String
	}),
	Schema.Struct({
		api_version: Schema.Literal(API_VERSION),
		result: Schema.Literal('already_terminal'),
		state: Schema.String
	})
);
const ReleaseResultSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	resource: Schema.String,
	attention: Schema.String
});

/** `GET /v1/tasks/{id}/log`. */
export interface LogTail {
	api_version: number;
	id: string;
	/** Log text. Empty while the child has written nothing. */
	log: string;
	/** Whether earlier lines were dropped. */
	truncated: boolean;
}

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
