// Typed client for the daemon read API (`GET /v1/*`). The shapes mirror the
// serde views in `src/daemon/api/views.rs`; keep both sides in step.

/** Schema version this client is written against. */
export const API_VERSION = 1;

/** Every request is answered from loopback, so a slow answer means trouble. */
const REQUEST_TIMEOUT_MS = 5000;

const API_BASE = '/v1';

/** Process lifecycle, `ProcessStatus` in the daemon. */
export type ProcessStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled' | 'lost';

/** Every status, in lifecycle order. */
export const PROCESS_STATUSES: readonly ProcessStatus[] = [
	'queued',
	'running',
	'succeeded',
	'failed',
	'cancelled',
	'lost'
];

/** Statuses of a worker that can still change on its own. */
export const IN_FLIGHT_STATUSES: readonly ProcessStatus[] = ['queued', 'running'];

/** Narrow a URL or user supplied string to a known status. */
export function isProcessStatus(value: string): value is ProcessStatus {
	return (PROCESS_STATUSES as readonly string[]).includes(value);
}

/** Whether the worker can still change status on its own. */
export function isInFlight(status: ProcessStatus): boolean {
	return status === 'queued' || status === 'running';
}

/** Agent CLI that runs the task. */
export type AgentKind = 'codex' | 'claude' | 'grok';

/** Callback delivery state. Outlives the process state. */
export type CallbackStatus = 'pending' | 'sending' | 'sent' | 'failed';

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
	{ type: 'agent'; agent: AgentKind; model: string | null } | { type: 'task'; command: string[] };

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
	/** Submitted name. Omitted only for tasks stored before name was required. */
	name?: string | null;
	/** Non-empty server-derived label. */
	display_name: string;
	status: ProcessStatus;
	workload: WorkloadView;
	/** Submitting Codex thread. */
	thread: string;
	cwd: string;
	/** Worker pid while running. */
	pid: number | null;
	callback: CallbackStatus;
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
	entries: FileEntry[];
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
	reports: TaskReport[];
	/** Combined stdout and stderr of the child. */
	output_log: string;
	/** Task directory. */
	evidence: string;
	last_event: TaskEvent | null;
}

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
	return getJson<DaemonStatus>('/status');
}

/** Filter for `GET /v1/tasks`. */
export interface TaskQuery {
	statuses?: readonly ProcessStatus[];
	thread?: string | null;
}

/**
 * `GET /v1/tasks`, newest first. The daemon answers in id order and ids are
 * UUID v7, so reversing is a time sort.
 */
export async function fetchTasks(query: TaskQuery = {}): Promise<TaskSummary[]> {
	const params = new URLSearchParams();
	if (query.statuses?.length) params.set('status', query.statuses.join(','));
	if (query.thread) params.set('thread', query.thread);
	const search = params.size > 0 ? `?${params}` : '';
	const body = await getJson<{ tasks: TaskSummary[] }>(`/tasks${search}`);
	return body.tasks.reverse();
}

/** `GET /v1/tasks/{id}`. */
export function fetchTask(id: string): Promise<TaskDetail> {
	return getJson<TaskDetail>(`/tasks/${encodeURIComponent(id)}`);
}

/** `GET /v1/tasks/{id}/log?tail=N`. */
export function fetchLogTail(id: string, tail: number): Promise<LogTail> {
	return getJson<LogTail>(`/tasks/${encodeURIComponent(id)}/log?tail=${tail}`);
}

/** `POST /v1/files/resolve`. */
export async function resolvePath(path: string): Promise<ResolvedPath> {
	return postJson<ResolvedPath>('/files/resolve', { path });
}

/** `GET /v1/files/{token}`. */
export function fetchDirectory(token: string): Promise<DirectoryListing> {
	return getJson<DirectoryListing>(`/files/${encodeURIComponent(token)}`);
}

/** `GET /v1/files/origin`. */
export function fetchContentOrigin(): Promise<ContentOrigin> {
	return getJson<ContentOrigin>('/files/origin');
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

async function getJson<T>(path: string): Promise<T> {
	return requestJson<T>(path, { method: 'GET' });
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
	return requestJson<T>(path, {
		method: 'POST',
		headers: { 'content-type': 'application/json' },
		body: JSON.stringify(body)
	});
}

async function requestJson<T>(path: string, init: RequestInit): Promise<T> {
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
	return (await response.json()) as T;
}

async function errorFromResponse(response: Response): Promise<ApiError> {
	const fallback: ApiErrorBody = {
		code: 'http_error',
		message: `${response.status} ${response.statusText}`.trim(),
		retryable: response.status >= 500,
		input: {}
	};
	try {
		const body = (await response.json()) as { error?: Partial<ApiErrorBody> };
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
