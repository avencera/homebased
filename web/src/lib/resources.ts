// Resource API types are intentionally limited to fields shown by the dashboard.
// Tagged domain records stay open so a newer enum variant remains visible as unknown.

import { Schema } from 'effect';
import { API_VERSION, ApiError, getJsonBody, postJsonBody } from './api';
import type { BrowserResourceAction } from './resource-actions';

export type { BrowserResourceAction, QueuePlacement } from './resource-actions';

const JsonObjectSchema = Schema.Record({ key: Schema.String, value: Schema.Unknown });

const SupervisorSchema = Schema.Struct({
	machine: Schema.String,
	thread: Schema.String
});

const ResourceSchema = Schema.Struct({
	id: Schema.String,
	display_name: Schema.String,
	authority_machine: Schema.String,
	supervisor: SupervisorSchema,
	assignment_revision: Schema.Finite,
	state_revision: Schema.Finite,
	registered_background_task: Schema.NullOr(Schema.String)
});

const ResourceTaskSchema = Schema.Struct({
	id: Schema.String,
	display_name: Schema.String,
	status: Schema.String,
	/** Submitting thread. Older authorities omit it. */
	thread: Schema.optional(Schema.String),
	origin_machine: Schema.optional(Schema.NullOr(Schema.String)),
	execution_machine: Schema.optional(Schema.NullOr(Schema.String)),
	created_at: Schema.optional(Schema.String),
	updated_at: Schema.optional(Schema.String),
	cancel_requested_at: Schema.optional(Schema.NullOr(Schema.String)),
	available: Schema.optional(Schema.Unknown),
	availability: Schema.optional(Schema.Unknown)
});

const AttentionSchema = Schema.Struct({
	code: Schema.String,
	message: Schema.String
});

const LoanSchema = Schema.Struct({
	id: Schema.String,
	resource_id: Schema.String,
	state: JsonObjectSchema
});

const ResourceRequestSchema = Schema.Struct({
	request_id: Schema.String,
	task_id: Schema.String,
	acceptance_sequence: Schema.Finite,
	origin_machine: Schema.String,
	/** Requesting thread, which runs on `origin_machine`. Older authorities omit it. */
	thread: Schema.optional(Schema.String),
	display_name: Schema.String,
	state: JsonObjectSchema
});

const SupervisorNoticeSchema = Schema.Struct({
	id: Schema.String,
	loan_id: Schema.String,
	action_id: Schema.String,
	state_revision: Schema.Finite,
	destination: SupervisorSchema,
	assignment_revision: Schema.Finite,
	payload: JsonObjectSchema,
	delivery: JsonObjectSchema
});

// the status stays a string so a newer reservation reason is shown as reserved, not dropped
const BackgroundLaunchSchema = Schema.Struct({
	request_id: Schema.String,
	task_id: Schema.String,
	status: Schema.String
});

const ReturnWindowSchema = Schema.Struct({
	action_id: Schema.String,
	opened_at: Schema.String,
	deadline_at: Schema.String
});

const ReturnExecutionModeSchema = Schema.Literal(
	'direct_segment_trainer',
	'native_foreground',
	'container'
);

const ResourceOverviewItemSchema = Schema.Struct({
	resource: ResourceSchema,
	loan: Schema.NullOr(LoanSchema),
	queued_count: Schema.Finite,
	current_task: Schema.NullOr(ResourceTaskSchema),
	attention: Schema.NullOr(AttentionSchema),
	background_launch: Schema.optional(BackgroundLaunchSchema),
	return_window: Schema.optional(ReturnWindowSchema)
});

const ResourceOverviewSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	resources: Schema.Array(ResourceOverviewItemSchema),
	unavailable_authorities: Schema.Array(JsonObjectSchema)
});

const ResourceDetailSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	resource: ResourceSchema,
	loan: Schema.NullOr(LoanSchema),
	requests: Schema.Array(ResourceRequestSchema),
	notices: Schema.Array(SupervisorNoticeSchema),
	current_task: Schema.NullOr(ResourceTaskSchema),
	background_task: Schema.NullOr(ResourceTaskSchema),
	current_task_id: Schema.optional(Schema.NullOr(Schema.String)),
	background_task_id: Schema.optional(Schema.NullOr(Schema.String)),
	attention: Schema.NullOr(AttentionSchema),
	background_launch: Schema.optional(BackgroundLaunchSchema),
	return_execution_mode: Schema.optional(ReturnExecutionModeSchema),
	return_window: Schema.optional(ReturnWindowSchema)
});

const PendingActionSchema = Schema.Struct({
	resource_id: Schema.String,
	loan_id: Schema.String,
	action_id: Schema.String,
	state_revision: Schema.Finite,
	supervisor: SupervisorSchema,
	phase: Schema.Unknown,
	return_context: Schema.optional(Schema.Unknown),
	notice: Schema.optional(Schema.Unknown)
});

const PendingActionsSchema = Schema.Struct({
	api_version: Schema.Literal(API_VERSION),
	actions: Schema.Array(PendingActionSchema),
	unavailable_authorities: Schema.optional(Schema.Array(JsonObjectSchema))
});

/** Resource queue state, tagged by `type` in the daemon response. */
export type ResourceRequestState = Schema.Schema.Type<typeof ResourceRequestSchema>['state'];

/** First background launch that reserves an unregistered resource. */
export type BackgroundLaunchReservation = Schema.Schema.Type<typeof BackgroundLaunchSchema>;

/** Time the supervisor has to decide a return before queued work takes the resource. */
export type ReturnWindow = Schema.Schema.Type<typeof ReturnWindowSchema>;

/** Accepted execution mode exposed for the exact current Restoring loan. */
export type ReturnExecutionMode = Schema.Schema.Type<typeof ReturnExecutionModeSchema>;

/** Displayable portion of a Homebased task summary. */
export type ResourceTaskSummary = Schema.Schema.Type<typeof ResourceTaskSchema>;

/** Resource header and current loan state. */
export type ResourceOverviewItem = Schema.Schema.Type<typeof ResourceOverviewItemSchema>;

/** Full dashboard detail for one resource. */
export type ResourceDetail = Schema.Schema.Type<typeof ResourceDetailSchema>;

/** Pending supervisor action returned by the read API. */
export type PendingAction = Schema.Schema.Type<typeof PendingActionSchema>;

/** Pending actions and authority failures returned for one supervisor. */
export type PendingActionResult = Schema.Schema.Type<typeof PendingActionsSchema>;

/** Versioned resource list with authority failures kept separate from resources. */
export type ResourceOverview = Schema.Schema.Type<typeof ResourceOverviewSchema>;

/** Fetch and validate `GET /v1/resources`. */
export async function fetchResourceOverview(): Promise<ResourceOverview> {
	return decode(ResourceOverviewSchema, await getJsonBody('/resources'));
}

/** Fetch and validate `GET /v1/resources/{id}`. */
export async function fetchResourceDetail(id: string): Promise<ResourceDetail> {
	return decode(ResourceDetailSchema, await getJsonBody(`/resources/${encodeURIComponent(id)}`));
}

/** Fetch pending decisions for the exact supervisor assigned to a resource. */
export async function fetchPendingActions(
	machine: string,
	thread: string
): Promise<PendingActionResult> {
	const query = new URLSearchParams({ machine, thread });
	return decode(PendingActionsSchema, await getJsonBody(`/resources/pending?${query.toString()}`));
}

/** Submit one idempotent browser control and validate the authoritative detail response. */
export async function submitResourceAction(
	id: string,
	expectedRevision: number,
	operationId: string,
	action: BrowserResourceAction
): Promise<ResourceDetail> {
	const body = {
		api_version: API_VERSION,
		expected_revision: expectedRevision,
		operation_id: operationId,
		action
	};
	return decode(
		ResourceDetailSchema,
		await postJsonBody(`/resources/${encodeURIComponent(id)}/actions`, body)
	);
}

function decode<S extends Schema.Schema.AnyNoContext>(
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
				message: `resource API returned an invalid response: ${message}`,
				retryable: true,
				input: {}
			},
			200
		);
	}
}
