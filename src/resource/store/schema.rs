//! SQLite schema for authority-local resource state

/// Resource tables installed by the Store migration hook
pub(crate) const RESOURCE_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS resources (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    authority_machine TEXT NOT NULL,
    supervisor_machine TEXT NOT NULL,
    supervisor_thread TEXT NOT NULL,
    assignment_revision INTEGER NOT NULL CHECK (assignment_revision >= 0),
    state_revision INTEGER NOT NULL CHECK (state_revision >= 0),
    registered_background_task TEXT
);

CREATE TABLE IF NOT EXISTS trainer_attempt_associations (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    authority_machine TEXT NOT NULL,
    association_json TEXT NOT NULL CHECK (
        json_valid(association_json)
        AND COALESCE(json_type(association_json) = 'object', 0)
        AND COALESCE(json_extract(association_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(association_json, '$.authority_machine') = authority_machine, 0)
        AND COALESCE(json_extract(association_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_type(association_json, '$.canonical_runtime_root') = 'text', 0)
        AND COALESCE(json_type(association_json, '$.attempt_binding') = 'object', 0)
        AND COALESCE(json_type(association_json, '$.request_sha256') = 'text', 0)
        AND COALESCE(json_type(association_json, '$.ownership_lock_identity') = 'object', 0)
        AND COALESCE(json_type(association_json, '$.normalized_spec_sha256') = 'text', 0)
    )
);

CREATE INDEX IF NOT EXISTS trainer_attempt_associations_resource_task
    ON trainer_attempt_associations(resource_id, task_id);

CREATE TABLE IF NOT EXISTS resource_requests (
    acceptance_sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK (acceptance_sequence > 0),
    request_id TEXT NOT NULL UNIQUE,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    origin_machine TEXT NOT NULL,
    spec_json TEXT NOT NULL CHECK (
        json_valid(spec_json)
        AND COALESCE(json_type(spec_json) = 'object', 0)
        AND COALESCE(json_type(spec_json, '$.api_version') = 'integer', 0)
        AND COALESCE(json_type(spec_json, '$.thread') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.name') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.cwd') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.timeout') = 'text', 0)
        AND COALESCE(json_type(spec_json, '$.workload') = 'object', 0)
        AND COALESCE(
            (
                json_extract(spec_json, '$.workload.type') = 'task'
                AND json_type(spec_json, '$.workload.command') = 'array'
            ) OR (
                json_extract(spec_json, '$.workload.type') = 'container'
                AND json_type(spec_json, '$.workload.image') = 'text'
                AND json_type(spec_json, '$.workload.gpus') IS NOT NULL
            ),
            0
        )
    ),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.type') = 'text', 0)
        AND COALESCE(json_extract(state_json, '$.type') IN (
            'queued', 'assigned', 'finished', 'cancelled_before_launch', 'rejected'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_requests_fifo
    ON resource_requests(resource_id, acceptance_sequence);
CREATE INDEX IF NOT EXISTS resource_requests_queued_fifo
    ON resource_requests(resource_id, acceptance_sequence)
    WHERE json_extract(state_json, '$.type') = 'queued';

CREATE TABLE IF NOT EXISTS resource_request_preventions (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    origin_machine TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS resource_cancellation_receipts (
    cancellation_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_extract(receipt_json, '$.cancellation') = cancellation_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS loans (
    id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.type') = 'text', 0)
        AND COALESCE(json_extract(state_json, '$.type') IN (
            'active', 'needs_attention', 'closed'
        ), 0)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS loans_one_non_closed_per_resource
    ON loans(resource_id)
    WHERE json_extract(state_json, '$.type') != 'closed';

CREATE TABLE IF NOT EXISTS resource_supervisor_notices (
    id TEXT PRIMARY KEY,
    loan_id TEXT NOT NULL REFERENCES loans(id),
    action_id TEXT NOT NULL UNIQUE,
    notice_json TEXT NOT NULL CHECK (
        json_valid(notice_json)
        AND COALESCE(json_type(notice_json) = 'object', 0)
        AND COALESCE(json_type(notice_json, '$.id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.loan_id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.action_id') = 'text', 0)
        AND COALESCE(json_type(notice_json, '$.state_revision') = 'integer', 0)
        AND COALESCE(json_type(notice_json, '$.destination') = 'object', 0)
        AND COALESCE(json_type(notice_json, '$.assignment_revision') = 'integer', 0)
        AND COALESCE(json_type(notice_json, '$.payload') = 'object', 0)
        AND COALESCE(json_extract(notice_json, '$.payload.type') IN (
            'release_required', 'return_required', 'attention_required'
        ), 0)
        AND COALESCE(json_type(notice_json, '$.delivery') = 'object', 0)
        AND COALESCE(json_extract(notice_json, '$.delivery.type') IN (
            'pending', 'retry_pending', 'sending', 'delivered', 'failed'
        ), 0)
        AND COALESCE(json_extract(notice_json, '$.id') = id, 0)
        AND COALESCE(json_extract(notice_json, '$.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(notice_json, '$.action_id') = action_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_release_completions (
    action_id TEXT PRIMARY KEY,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_type(receipt_json, '$.action_id') = 'text', 0)
        AND COALESCE(json_extract(receipt_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_type(receipt_json, '$.authority_machine') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.resource_id') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.expected_state_revision') = 'integer', 0)
        AND COALESCE(json_type(receipt_json, '$.return_context') = 'object', 0)
        AND COALESCE(json_type(receipt_json, '$.result') = 'object', 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_task_completions (
    task_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_release_checkpoint_states (
    action_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    state_json TEXT NOT NULL CHECK (
        json_valid(state_json)
        AND COALESCE(json_type(state_json) = 'object', 0)
        AND COALESCE(json_type(state_json, '$.action') = 'object', 0)
        AND COALESCE(json_type(state_json, '$.phase') = 'object', 0)
        AND COALESCE(json_extract(state_json, '$.action.action_id') = action_id, 0)
        AND COALESCE(json_extract(state_json, '$.action.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(state_json, '$.phase.type') IN (
            'watcher_binding_pending', 'baseline_captured', 'stop_reserved', 'cancellation_committed'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_release_checkpoint_states_resource
    ON resource_release_checkpoint_states(resource_id, action_id);

CREATE TABLE IF NOT EXISTS resource_return_decisions (
    action_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    loan_id TEXT NOT NULL REFERENCES loans(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.loan_id') = loan_id, 0)
        AND COALESCE(json_type(receipt_json, '$.decision') = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.result.type') IN (
            'closed', 'restore_bound'
        ), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_restore_closures (
    action_id TEXT PRIMARY KEY REFERENCES resource_return_decisions(action_id),
    task_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.basis.type') IN (
            'confirmed_running', 'foreground_ended', 'container_ended', 'supervisor_resolved_end'
        ), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_action_task_receipts (
    task_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    action_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.authority.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.kind') IN ('release_watcher', 'return'), 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_background_launches (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.contract') = 'object', 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_background_launches_resource
    ON resource_background_launches(resource_id);

CREATE TABLE IF NOT EXISTS resource_idle_openings (
    loan_id TEXT PRIMARY KEY REFERENCES loans(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.proof') = 'object', 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_supervisor_notices_pending
    ON resource_supervisor_notices(id)
    WHERE json_extract(notice_json, '$.delivery.type') IN ('pending', 'retry_pending');

CREATE TABLE IF NOT EXISTS resource_control_operations (
    operation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    request_json TEXT NOT NULL CHECK (
        json_valid(request_json)
        AND COALESCE(json_type(request_json) = 'object', 0)
        AND COALESCE(json_extract(request_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(request_json, '$.action') = 'object', 0)
    ),
    attempt_id TEXT
);

CREATE TABLE IF NOT EXISTS resource_operator_attestations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    resource_id TEXT NOT NULL REFERENCES resources(id),
    task_id TEXT NOT NULL UNIQUE,
    preceding_loan TEXT,
    preceding_launch TEXT,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.operation_id') = operation_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.resource_id') = resource_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.task_id') = task_id, 0)
        AND COALESCE(
            json_extract(receipt_json, '$.attestation.confirmation') = 'operator_confirmed_gpu_free',
            0
        )
        AND COALESCE(length(trim(json_extract(receipt_json, '$.attestation.observation'))) > 0, 0)
        AND COALESCE(json_type(receipt_json, '$.evidence') = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.outcome.type') IN (
            'release_resolved_serving', 'release_resolved_return_required',
            'idle_serving', 'idle_boundary',
            'restore_closed_serving', 'restore_closed_idle_boundary'
        ), 0)
    )
);

CREATE INDEX IF NOT EXISTS resource_operator_attestations_resource
    ON resource_operator_attestations(resource_id);

CREATE TABLE IF NOT EXISTS resource_initial_idle_attestations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    resource_id TEXT NOT NULL UNIQUE REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.operation_id') = operation_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.attestation.resource_id') = resource_id, 0)
        AND COALESCE(
            json_extract(receipt_json, '$.attestation.confirmation') = 'operator_confirmed_gpu_free',
            0
        )
        AND COALESCE(length(trim(json_extract(receipt_json, '$.attestation.observation'))) > 0, 0)
        AND COALESCE(json_type(receipt_json, '$.state_revision') = 'integer', 0)
    )
);

CREATE TABLE IF NOT EXISTS resource_registration_receipts (
    resource_id TEXT PRIMARY KEY NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.display_name') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.authority_machine') = 'text', 0)
        AND COALESCE(json_type(receipt_json, '$.initial_supervisor') = 'object', 0)
    )
);
";
