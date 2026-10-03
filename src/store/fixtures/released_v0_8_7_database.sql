PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    name TEXT,
    workload_json TEXT NOT NULL,
    cwd TEXT NOT NULL,
    timeout_secs TEXT NOT NULL,
    env_path TEXT NOT NULL,
    env_home TEXT NOT NULL,
    binary TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_reason TEXT,
    callback_status TEXT NOT NULL,
    attention_state TEXT NOT NULL,
    timeout_notified_at TEXT,
    pid INTEGER,
    cancel_requested_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
, project_root TEXT, process_group_exit_evidence TEXT, container_exit_evidence TEXT);
INSERT INTO tasks VALUES('01a101c1-02f4-7023-b065-9d8753b97bab','01a0ab97-a7aa-7463-a5b0-8d500e40e431','fixture task','{"type":"task","command":["/bin/echo","fixture"]}','/home/fixture/work','3600','/home/fixture/.t3/userdata/device/bin:/Applications/Android Studio.app/Contents/jbr/Contents/Home/bin:/home/fixture/Library/Android/sdk/emulator:/home/fixture/Library/Android/sdk/platform-tools:/home/fixture/.atuin/bin:/home/fixture/.local/state/fnm_multishells/72989_1790992693623/bin:/home/fixture/Library/Application Support/ns/bin:/home/fixture/Library/pnpm:/opt/homebrew/opt/ruby/bin:/opt/homebrew/bin:/opt/homebrew/opt/llvm/bin:/home/fixture/.opencode/bin:/home/fixture/go/bin:/usr/local/opt/openssl@1.1/bin:/bin:/usr/local/sbin:/Applications/Sublime Text.app/Contents/SharedSupport/bin:/home/fixture/.local/bin:/opt/homebrew/sbin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/pkg/env/global/bin:/Library/Apple/usr/bin:/usr/local/MacGPG2/bin:/home/fixture/.codex/packages/standalone/releases/0.159.0-aarch64-apple-darwin/codex-path:/home/fixture/.local/state/fnm_multishells/37983_1790737290048/bin:/home/fixture/.codex/tmp/arg0/codex-arg0zsdEgB:/home/fixture/.t3/userdata/device/bin:/home/fixture/.local/state/fnm_multishells/36718_1790737289326/bin:/home/fixture/.local/state/fnm_multishells/36636_1790737288920/bin:/home/fixture/.cargo/bin:/home/fixture/.mix/escripts:/Applications/Postgres.app/Contents/Versions/latest/bin:/home/fixture/Library/Android/sdk/tools:/home/fixture/Library/Android/sdk/tools/bin:/home/fixture/.fnm/:/home/fixture/.t3/runtime/versions/0.0.43:/home/fixture/.t3/runtime/versions/0.0.44','/home/fixture','/bin/echo','succeeded','{"kind":"exit","code":0}','pending','pending',NULL,62291,NULL,'2026-10-03T12:33:08.853Z','2026-10-03T12:33:08.871Z','/home/fixture/code/homebased','confirmed_exited',NULL);
CREATE TABLE reports (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    summary TEXT NOT NULL,
    reported_at TEXT NOT NULL,
    notified_at TEXT,
    PRIMARY KEY (task_id, seq),
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);
CREATE TABLE report_notification_intents (
    task_id TEXT NOT NULL,
    report_seq INTEGER NOT NULL,
    requested_at TEXT NOT NULL,
    PRIMARY KEY (task_id, report_seq),
    FOREIGN KEY (task_id, report_seq) REFERENCES reports(task_id, seq)
);
CREATE TABLE origin_routes (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    execution_machine TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    route_json TEXT NOT NULL
);
INSERT INTO origin_routes VALUES('01a101c1-02f5-72eb-870a-843f0fccdf7e','01a101c1-02f4-7023-b065-9d8753b97bab','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"api_version":1,"thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","name":"fixture task","cwd":"/home/fixture/work","timeout":"1h","workload":{"type":"task","command":["/bin/echo","fixture"]}}','{"request":"01a101c1-02f5-72eb-870a-843f0fccdf7e","task":"01a101c1-02f4-7023-b065-9d8753b97bab","origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","callback":{"env":{"path":"/home/fixture/.t3/userdata/device/bin:/Applications/Android Studio.app/Contents/jbr/Contents/Home/bin:/home/fixture/Library/Android/sdk/emulator:/home/fixture/Library/Android/sdk/platform-tools:/home/fixture/.atuin/bin:/home/fixture/.local/state/fnm_multishells/72989_1790992693623/bin:/home/fixture/Library/Application Support/ns/bin:/home/fixture/Library/pnpm:/opt/homebrew/opt/ruby/bin:/opt/homebrew/bin:/opt/homebrew/opt/llvm/bin:/home/fixture/.opencode/bin:/home/fixture/go/bin:/usr/local/opt/openssl@1.1/bin:/bin:/usr/local/sbin:/Applications/Sublime Text.app/Contents/SharedSupport/bin:/home/fixture/.local/bin:/opt/homebrew/sbin:/usr/local/bin:/System/Cryptexes/App/usr/bin:/usr/bin:/usr/sbin:/sbin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/local/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/bin:/var/run/com.apple.security.cryptexd/codex.system/bootstrap/usr/appleinternal/bin:/pkg/env/global/bin:/Library/Apple/usr/bin:/usr/local/MacGPG2/bin:/home/fixture/.codex/packages/standalone/releases/0.159.0-aarch64-apple-darwin/codex-path:/home/fixture/.local/state/fnm_multishells/37983_1790737290048/bin:/home/fixture/.codex/tmp/arg0/codex-arg0zsdEgB:/home/fixture/.t3/userdata/device/bin:/home/fixture/.local/state/fnm_multishells/36718_1790737289326/bin:/home/fixture/.local/state/fnm_multishells/36636_1790737288920/bin:/home/fixture/.cargo/bin:/home/fixture/.mix/escripts:/Applications/Postgres.app/Contents/Versions/latest/bin:/home/fixture/Library/Android/sdk/tools:/home/fixture/Library/Android/sdk/tools/bin:/home/fixture/.fnm/:/home/fixture/.t3/runtime/versions/0.0.43:/home/fixture/.t3/runtime/versions/0.0.44","home":"/home/fixture"},"cwd":"/home/fixture/work","codex":{"type":"available","path":"/usr/bin/true"}},"spec":{"api_version":1,"thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","name":"fixture task","cwd":"/home/fixture/work","timeout":"1h","workload":{"type":"task","command":["/bin/echo","fixture"]}},"submission":{"type":"accepted"},"last_execution_state":"succeeded","last_updated_at":"2026-10-03T12:33:09.229492Z","last_accepted_seq":3,"last_settled_seq":3}');
CREATE TABLE executor_identities (
    task_id TEXT PRIMARY KEY,
    origin_machine TEXT NOT NULL,
    identity_json TEXT NOT NULL
);
INSERT INTO executor_identities VALUES('01a101c1-02f4-7023-b065-9d8753b97bab','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"type":"accepted","task":"01a101c1-02f4-7023-b065-9d8753b97bab","origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","spec":{"api_version":1,"thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","name":"fixture task","cwd":"/home/fixture/work","timeout":"1h","workload":{"type":"task","command":["/bin/echo","fixture"]}},"state":"succeeded"}');
CREATE TABLE executor_outbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    origin_machine TEXT NOT NULL,
    execution_machine TEXT NOT NULL,
    event_json TEXT NOT NULL,
    notification_required INTEGER NOT NULL CHECK (notification_required IN (0, 1)),
    state TEXT NOT NULL CHECK (state IN ('pending', 'acknowledged')),
    acknowledged_at TEXT,
    PRIMARY KEY (task_id, seq)
);
INSERT INTO executor_outbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',1,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":1,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"state","status":"queued"}}',0,'acknowledged','2026-10-03T12:33:09.228Z');
INSERT INTO executor_outbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',2,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":2,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"state","status":"running"}}',0,'acknowledged','2026-10-03T12:33:09.228Z');
INSERT INTO executor_outbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',3,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":3,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"callback","event":{"api_version":1,"event":"TASK_SUCCEEDED","task":"01a101c1-02f4-7023-b065-9d8753b97bab","name":"fixture task","display_name":"fixture task","workload":{"type":"task","command":["/bin/echo","fixture"]},"thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","cwd":"/home/fixture/work","evidence":"/home/fixture/work/state/tasks/01a101c1-02f4-7023-b065-9d8753b97bab","reports":[],"process":{"kind":"exit","code":0},"next_action":"review_output"},"state":"succeeded"}}',1,'acknowledged','2026-10-03T12:33:09.229Z');
CREATE TABLE executor_event_cursors (
    task_id TEXT PRIMARY KEY,
    last_seq INTEGER NOT NULL CHECK (last_seq >= 0)
);
INSERT INTO executor_event_cursors VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',3);
CREATE TABLE executor_event_routes (
    task_id TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK (state = 'orphaned'),
    reason TEXT NOT NULL
);
CREATE TABLE origin_inbox (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    origin_machine TEXT NOT NULL,
    execution_machine TEXT NOT NULL,
    event_json TEXT NOT NULL,
    notification_required INTEGER NOT NULL CHECK (notification_required IN (0, 1)),
    delivery_json TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (task_id, seq)
);
INSERT INTO origin_inbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',1,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":1,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"state","status":"queued"}}',0,'{"type":"not_required"}','2026-10-03T12:33:09.227Z');
INSERT INTO origin_inbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',2,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":2,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"state","status":"running"}}',0,'{"type":"not_required"}','2026-10-03T12:33:09.228Z');
INSERT INTO origin_inbox VALUES('01a101c1-02f4-7023-b065-9d8753b97bab',3,'01a101c0-fc83-75f8-aba2-309288fa0c9d','01a101c0-fc83-75f8-aba2-309288fa0c9d','{"task":"01a101c1-02f4-7023-b065-9d8753b97bab","seq":3,"origin_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","execution_machine":"01a101c0-fc83-75f8-aba2-309288fa0c9d","payload":{"type":"callback","event":{"api_version":1,"event":"TASK_SUCCEEDED","task":"01a101c1-02f4-7023-b065-9d8753b97bab","name":"fixture task","display_name":"fixture task","workload":{"type":"task","command":["/bin/echo","fixture"]},"thread":"01a0ab97-a7aa-7463-a5b0-8d500e40e431","cwd":"/home/fixture/work","evidence":"/home/fixture/work/state/tasks/01a101c1-02f4-7023-b065-9d8753b97bab","reports":[],"process":{"kind":"exit","code":0},"next_action":"review_output"},"state":"succeeded"}}',1,'{"type":"delivered","attempts":1,"last_error":null}','2026-10-03T12:33:09.257Z');
CREATE TABLE executor_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    result_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);
CREATE TABLE origin_event_receipts (
    task_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_digest TEXT NOT NULL,
    delivery_json TEXT NOT NULL,
    terminal_callback INTEGER NOT NULL CHECK (terminal_callback IN (0, 1)),
    PRIMARY KEY (task_id, seq)
);
CREATE TABLE cancellation_requests (
    task_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL
);
CREATE TABLE executor_cancellations (
    cancellation_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    receipt_json TEXT NOT NULL
);
CREATE TABLE message_attempts (
    message_id TEXT PRIMARY KEY,
    attempt_json TEXT NOT NULL
);
CREATE TABLE message_receipts (
    message_id TEXT PRIMARY KEY REFERENCES message_attempts(message_id),
    receipt_json TEXT NOT NULL
);
CREATE TABLE outbound_message_bindings (
    message_id TEXT PRIMARY KEY,
    binding_json TEXT NOT NULL
);
CREATE TABLE task_containers (
    task_id TEXT PRIMARY KEY NOT NULL REFERENCES tasks(id),
    container_id TEXT CHECK (
        container_id IS NULL
        OR (length(container_id) = 64 AND container_id NOT GLOB '*[^0-9a-f]*')
    ),
    started_at TEXT,
    adoptions INTEGER NOT NULL DEFAULT 0 CHECK (adoptions >= 0)
);
CREATE TABLE resources (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    authority_machine TEXT NOT NULL,
    supervisor_machine TEXT NOT NULL,
    supervisor_thread TEXT NOT NULL,
    assignment_revision INTEGER NOT NULL CHECK (assignment_revision >= 0),
    state_revision INTEGER NOT NULL CHECK (state_revision >= 0),
    registered_background_task TEXT
);
CREATE TABLE trainer_attempt_associations (
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
CREATE TABLE resource_requests (
    acceptance_sequence INTEGER PRIMARY KEY AUTOINCREMENT CHECK (acceptance_sequence > 0),
    queue_rank INTEGER NOT NULL CHECK (queue_rank > 0),
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
CREATE TABLE resource_request_preventions (
    request_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL UNIQUE,
    resource_id TEXT NOT NULL,
    origin_machine TEXT NOT NULL
);
CREATE TABLE resource_cancellation_receipts (
    cancellation_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL CHECK (json_valid(request_json)),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_extract(receipt_json, '$.cancellation') = cancellation_id, 0)
    )
);
CREATE TABLE loans (
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
CREATE TABLE resource_supervisor_notices (
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
CREATE TABLE resource_release_completions (
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
CREATE TABLE resource_task_completions (
    task_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.task_id') = task_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.request_id') = request_id, 0)
    )
);
CREATE TABLE resource_release_checkpoint_states (
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
CREATE TABLE resource_return_decisions (
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
CREATE TABLE resource_return_windows (
    action_id TEXT PRIMARY KEY,
    loan_id TEXT NOT NULL REFERENCES loans(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    window_json TEXT NOT NULL CHECK (
        json_valid(window_json)
        AND COALESCE(json_type(window_json) = 'object', 0)
        AND COALESCE(json_extract(window_json, '$.action_id') = action_id, 0)
        AND COALESCE(json_extract(window_json, '$.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(window_json, '$.resource_id') = resource_id, 0)
        AND COALESCE(json_type(window_json, '$.opened_at') = 'text', 0)
        AND COALESCE(json_type(window_json, '$.deadline_at') = 'text', 0)
    )
);
CREATE TABLE resource_return_deadline_servings (
    action_id TEXT PRIMARY KEY REFERENCES resource_return_windows(action_id),
    loan_id TEXT NOT NULL REFERENCES loans(id),
    resource_id TEXT NOT NULL REFERENCES resources(id),
    receipt_json TEXT NOT NULL CHECK (
        json_valid(receipt_json)
        AND COALESCE(json_type(receipt_json) = 'object', 0)
        AND COALESCE(json_extract(receipt_json, '$.window.action_id') = action_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.window.loan_id') = loan_id, 0)
        AND COALESCE(json_extract(receipt_json, '$.window.resource_id') = resource_id, 0)
        AND COALESCE(json_type(receipt_json, '$.return_context') = 'object', 0)
        AND COALESCE(json_type(receipt_json, '$.request_id') = 'text', 0)
    )
);
CREATE TABLE resource_restore_closures (
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
CREATE TABLE resource_action_task_receipts (
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
CREATE TABLE resource_background_launches (
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
CREATE TABLE resource_idle_openings (
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
CREATE TABLE resource_control_operations (
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
CREATE TABLE resource_operator_attestations (
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
CREATE TABLE resource_initial_idle_attestations (
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
CREATE TABLE resource_registration_receipts (
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
DELETE FROM sqlite_sequence;
CREATE INDEX tasks_status ON tasks(status);
CREATE INDEX tasks_thread ON tasks(thread_id);
CREATE INDEX executor_outbox_pending ON executor_outbox(state, task_id, seq);
CREATE INDEX executor_outbox_retention ON executor_outbox(acknowledged_at, task_id, seq);
CREATE INDEX origin_inbox_order ON origin_inbox(task_id, seq);
CREATE INDEX origin_inbox_retention ON origin_inbox(settled_at, task_id, seq);
CREATE INDEX executor_cancellations_task ON executor_cancellations(task_id);
CREATE INDEX trainer_attempt_associations_resource_task
    ON trainer_attempt_associations(resource_id, task_id);
CREATE INDEX resource_requests_serving_order
    ON resource_requests(resource_id, queue_rank, acceptance_sequence);
CREATE INDEX resource_requests_queued_serving_order
    ON resource_requests(resource_id, queue_rank, acceptance_sequence)
    WHERE json_extract(state_json, '$.type') = 'queued';
CREATE UNIQUE INDEX loans_one_non_closed_per_resource
    ON loans(resource_id)
    WHERE json_extract(state_json, '$.type') != 'closed';
CREATE INDEX resource_release_checkpoint_states_resource
    ON resource_release_checkpoint_states(resource_id, action_id);
CREATE INDEX resource_background_launches_resource
    ON resource_background_launches(resource_id);
CREATE INDEX resource_supervisor_notices_pending
    ON resource_supervisor_notices(id)
    WHERE json_extract(notice_json, '$.delivery.type') IN ('pending', 'retry_pending');
CREATE INDEX resource_operator_attestations_resource
    ON resource_operator_attestations(resource_id);
COMMIT;
