use super::{
    NormalizedWorkload, PromptSource, SubmitAgent, SubmitSpec, SubmitWorkloadValidated,
    default_timeout, normalize, parse_normalized_value, parse_spec_value, schema_json,
};
use crate::domain::{AgentKind, TaskName, ThreadId};
use crate::error::AppError;
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

fn example_agent_json() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "implement file browser",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {
            "type": "agent",
            "agent": "claude",
            "model": "fable",
            "prompt": "do the work",
            "extra_args": ["--verbose"]
        }
    })
}

fn example_task_json() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "cargo release build",
        "cwd": "/tmp",
        "timeout": "4h",
        "workload": {
            "type": "task",
            "command": ["cargo", "build", "--release"]
        }
    })
}

#[test]
fn absent_machine_keeps_local_spec_valid() {
    let spec = parse_spec_value(&valid_task()).unwrap();
    assert!(spec.machine.is_none());
    assert!(normalize(&spec).unwrap().machine.is_none());
}

#[test]
fn remote_prompt_file_must_be_absolute_on_origin() {
    let mut value = valid_agent();
    value["machine"] = json!("code");
    value["workload"].as_object_mut().unwrap().remove("prompt");
    value["workload"]["prompt_file"] = json!("prompt.txt");
    let spec = parse_spec_value(&value).unwrap();
    assert!(matches!(
        normalize(&spec),
        Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt_file"
    ));
}

#[test]
fn blank_prompt_rejected() {
    let mut value = valid_agent();
    value["workload"]["prompt"] = json!(" \n");
    let spec = parse_spec_value(&value).unwrap();
    assert!(matches!(
        normalize(&spec),
        Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt"
    ));

    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("prompt.md");
    fs::write(&file, "\n").unwrap();
    value["workload"].as_object_mut().unwrap().remove("prompt");
    value["workload"]["prompt_file"] = json!(file);
    let spec = parse_spec_value(&value).unwrap();
    assert!(matches!(
        normalize(&spec),
        Err(AppError::InvalidSpec { pointer, .. }) if pointer == "/workload/prompt_file"
    ));
}

fn valid_agent() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "test agent",
        "cwd": "/tmp",
        "workload": {
            "type": "agent",
            "agent": "claude",
            "prompt": "hello"
        }
    })
}

fn valid_task() -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "test task",
        "cwd": "/tmp",
        "workload": {
            "type": "task",
            "command": ["echo", "hi"]
        }
    })
}

fn valid_opencode(model: Option<&str>) -> Value {
    let mut value = valid_agent();
    value["workload"]["agent"] = json!("opencode");
    if let Some(model) = model {
        value["workload"]["model"] = json!(model);
    } else {
        value["workload"].as_object_mut().unwrap().remove("model");
    }
    value
}

#[track_caller]
fn after_error(after: Value) -> (String, Value) {
    let mut value = valid_task();
    value["after"] = after;
    match parse_spec_value(&value).unwrap_err() {
        AppError::InvalidSpec { pointer, value, .. } => (pointer, value),
        other => panic!("expected invalid_spec, got {other:?}"),
    }
}

#[test]
fn after_is_optional_sorted_and_kept_out_of_the_normalized_spec() {
    let spec = parse_spec_value(&valid_task()).unwrap();
    assert!(spec.after.is_none());

    let first = crate::domain::TaskId::new();
    let second = crate::domain::TaskId::new();
    let mut value = valid_task();
    value["after"] = json!([second, first]);
    let spec = parse_spec_value(&value).unwrap();
    assert_eq!(spec.after.as_ref().unwrap().tasks(), &[first, second]);
    let normalized = serde_json::to_value(normalize(&spec).unwrap()).unwrap();
    assert!(normalized.get("after").is_none());
}

#[test]
fn after_list_errors_point_at_the_offending_entry() {
    let (pointer, value) = after_error(json!([]));
    assert_eq!((pointer.as_str(), value), ("/after", json!([])));

    let task = crate::domain::TaskId::new();
    let (pointer, value) = after_error(json!([task, crate::domain::TaskId::new(), task]));
    assert_eq!((pointer.as_str(), value), ("/after/2", json!(task)));

    let many: Vec<_> = (0..=crate::dependency::MAX_DEPENDENCIES)
        .map(|_| crate::domain::TaskId::new())
        .collect();
    let (pointer, _) = after_error(json!(many));
    assert_eq!(pointer, "/after");

    let (pointer, value) = after_error(json!(["not-a-uuid"]));
    assert_eq!((pointer.as_str(), value), ("/after/0", json!("not-a-uuid")));
}

#[test]
fn unknown_field_rejected() {
    let mut value = valid_agent();
    value["typo"] = json!(true);
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/typo"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn both_prompt_and_file_rejected() {
    let mut value = valid_agent();
    value["workload"]["prompt_file"] = json!("/tmp/p.txt");
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/prompt"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn neither_prompt_rejected() {
    let mut value = valid_agent();
    value["workload"].as_object_mut().unwrap().remove("prompt");
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/prompt"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn cross_variant_agent_fields_on_task_rejected() {
    let mut value = valid_task();
    value["workload"]["agent"] = json!("claude");
    let err = parse_spec_value(&value).unwrap_err();
    assert!(matches!(err, AppError::InvalidSpec { .. }));
}

#[test]
fn empty_command_rejected() {
    let mut value = valid_task();
    value["workload"]["command"] = json!([]);
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => {
            assert!(pointer.starts_with("/workload/command"), "{pointer}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn empty_program_rejected() {
    let mut value = valid_task();
    value["workload"]["command"] = json!([""]);
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/workload/command/0"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn pointer_on_bad_array_element() {
    let mut value = valid_agent();
    value["workload"]["extra_args"] = json!([1]);
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec {
            pointer,
            value: field,
            ..
        } => {
            assert_eq!(pointer, "/workload/extra_args/0");
            assert_eq!(field, json!(1));
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn resume_thread_is_rejected_for_non_codex_agents() {
    let mut value = valid_agent();
    value["workload"]["resume_thread"] = json!("01a0e487-b877-76e2-9dc2-806bff0bf685");
    let err = parse_spec_value(&value).unwrap_err();
    assert!(matches!(
        err,
        AppError::InvalidSpec { pointer, value, .. }
            if pointer == "/workload/resume_thread"
                && value == json!("01a0e487-b877-76e2-9dc2-806bff0bf685")
    ));
}

#[test]
fn null_resume_thread_is_accepted_for_non_codex_agents() {
    let mut value = valid_agent();
    value["workload"]["resume_thread"] = Value::Null;
    let parsed = parse_spec_value(&value).unwrap();
    let SubmitWorkloadValidated::Agent(agent) = parsed.workload else {
        panic!("expected agent workload");
    };
    assert_eq!(agent.resume_thread, None);
}

#[test]
fn opencode_accepts_provider_qualified_models_and_preserves_variants() {
    for model in [
        Some("zai-coding-plan/glm-5.3-flash"),
        Some("other/provider#fast"),
        None,
    ] {
        let value = valid_opencode(model);
        let spec = parse_spec_value(&value).unwrap();
        let normalized = normalize(&spec).unwrap();
        let NormalizedWorkload::Agent(agent) = normalized.workload else {
            panic!("expected agent workload");
        };
        assert_eq!(agent.agent, AgentKind::OpenCode);
        assert_eq!(agent.model.as_deref(), model);
    }
}

#[test]
fn opencode_forbidden_extra_args_report_their_array_pointer() {
    for extra in [
        "--agent=other",
        "--dir=/other",
        "--server=http://localhost",
        "-c",
        "--session=session",
        "--fork",
        "--model=other/provider",
        "-m=other/provider",
        "--standalone=false",
        "prompt in argv",
    ] {
        let mut value = valid_opencode(None);
        value["workload"]["extra_args"] = json!([extra]);
        let err = parse_spec_value(&value).unwrap_err();
        match err {
            AppError::InvalidSpec { pointer, value, .. } => {
                assert_eq!(pointer, "/workload/extra_args/0");
                assert_eq!(value, json!(extra));
            }
            other => panic!("unexpected {other:?}"),
        }
    }
}

#[test]
fn existing_agent_extra_args_keep_their_previous_rules() {
    let mut value = valid_agent();
    value["workload"]["extra_args"] = json!(["free-form-value"]);
    assert!(parse_spec_value(&value).is_ok());
}

#[test]
fn relative_prompt_file_resolves_against_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let prompt_path = dir.path().join("p.txt");
    fs::write(&prompt_path, "from-file").unwrap();
    let spec = SubmitSpec {
        machine: None,
        api_version: 1,
        thread: ThreadId::from_str_ok(),
        name: TaskName::parse("from file").unwrap(),
        cwd: dir.path().to_path_buf(),
        timeout: default_timeout(),
        after: None,
        workload: SubmitWorkloadValidated::Agent(SubmitAgent {
            agent: AgentKind::Claude,
            model: None,
            prompt: PromptSource::File(PathBuf::from("p.txt")),
            extra_args: vec![],
            report_trailer: true,
            resume_thread: None,
        }),
    };
    let normalized = normalize(&spec).unwrap();
    match normalized.workload {
        NormalizedWorkload::Agent(agent) => assert_eq!(agent.prompt, "from-file"),
        other => panic!("unexpected {other:?}"),
    }
}

/// One published schema, compiled once per assertion set
fn validator() -> jsonschema::Validator {
    jsonschema::validator_for(&schema_json().unwrap()).unwrap()
}

/// The schema and the parser must reach the same verdict on every spec
/// Asserting both here is what stops the generated document from drifting
/// away from the runtime rules
#[track_caller]
fn assert_verdict(value: &Value, accepted: bool, why: &str) {
    let schema_ok = validator().is_valid(value);
    assert_eq!(
        schema_ok,
        accepted,
        "schema should {} {why}: {value}",
        if accepted { "accept" } else { "reject" }
    );
    let parser = parse_spec_value(value);
    assert_eq!(
        parser.is_ok(),
        accepted,
        "parser should {} {why}: {parser:?}",
        if accepted { "accept" } else { "reject" }
    );
}

fn with_workload(workload: Value) -> Value {
    json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "test agent",
        "cwd": "/tmp",
        "workload": workload
    })
}

#[test]
fn schema_accepts_both_documented_examples() {
    assert_verdict(&example_agent_json(), true, "the agent example");
    assert_verdict(&example_task_json(), true, "the task example");
}

#[test]
fn schema_pins_api_version_one() {
    let mut value = valid_task();
    value["api_version"] = json!(2);
    assert_verdict(&value, false, "api_version 2");
    let schema = schema_json().unwrap();
    assert_eq!(schema["properties"]["api_version"]["const"], json!(1));
}

#[test]
fn schema_requires_exactly_one_agent_prompt_source() {
    assert_verdict(
        &with_workload(json!({"type": "agent", "agent": "claude", "prompt": "hi"})),
        true,
        "an inline prompt",
    );
    assert_verdict(
        &with_workload(json!({"type": "agent", "agent": "claude", "prompt_file": "/tmp/p"})),
        true,
        "a prompt file",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "agent", "agent": "claude",
            "prompt": "hi", "prompt_file": "/tmp/p"
        })),
        false,
        "both prompt sources",
    );
    assert_verdict(
        &with_workload(json!({"type": "agent", "agent": "claude"})),
        false,
        "no prompt source",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "agent", "agent": "claude",
            "prompt": "hi", "prompt_file": null
        })),
        false,
        "a null prompt file beside an inline prompt",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "agent", "agent": "claude",
            "prompt": null, "prompt_file": "/tmp/p"
        })),
        false,
        "a null inline prompt beside a prompt file",
    );
}

#[test]
fn schema_requires_a_non_empty_command() {
    assert_verdict(
        &with_workload(json!({"type": "task", "command": ["cargo", "build"]})),
        true,
        "a normal command",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": ["echo", "", " "]})),
        true,
        "empty and whitespace arguments after the program",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": []})),
        false,
        "an empty command array",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": [""]})),
        false,
        "an empty program",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": ["", "build"]})),
        false,
        "an empty program with arguments",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": ["echo", "a\0b"]})),
        false,
        "a NUL byte in an argument",
    );
    assert_verdict(
        &with_workload(json!({"type": "task", "command": ["car\0go"]})),
        false,
        "a NUL byte in the program",
    );
}

#[test]
fn schema_rejects_cross_variant_fields() {
    assert_verdict(
        &with_workload(json!({
            "type": "task", "command": ["true"], "agent": "claude"
        })),
        false,
        "an agent field on a task",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "task", "command": ["true"], "prompt": "hi"
        })),
        false,
        "a prompt on a task",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "task", "command": ["true"], "report_trailer": true
        })),
        false,
        "a trailer flag on a task",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "agent", "agent": "claude", "prompt": "hi", "command": ["true"]
        })),
        false,
        "a command on an agent",
    );
}

#[test]
fn schema_rejects_an_unknown_variant_and_unknown_keys() {
    assert_verdict(
        &with_workload(json!({"type": "shell", "command": ["true"]})),
        false,
        "an unknown workload type",
    );
    assert_verdict(
        &with_workload(json!({
            "type": "agent", "agent": "claude", "prompt": "hi", "typo": 1
        })),
        false,
        "an unknown agent key",
    );
    assert_verdict(
        &with_workload(json!({"type": "agent", "agent": "gemini", "prompt": "hi"})),
        false,
        "an unsupported agent kind",
    );
}

#[test]
fn schema_and_parser_agree_on_container_workloads() {
    let digest = format!("sha256:{}", "0".repeat(64));
    let container = |extra: Value| {
        let mut workload = json!({
            "type": "container",
            "image": format!("eval@{digest}"),
            "memory": "1g"
        });
        if let (Some(workload), Value::Object(extra)) = (workload.as_object_mut(), extra) {
            workload.extend(extra);
        }
        with_workload(workload)
    };
    for (extra, why) in [
        (json!({}), "a minimal container"),
        (json!({ "image": digest }), "an image ID"),
        (
            json!({ "image": format!("misc.local:5000/team/eval-probe@{digest}") }),
            "a registry image",
        ),
        (
            json!({
                "entrypoint": ["/usr/bin/python3", "-m", "eval"],
                "args": ["--ckpt", "/data/ckpt", ""],
                "gpus": [0, 1],
                "memory": 25_769_803_776_u64,
                "user": "1000:1000",
                "workdir": "/work",
                "mounts": [{ "source": "/shared/ckpt", "target": "/data", "read_only": true }],
                "env": { "HF_HOME": "/data/hf" }
            }),
            "every field",
        ),
        (json!({ "gpus": "all" }), "all GPUs"),
    ] {
        assert_verdict(&container(extra), true, why);
    }
    for (extra, why) in [
        (json!({ "image": "eval:latest" }), "a tag alone"),
        (
            json!({ "image": format!("eval:1.0@{digest}") }),
            "a tag beside a digest",
        ),
        (json!({ "memory": null }), "a null memory limit"),
        (json!({ "privileged": true }), "privileged mode"),
        (json!({ "pid": "host" }), "a host PID namespace"),
        (json!({ "restart": "always" }), "a restart policy"),
        (json!({ "command": ["python"] }), "a command field"),
        (json!({ "gpus": [] }), "an empty GPU list"),
        (json!({ "gpus": [0, 0] }), "a repeated GPU"),
        (json!({ "gpus": null }), "a null GPU request"),
        (json!({ "entrypoint": [] }), "an empty entrypoint"),
        (json!({ "args": "python eval.py" }), "a shell string"),
        (json!({ "user": "root" }), "a named user"),
        (json!({ "workdir": "work" }), "a relative workdir"),
        (
            json!({ "env": { "1X": "v" } }),
            "an invalid environment name",
        ),
        (
            json!({ "mounts": [{ "source": "data", "target": "/d" }] }),
            "a relative mount source",
        ),
        (
            json!({ "mounts": [{ "source": "/data", "target": "/d", "propagation": "shared" }] }),
            "an unknown mount option",
        ),
    ] {
        assert_verdict(&container(extra), false, why);
    }
    let mut missing_memory = container(json!({}));
    missing_memory["workload"]
        .as_object_mut()
        .unwrap()
        .remove("memory");
    assert_verdict(&missing_memory, false, "a missing memory limit");
}

#[test]
fn container_workloads_normalize_and_survive_the_socket() {
    let digest = format!("sha256:{}", "0".repeat(64));
    let spec = parse_spec_value(&with_workload(json!({
        "type": "container",
        "image": digest,
        "memory": "2g",
        "gpus": [1],
        "env": { "B": "2", "A": "1" }
    })))
    .unwrap();
    let normalized = normalize(&spec).unwrap();
    let mut body = serde_json::to_value(&normalized).unwrap();
    assert_eq!(body["workload"]["type"], "container");
    assert_eq!(body["workload"]["memory"], json!(2_u64 << 30));
    assert_eq!(parse_normalized_value(&body).unwrap(), normalized);
    body["workload"]["privileged"] = json!(true);
    let err = parse_normalized_value(&body).unwrap_err();
    assert!(
        matches!(&err, AppError::InvalidSpec { pointer, .. } if pointer == "/workload/privileged"),
        "{err:?}"
    );
}

#[test]
fn default_timeout_is_one_hour() {
    let spec = parse_spec_value(&valid_agent()).unwrap();
    assert_eq!(spec.timeout, Duration::from_secs(3600));
    assert_eq!(spec.name.as_str(), "test agent");
    match spec.workload {
        SubmitWorkloadValidated::Agent(agent) => assert!(agent.report_trailer),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn required_name_is_parsed_and_normalized() {
    let mut value = valid_task();
    value["name"] = json!("  build release  ");
    let spec = parse_spec_value(&value).unwrap();
    assert_eq!(spec.name.as_str(), "build release");
    let normalized = normalize(&spec).unwrap();
    assert_eq!(normalized.name.as_str(), "build release");
}

#[test]
fn blank_name_is_rejected_at_pointer() {
    let mut value = valid_task();
    value["name"] = json!("   ");
    let err = parse_spec_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn schema_requires_name() {
    let schema = schema_json().unwrap();
    assert!(schema["properties"]["name"].is_object());
    let required = schema["required"]
        .as_array()
        .expect("schema required array");
    assert!(
        required.iter().any(|value| value == "name"),
        "schema required={required:?}"
    );
    assert_verdict(&valid_task(), true, "a named task");
    let mut nameless = valid_task();
    nameless.as_object_mut().unwrap().remove("name");
    assert_verdict(&nameless, false, "a missing name");
    let mut named = valid_task();
    named["name"] = json!("ci watch");
    assert_verdict(&named, true, "a renamed task");
    named["name"] = json!("");
    assert_verdict(&named, false, "an empty name");
    named["name"] = json!("name\n");
    assert_verdict(&named, false, "a name with a trailing line break");
    named["name"] = json!("\tname");
    assert_verdict(&named, false, "a name with a control character");

    let err = parse_spec_value(&nameless).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
        other => panic!("unexpected {other:?}"),
    }
    let mut normalized = nameless.clone();
    normalized["timeout"] = json!("4h");
    let err = parse_normalized_value(&normalized).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/name"),
        other => panic!("unexpected {other:?}"),
    }
}

#[test]
fn normalized_rejects_short_timeout() {
    let value = json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "test task",
        "cwd": "/tmp",
        "timeout": "29m",
        "workload": { "type": "task", "command": ["true"] }
    });
    let err = parse_normalized_value(&value).unwrap_err();
    match err {
        AppError::InvalidSpec { pointer, .. } => assert_eq!(pointer, "/timeout"),
        other => panic!("unexpected {other:?}"),
    }
}

impl ThreadId {
    fn from_str_ok() -> Self {
        "01a0ab97-a7aa-7463-a5b0-8d500e40e431".parse().unwrap()
    }
}

#[test]
fn container_cwd_names_the_host_path_under_the_deepest_mount_target() {
    let host = tempfile::tempdir().unwrap();
    let spec: super::NormalizedSpec = serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "container cwd",
        "cwd": "/scratch/cache/runs",
        "timeout": "4h",
        "workload": {
            "type": "container",
            "image": format!("bench@sha256:{}", "0".repeat(64)),
            "memory": "8g",
            "mounts": [
                { "source": host.path(), "target": "/scratch" },
                { "source": host.path().join("cache"), "target": "/scratch/cache" }
            ]
        }
    }))
    .unwrap();

    let error = super::check_spec_host(&spec).unwrap_err();
    assert_eq!(
        error.rejection,
        super::HostInputRejection::Cwd(super::CwdProblem::NotFound)
    );
    let AppError::InvalidCwd { suggested_cwd, .. } = &error.error else {
        panic!("expected invalid_cwd, got {:?}", error.error);
    };
    assert_eq!(
        suggested_cwd.as_deref(),
        Some(host.path().join("cache/runs").as_path())
    );
}
