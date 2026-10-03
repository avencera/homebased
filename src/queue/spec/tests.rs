use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::Path;

use serde_json::{Value, json};
use tempfile::tempdir;

use super::{JobSpec, schema_json};
use crate::error::AppError;
use crate::queue::{Preemption, Priority, ResourceSelector, RestartWindow, StepWorkload};

const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// A two-step spec using every top-level field
fn two_step_example() -> Value {
    json!({
        "api_version": 1,
        "thread": "77777777-7777-4777-8777-777777777777",
        "machine": "main",
        "name": "sglang serving sweep",
        "cwd": "/Users/praveen/code/mac-decode-roof",
        "timeout": "1h",
        "priority": "medium",
        "preempt": { "mode": "yield", "restart_within": "5m" },
        "steps": [
            { "type": "task", "command": ["python", "scripts/serving_suite.py", "--cases", "dense-q4"] },
            { "type": "task", "command": ["python", "scripts/serving_suite.py", "--cases", "moe-q4"] }
        ]
    })
}

fn single(workload: Value) -> Value {
    json!({
        "api_version": 1,
        "thread": "77777777-7777-4777-8777-777777777777",
        "name": "bench",
        "cwd": "/tmp",
        "priority": "high",
        "preempt": { "mode": "wait" },
        "workload": workload
    })
}

fn container(extra: Value) -> Value {
    let mut step = json!({ "type": "container", "image": DIGEST, "memory": "1g" });
    if let (Some(step), Value::Object(extra)) = (step.as_object_mut(), extra) {
        step.extend(extra);
    }
    step
}

fn with(mut spec: Value, key: &str, value: Value) -> Value {
    spec[key] = value;
    spec
}

fn without(mut spec: Value, key: &str) -> Value {
    if let Some(object) = spec.as_object_mut() {
        object.remove(key);
    }
    spec
}

fn pointer_of(error: &AppError) -> &str {
    match error {
        AppError::InvalidSpec { pointer, .. } => pointer,
        other => panic!("expected invalid_spec, got {other:?}"),
    }
}

#[test]
fn a_two_step_spec_parses() {
    let spec = JobSpec::parse_value(&two_step_example()).unwrap();
    assert_eq!(spec.priority, Priority::Medium);
    assert_eq!(
        spec.preempt,
        Preemption::Yield {
            restart_within: Some(
                RestartWindow::try_from(std::time::Duration::from_secs(300)).unwrap()
            )
        }
    );
    assert_eq!(spec.steps.count(), 2);
    assert!(spec.resource.is_none());
    assert_eq!(spec.machine.as_ref().unwrap().to_string(), "main");
}

#[test]
fn valid_forms_parse() {
    let cases = [
        single(json!({ "type": "task", "command": ["python", "bench.py"] })),
        single(container(json!({}))),
        with(
            single(json!({ "type": "task", "command": ["true"] })),
            "preempt",
            json!({ "mode": "restart" }),
        ),
        with(
            single(json!({ "type": "task", "command": ["true"] })),
            "resource",
            json!("gpu1"),
        ),
        with(
            single(json!({ "type": "task", "command": ["true"] })),
            "machine",
            json!("01a0ab97-a7aa-7463-a5b0-8d500e40e431"),
        ),
        with(
            without(single(json!({})), "workload"),
            "steps",
            Value::Array(vec![json!({ "type": "task", "command": ["true"] }); 32]),
        ),
        with(
            single(container(
                json!({ "mounts": [{ "source": "/tmp", "target": "/homebasedx" }] }),
            )),
            "timeout",
            json!("30m"),
        ),
    ];
    for spec in cases {
        if let Err(error) = JobSpec::parse_value(&spec) {
            panic!("{spec} must parse: {error}");
        }
    }
    let pinned = JobSpec::parse_value(&with(
        single(json!({ "type": "task", "command": ["true"] })),
        "resource",
        json!("gpu1"),
    ))
    .unwrap();
    assert!(matches!(pinned.resource, Some(ResourceSelector::Name(_))));
}

#[test]
fn every_structural_refusal_has_its_pointer() {
    let task = json!({ "type": "task", "command": ["true"] });
    let base = single(task.clone());
    let cases = [
        (with(base.clone(), "after", json!([])), "/after"),
        (with(base.clone(), "unknown", json!(1)), "/unknown"),
        (with(base.clone(), "steps", json!([task.clone()])), ""),
        (without(base.clone(), "workload"), ""),
        (without(base.clone(), "priority"), "/priority"),
        (without(base.clone(), "preempt"), "/preempt"),
        (with(base.clone(), "priority", json!("urgent")), "/priority"),
        (with(base.clone(), "api_version", json!(2)), "/api_version"),
        (with(base.clone(), "timeout", json!("10m")), "/timeout"),
        (
            with(
                base.clone(),
                "preempt",
                json!({ "mode": "yield", "restart_within": "0s" }),
            ),
            "/preempt/restart_within",
        ),
        (
            with(
                base.clone(),
                "preempt",
                json!({ "mode": "wait", "restart_within": "2d" }),
            ),
            "/preempt/restart_within",
        ),
        (
            with(
                base.clone(),
                "preempt",
                json!({ "mode": "restart", "restart_within": "5m" }),
            ),
            "/preempt",
        ),
        (with(base.clone(), "resource", json!("GPU 0")), "/resource"),
        (
            with(without(base.clone(), "workload"), "steps", json!([])),
            "/steps",
        ),
        (
            with(
                without(base.clone(), "workload"),
                "steps",
                Value::Array(vec![task.clone(); 33]),
            ),
            "/steps",
        ),
        (
            single(json!({ "type": "agent", "agent": "codex", "prompt": "do it" })),
            "/workload/type",
        ),
        (
            with(
                without(base.clone(), "workload"),
                "steps",
                json!([task.clone(), { "type": "agent", "agent": "claude", "prompt": "x" }]),
            ),
            "/steps/1/type",
        ),
        (
            with(
                without(base.clone(), "workload"),
                "steps",
                json!([{ "type": "steps", "steps": [task.clone()] }]),
            ),
            "/steps/0/type",
        ),
        (
            single(json!({ "type": "task", "command": [] })),
            "/workload/command",
        ),
        (
            single(json!({ "type": "task", "command": ["x"], "cwd": "/" })),
            "/workload/cwd",
        ),
        (
            single(container(json!({ "gpus": "all" }))),
            "/workload/gpus",
        ),
        (single(container(json!({ "gpus": [0] }))), "/workload/gpus"),
        (
            single(container(
                json!({ "mounts": [{ "source": "/tmp", "target": "/homebased" }] }),
            )),
            "/workload/mounts/0/target",
        ),
        (
            with(
                without(base.clone(), "workload"),
                "steps",
                json!([
                    task.clone(),
                    container(json!({ "mounts": [
                    { "source": "/tmp", "target": "/data" },
                    { "source": "/tmp", "target": "/homebased/job" }
                ] }))
                ]),
            ),
            "/steps/1/mounts/1/target",
        ),
        (
            single(container(json!({ "env": { "HOMEBASED_JOB_DIR": "/x" } }))),
            "/workload/env/HOMEBASED_JOB_DIR",
        ),
        (
            single(container(json!({ "env": { "HOMEBASED_ANYTHING": "1" } }))),
            "/workload/env/HOMEBASED_ANYTHING",
        ),
    ];
    for (spec, pointer) in cases {
        let error = JobSpec::parse_value(&spec).unwrap_err();
        assert_eq!(pointer_of(&error), pointer, "{spec}: {error}");
    }
}

#[test]
fn canonical_form_round_trips_and_digests_spelling_independently() {
    let task = json!({ "type": "task", "command": ["python", "bench.py"] });
    let one = JobSpec::parse_value(&single(task.clone())).unwrap();
    let as_steps = JobSpec::parse_value(&with(
        without(single(Value::Null), "workload"),
        "steps",
        json!([task]),
    ))
    .unwrap();
    assert_eq!(one, as_steps);
    assert_eq!(one.digest().unwrap(), as_steps.digest().unwrap());

    let stored: Value = serde_json::from_str(&one.to_canonical_json().unwrap()).unwrap();
    assert_eq!(stored["timeout"], json!("1h"), "defaults are written out");
    let reparsed = JobSpec::parse_value(&stored).unwrap();
    assert_eq!(reparsed, one);
    assert_eq!(reparsed.digest().unwrap(), one.digest().unwrap());

    let other = JobSpec::parse_value(&with(
        single(json!({ "type": "task", "command": ["true"] })),
        "priority",
        json!("low"),
    ))
    .unwrap();
    assert_ne!(other.digest().unwrap(), one.digest().unwrap());
}

fn executable(dir: &Path, name: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

fn task_spec(cwd: &Path, command: &[&str]) -> JobSpec {
    let mut spec = single(json!({ "type": "task", "command": command }));
    spec["cwd"] = json!(cwd);
    JobSpec::parse_value(&spec).unwrap()
}

#[test]
fn authority_accepts_an_existing_cwd_and_executable() {
    let dir = tempdir().unwrap();
    let tool = executable(dir.path(), "bench");
    task_spec(dir.path(), &[&tool])
        .check_on_authority("")
        .unwrap();
    // found through PATH too
    let path = dir.path().to_string_lossy().into_owned();
    task_spec(dir.path(), &["bench"])
        .check_on_authority(&path)
        .unwrap();
}

#[test]
fn authority_refuses_missing_inputs() {
    let dir = tempdir().unwrap();
    let tool = executable(dir.path(), "bench");

    let missing_cwd = task_spec(&dir.path().join("gone"), &[&tool]);
    assert!(matches!(
        missing_cwd.check_on_authority(""),
        Err(AppError::InvalidCwd { .. })
    ));

    let missing_program = task_spec(dir.path(), &["no-such-bench-tool"]);
    assert!(matches!(
        missing_program.check_on_authority("/nonexistent"),
        Err(AppError::ExecutableMissing { .. })
    ));

    let mut spec = single(container(json!({
        "mounts": [{ "source": dir.path().join("absent"), "target": "/data" }]
    })));
    spec["cwd"] = json!(dir.path());
    let error = JobSpec::parse_value(&spec)
        .unwrap()
        .check_on_authority("")
        .unwrap_err();
    assert_eq!(pointer_of(&error), "/steps/0/mounts/0/source", "{error}");
}

#[test]
fn authority_refuses_every_escape_entry_point_by_resolved_basename() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_string_lossy().into_owned();
    for name in super::ESCAPE_ENTRY_POINTS {
        let tool = executable(dir.path(), name);
        for program in [tool.as_str(), name] {
            let error = task_spec(dir.path(), &[program, "new-session"])
                .check_on_authority(&path)
                .unwrap_err();
            assert_eq!(
                pointer_of(&error),
                "/steps/0/command/0",
                "{program}: {error}"
            );
        }
    }

    // a link with another name still resolves to tmux
    symlink(dir.path().join("tmux"), dir.path().join("t")).unwrap();
    let error = task_spec(dir.path(), &["t"])
        .check_on_authority(&path)
        .unwrap_err();
    assert_eq!(pointer_of(&error), "/steps/0/command/0", "{error}");

    // only the entry point is checked: an argument naming tmux is fine
    let tool = executable(dir.path(), "bench");
    task_spec(dir.path(), &[&tool, "tmux"])
        .check_on_authority(&path)
        .unwrap();
}

#[test]
fn schema_accepts_a_two_step_spec_and_rejects_refused_shapes() {
    let validator = jsonschema::validator_for(&schema_json().unwrap()).unwrap();
    let task = json!({ "type": "task", "command": ["true"] });
    assert!(validator.is_valid(&two_step_example()));
    assert!(validator.is_valid(&single(task.clone())));
    assert!(validator.is_valid(&single(container(json!({})))));

    let refused = [
        with(single(task.clone()), "steps", json!([task.clone()])),
        without(single(task.clone()), "workload"),
        without(single(task.clone()), "priority"),
        without(single(task.clone()), "preempt"),
        with(single(task.clone()), "after", json!([])),
        with(
            single(task.clone()),
            "preempt",
            json!({ "mode": "restart", "restart_within": "5m" }),
        ),
        single(json!({ "type": "agent", "agent": "codex", "prompt": "x" })),
        single(container(json!({ "gpus": "all" }))),
        with(
            without(single(task.clone()), "workload"),
            "steps",
            json!([]),
        ),
        with(
            without(single(task.clone()), "workload"),
            "steps",
            Value::Array(vec![task; 33]),
        ),
    ];
    for spec in refused {
        assert!(!validator.is_valid(&spec), "schema must refuse {spec}");
        assert!(
            JobSpec::parse_value(&spec).is_err(),
            "parser must refuse {spec}"
        );
    }
}

#[test]
fn steps_keep_task_and_container_workloads() {
    let spec = JobSpec::parse_value(&with(
        without(single(Value::Null), "workload"),
        "steps",
        json!([{ "type": "task", "command": ["true"] }, container(json!({}))]),
    ))
    .unwrap();
    let kinds: Vec<_> = spec
        .steps
        .as_slice()
        .iter()
        .map(|step| matches!(step, StepWorkload::Task(_)))
        .collect();
    assert_eq!(kinds, [true, false]);
}
