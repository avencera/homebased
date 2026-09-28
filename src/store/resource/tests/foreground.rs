//! Foreground ownership contract at request acceptance and task binding

use std::os::unix::fs::symlink;

use super::fixtures::{acceptance_input, resource, serving_fixture_with_spec};
use crate::domain::{ProcessStatus, TaskId};
use crate::machine::MachineId;
use crate::resource::ResourceTaskOwnershipRisk;
use crate::resource::foreground::test_support::native_fake_command;
use crate::resource::store::{PreLaunchFailure, ResourceStoreError, ResourceTaskAcceptance};
use crate::spec::NormalizedSpec;
use crate::store::Store;
use crate::submission::RequestId;
use serde_json::json;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use tempfile::tempdir;

fn command_in(root: &Path, argv: &[&Path]) -> NormalizedSpec {
    serde_json::from_value(json!({
        "api_version": 1,
        "thread": "01a0ab97-a7aa-7463-a5b0-8d500e40e431",
        "name": "gpu command",
        "cwd": root,
        "timeout": "4h",
        "workload": { "type": "task", "command": argv }
    }))
    .unwrap()
}

fn write_script(path: &Path) {
    fs::write(path, "#!/bin/sh\n/opt/gpu/bench &\n").unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

#[test]
fn wrapped_detached_and_script_launches_never_enter_the_queue() {
    let directory = tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    let mut store = Store::open(&root.join("db")).unwrap();
    let authority = MachineId::new();
    let resource = resource(authority);
    store.register_resource(authority, &resource).unwrap();
    let native = native_fake_command();
    let marker = root.join("marker");
    let script = root.join("prepared-command");
    write_script(&script);
    // a renamed link to a shell is judged by its target
    let renamed = root.join("bench");
    symlink("/bin/sh", &renamed).unwrap();

    let path = Path::new;
    let refused: [(&[&Path], ResourceTaskOwnershipRisk); 6] = [
        (
            &[
                path("/usr/bin/env"),
                path("docker"),
                path("run"),
                path("-d"),
                path("gpu"),
            ],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        (
            &[path("sudo"), path("-n"), native],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        (
            &[path("timeout"), path("1h"), native],
            ResourceTaskOwnershipRisk::ProgramLauncher,
        ),
        // an unrecognized launcher still exposes the nested container client
        (
            &[native, path("docker"), path("run"), path("-d"), path("gpu")],
            ResourceTaskOwnershipRisk::ContainerClient,
        ),
        (
            &[path("/bin/sh"), path("-c"), path("/opt/gpu/bench &")],
            ResourceTaskOwnershipRisk::ShellWrapper,
        ),
        (&[&script], ResourceTaskOwnershipRisk::ScriptEntryPoint),
    ];
    for (argv, expected) in refused {
        let result = store.accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_in(&root, argv),
        );
        assert!(
            matches!(
                result,
                Err(ResourceStoreError::UnsupportedCommandOwnership { risk }) if risk == expected
            ),
            "{argv:?}: {result:?}"
        );
    }
    assert!(matches!(
        store.accept_resource_request(
            authority,
            RequestId::new(),
            TaskId::new(),
            resource.id,
            MachineId::new(),
            command_in(&root, &[&renamed]),
        ),
        Err(ResourceStoreError::UnsupportedCommandOwnership { .. })
    ));
    assert!(
        store
            .resource_requests(authority, resource.id)
            .unwrap()
            .is_empty()
    );

    // a direct native command enters the queue, and its exact retry answers from
    // the saved request even after the prepared file changed
    let prepared = root.join("prepared-bench");
    symlink(native, &prepared).unwrap();
    let request_id = RequestId::new();
    let task_id = TaskId::new();
    let origin = MachineId::new();
    let spec = command_in(&root, &[&prepared, &marker]);
    let accepted = store
        .accept_resource_request(
            authority,
            request_id,
            task_id,
            resource.id,
            origin,
            spec.clone(),
        )
        .unwrap();
    fs::remove_file(&prepared).unwrap();
    write_script(&prepared);
    let retried = store
        .accept_resource_request(authority, request_id, task_id, resource.id, origin, spec)
        .unwrap();
    assert_eq!(retried.acceptance_sequence, accepted.acceptance_sequence);
    assert_eq!(
        store
            .resource_requests(authority, resource.id)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn bare_program_that_resolves_to_a_script_is_accepted_only_to_fail_before_launch() {
    let accept_on = |directory: &str, native: bool| {
        let spec = command_in(Path::new("/tmp"), &[Path::new("prepared-command")]);
        let mut fixture = serving_fixture_with_spec(true, true, spec);
        let bin = fixture.directory.path().join(directory);
        fs::create_dir(&bin).unwrap();
        if native {
            symlink(native_fake_command(), bin.join("prepared-command")).unwrap();
        } else {
            write_script(&bin.join("prepared-command"));
        }
        let mut input = acceptance_input(&fixture);
        input.executor_env.path = bin.display().to_string();
        let acceptance = fixture.store.accept_assigned_resource_task(input).unwrap();
        (fixture, acceptance)
    };

    // the task gets its records so it can end with a reason, but it must never spawn
    let (fixture, acceptance) = accept_on("scripts", false);
    let task = fixture.request.task_id;
    let ResourceTaskAcceptance::Unlaunchable {
        task: accepted,
        failure: PreLaunchFailure::Preparation { message },
    } = acceptance
    else {
        panic!("a script entry point must be accepted as unlaunchable, got {acceptance:?}");
    };
    assert_eq!(accepted, task);
    assert!(message.contains("ScriptEntryPoint"), "{message}");
    let row = fixture.store.get_task(task).unwrap().unwrap();
    assert_eq!(row.status(), ProcessStatus::Queued);

    // the same bare name on a PATH with a native executable binds normally
    let (fixture, acceptance) = accept_on("natives", true);
    assert_eq!(
        acceptance,
        ResourceTaskAcceptance::Inserted {
            task: fixture.request.task_id
        }
    );
}
