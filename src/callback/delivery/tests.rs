use std::cell::Cell;
use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::json;
use tempfile::TempDir;

use super::{IntentIo, PendingRetry, PendingT3Send, retry_pending_t3, wake_provider_thread};
use crate::callback::send_check::{SendCheck, SendFailure, SendGate};
use crate::domain::{TaskEnv, ThreadId};
use crate::submission::{CallbackContext, CallbackExecutable};
use crate::t3::test_support::{FakeResponse, FakeT3Server, V2State, rpc_exit, write_runtime};
use crate::t3::{ProviderThread, WakeOutcome};

const THREAD: &str = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Create,
    Write,
    FileSync,
    DirectorySync,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    StopAfterCreate,
    StopDuringWrite,
    Write,
    FileSync,
    DirectorySync,
}

#[derive(Default)]
struct SaveState {
    operations: Vec<Operation>,
    durable: bool,
}

struct InjectedIo {
    failure: Cell<Option<Failure>>,
    state: Arc<Mutex<SaveState>>,
}

impl InjectedIo {
    fn new(failure: Option<Failure>) -> Self {
        Self {
            failure: Cell::new(failure),
            state: Arc::default(),
        }
    }

    fn operation(&self, operation: Operation) {
        self.state.lock().unwrap().operations.push(operation);
    }

    fn fail(&self, failure: Failure) -> io::Result<()> {
        if self.failure.get() == Some(failure) {
            self.failure.set(None);
            return Err(io::Error::other(format!("injected {failure:?}")));
        }
        Ok(())
    }

    fn operations(&self) -> Vec<Operation> {
        self.state.lock().unwrap().operations.clone()
    }
}

impl IntentIo for InjectedIo {
    fn create(&self, path: &Path) -> io::Result<File> {
        self.operation(Operation::Create);
        let file = File::create(path)?;
        assert_ne!(
            self.failure.get(),
            Some(Failure::StopAfterCreate),
            "stop after create"
        );
        Ok(file)
    }

    fn write(&self, file: &mut File, bytes: &[u8]) -> io::Result<()> {
        self.operation(Operation::Write);
        if matches!(
            self.failure.get(),
            Some(Failure::StopDuringWrite | Failure::Write)
        ) {
            file.write_all(&bytes[..2])?;
            assert_ne!(
                self.failure.get(),
                Some(Failure::StopDuringWrite),
                "stop during write"
            );
            self.fail(Failure::Write)?;
        }
        file.write_all(bytes)
    }

    fn sync_file(&self, file: &File) -> io::Result<()> {
        self.operation(Operation::FileSync);
        self.fail(Failure::FileSync)?;
        file.sync_all()
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.operation(Operation::DirectorySync);
        self.fail(Failure::DirectorySync)?;
        File::open(path)?.sync_all()?;
        self.state.lock().unwrap().durable = true;
        Ok(())
    }
}

struct Fixture {
    directory: TempDir,
    context: CallbackContext,
    server: FakeT3Server,
    lock: PathBuf,
    log: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        // these tests must exercise a real dispatch, not pass when curl is absent
        assert!(which::which("curl").is_ok());
        let server = FakeT3Server::start(
            Some(2),
            vec![
                FakeResponse::http(
                    200,
                    &json!({ "ticket": "fake-ticket", "expiresAt": "later" }),
                ),
                FakeResponse::WebSocket(vec![rpc_exit(json!({
                    "_tag": "Success", "value": { "sequence": 47 }
                }))]),
            ],
        );
        let directory = tempfile::tempdir().unwrap();
        let userdata = directory.path().join(".t3/userdata");
        fs::create_dir_all(&userdata).unwrap();
        write_runtime(
            &userdata,
            &server.origin,
            i32::try_from(std::process::id()).unwrap(),
        );
        V2State::create(&userdata.join("statev2.sqlite"))
            .thread("t3-thread", "Intent test", false)
            .native("codex", THREAD, "t3-thread");
        let bin = directory.path().join("bin");
        fs::create_dir(&bin).unwrap();
        let script = bin.join("t3");
        fs::write(&script, "#!/bin/sh\ncase \"$1 $2 $3\" in\n  'auth session issue') printf '%s\\n' '{\"sessionId\":\"fake-session\",\"token\":\"token\",\"method\":\"bearer-access-token\",\"scopes\":[]}' ;;\n  'auth session revoke') exit 0 ;;\n  *) exit 3 ;;\nesac\n").unwrap();
        fs::set_permissions(script, fs::Permissions::from_mode(0o755)).unwrap();
        Self {
            context: CallbackContext {
                env: TaskEnv {
                    path: bin.display().to_string(),
                    home: directory.path().display().to_string(),
                },
                cwd: directory.path().to_path_buf(),
                codex: CallbackExecutable::Unavailable {
                    reason: "unused".into(),
                },
            },
            lock: directory.path().join("delivery.lock"),
            log: directory.path().join("callback.log"),
            directory,
            server,
        }
    }

    fn thread(&self) -> ThreadId {
        THREAD.parse().unwrap()
    }

    fn pending(&self, failure: Option<Failure>) -> PendingT3Send<InjectedIo> {
        PendingT3Send(
            self.directory.path().join("t3-pending-1"),
            InjectedIo::new(failure),
        )
    }

    fn gate_check(&self, pending: &PendingT3Send<InjectedIo>) -> SendCheck {
        let state = Arc::clone(&pending.1.state);
        let path = pending.0.clone();
        Arc::new(move || {
            assert!(
                state.lock().unwrap().durable,
                "dispatch before intent durability"
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), "codex");
            Ok(())
        })
    }

    fn send(&self, pending: &PendingT3Send<InjectedIo>) -> Result<WakeOutcome, SendFailure> {
        let check = self.gate_check(pending);
        wake_provider_thread(
            &self.context,
            ProviderThread::Codex(self.thread()),
            "message",
            &self.log,
            pending,
            SendGate {
                path: &self.lock,
                check: Some(&check),
                hold: false,
            },
        )
    }

    fn retry(
        &self,
        pending: &PendingT3Send<InjectedIo>,
    ) -> Result<Option<PendingRetry>, SendFailure> {
        let check = self.gate_check(pending);
        retry_pending_t3(
            &self.context,
            self.thread(),
            "message",
            &self.log,
            pending,
            SendGate {
                path: &self.lock,
                check: Some(&check),
                hold: false,
            },
        )
    }

    fn assert_delivered(&self, pending: &PendingT3Send<InjectedIo>) {
        assert_eq!(self.server.requests().len(), 2);
        assert_eq!(self.server.requests()[1].method, "WS");
        assert!(!pending.0.exists());
        assert!(!pending.temporary_path().exists());
        assert!(self.log.exists());
    }
}

fn interrupted_save_recovers(failure: Failure, bytes: &[u8], operations: &[Operation]) {
    let fixture = Fixture::new();
    let pending = fixture.pending(Some(failure));
    let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fixture.send(&pending)));
    assert!(stopped.is_err());
    assert_eq!(pending.1.operations(), operations);
    assert!(fixture.server.requests().is_empty());
    assert!(!fixture.log.exists());
    assert!(
        !pending.0.exists(),
        "an interrupted save must not publish a final record"
    );
    assert_eq!(fs::read(pending.temporary_path()).unwrap(), bytes);

    let reopened = fixture.pending(None);
    assert!(fixture.retry(&reopened).unwrap().is_none());
    assert!(
        !reopened.temporary_path().exists(),
        "retry must remove the leftover temporary file"
    );
    assert!(matches!(
        fixture.send(&reopened).unwrap(),
        WakeOutcome::Woken { .. }
    ));
    assert_eq!(
        reopened.1.operations(),
        [
            Operation::Create,
            Operation::Write,
            Operation::FileSync,
            Operation::DirectorySync
        ]
    );
    fixture.assert_delivered(&reopened);
}

#[test]
fn interrupted_t3_intent_after_create_recovers_on_reopen() {
    interrupted_save_recovers(Failure::StopAfterCreate, b"", &[Operation::Create]);
}

#[test]
fn interrupted_t3_intent_after_partial_write_recovers_on_reopen() {
    interrupted_save_recovers(
        Failure::StopDuringWrite,
        b"co",
        &[Operation::Create, Operation::Write],
    );
}

fn failed_save_recovers(failure: Failure, operations: &[Operation]) {
    let fixture = Fixture::new();
    let pending = fixture.pending(Some(failure));
    let error = fixture.send(&pending).unwrap_err();
    assert!(error.to_string().contains("injected"));
    assert_eq!(pending.1.operations(), operations);
    assert!(fixture.server.requests().is_empty());
    assert!(!fixture.log.exists());
    assert!(
        !pending.0.exists(),
        "a failed save must not publish a final record"
    );
    assert!(
        !pending.temporary_path().exists(),
        "a failed save must clean up its temporary file"
    );

    let reopened = fixture.pending(None);
    assert!(fixture.retry(&reopened).unwrap().is_none());
    assert!(matches!(
        fixture.send(&reopened).unwrap(),
        WakeOutcome::Woken { .. }
    ));
    fixture.assert_delivered(&reopened);
}

#[test]
fn bugfix_t3_send_requires_a_saved_intent() {
    failed_save_recovers(Failure::Write, &[Operation::Create, Operation::Write]);

    // an invalid final record must still block fallback, even beside a staging file
    for record in [Some("unknown provider"), None] {
        let fixture = Fixture::new();
        let pending = PendingT3Send::new(fixture.directory.path().join("t3-pending-1"));
        match record {
            Some(content) => fs::write(&pending.0, content).unwrap(),
            None => fs::create_dir(&pending.0).unwrap(),
        }

        fs::write(pending.temporary_path(), "co").unwrap();
        let error = super::send_saved_queue_attempt(
            &fixture.context,
            fixture.thread(),
            "message",
            &fixture.log,
            &fixture.lock,
            &pending,
        )
        .unwrap_err();
        assert!(error.contains("invalid pending T3") || error.contains("read pending T3"));
        assert!(fixture.server.requests().is_empty());
        assert!(!fixture.log.exists());
        assert!(pending.0.exists());
        assert!(!pending.temporary_path().exists());
    }
}

#[test]
fn failed_t3_intent_file_sync_recovers_on_reopen() {
    failed_save_recovers(
        Failure::FileSync,
        &[Operation::Create, Operation::Write, Operation::FileSync],
    );
}

fn failed_directory_sync_is_redone(failure: Failure, operations: &[Operation]) {
    let fixture = Fixture::new();
    let pending = fixture.pending(Some(Failure::DirectorySync));
    assert!(
        fixture
            .send(&pending)
            .unwrap_err()
            .to_string()
            .contains("injected")
    );
    assert!(fixture.server.requests().is_empty());
    assert_eq!(fs::read_to_string(&pending.0).unwrap(), "codex");

    let reopened = fixture.pending(Some(failure));
    assert!(
        fixture
            .retry(&reopened)
            .err()
            .unwrap()
            .to_string()
            .contains("injected")
    );
    assert_eq!(reopened.1.operations(), operations);
    assert!(
        fixture.server.requests().is_empty(),
        "a retry must not skip the failed durability step"
    );
    assert!(reopened.0.exists());

    let retried = fixture.pending(None);
    assert!(matches!(
        fixture.retry(&retried).unwrap(),
        Some(PendingRetry::Delivered)
    ));
    assert_eq!(
        retried.1.operations(),
        [Operation::FileSync, Operation::DirectorySync]
    );
    fixture.assert_delivered(&retried);
}

#[test]
fn failed_t3_intent_directory_sync_redoes_file_sync_before_retry() {
    failed_directory_sync_is_redone(Failure::FileSync, &[Operation::FileSync]);
}

#[test]
fn failed_t3_intent_directory_sync_redoes_directory_sync_before_retry() {
    failed_directory_sync_is_redone(
        Failure::DirectorySync,
        &[Operation::FileSync, Operation::DirectorySync],
    );
}
