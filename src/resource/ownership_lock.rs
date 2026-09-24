//! Ownership-lock evidence for direct-segment trainers
//!
//! A trainer holds `.segment.lock` under its runtime root while its GPU worker
//! lives. [`attempt_evidence`] captures the lock and exact request of one live
//! attempt at registration time, and [`lock_probe`] later checks whether that
//! exact lock was released

mod attempt_evidence;
mod lock_probe;

pub use attempt_evidence::{
    TrainerAttemptEvidenceError, TrainerAttemptUnsafePathReason, TrainerRequestDigest,
    VerifiedTrainerAttempt, build_trainer_attempt_registration_evidence,
};
pub use lock_probe::{
    OwnershipLockGuard, OwnershipLockIdentity, OwnershipLockIdentityMismatchReason,
    OwnershipLockProbe, OwnershipLockProbeError, OwnershipLockProbeOperation,
    probe_segment_ownership_lock, verify_ownership_lock_guard,
};

/// Lock file that the trainer holds directly under its runtime root
const SEGMENT_LOCK_FILE: &str = ".segment.lock";

#[cfg(test)]
pub(crate) mod test_support {
    use std::env;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, ChildStdout, Command, Stdio};

    use std::fs::{self, File, OpenOptions};
    use std::time::SystemTime;

    use super::{OwnershipLockIdentity, TrainerRequestDigest, VerifiedTrainerAttempt};
    use crate::digest::Sha256Digest;
    use crate::resource::trainer_publication::AttemptBinding;

    pub(crate) fn attempt_binding(attempt_id: &str) -> AttemptBinding {
        AttemptBinding {
            campaign_id: "campaign-1".into(),
            campaign_revision_id: "revision-1".into(),
            task_id: "trainer-task-1".into(),
            attempt_id: attempt_id.into(),
            attempt_number: 1,
            ownership_token: "token-1".into(),
        }
    }

    pub(crate) fn verified_attempt(
        binding: AttemptBinding,
        request_sha256: [u8; 32],
        lock_identity: OwnershipLockIdentity,
    ) -> VerifiedTrainerAttempt {
        VerifiedTrainerAttempt::from_persisted(
            PathBuf::from("/trainer/runtime"),
            binding,
            TrainerRequestDigest::from_hex(&Sha256Digest::from_bytes(request_sha256).to_hex())
                .expect("test request digest is canonical"),
            lock_identity,
        )
        .expect("test attempt evidence is valid")
    }

    pub(crate) const CHILD_PATH_ENV: &str = "HOMEBASED_OWNERSHIP_LOCK_TEST_PATH";
    pub(crate) const CHILD_MODE_ENV: &str = "HOMEBASED_OWNERSHIP_LOCK_TEST_MODE";
    pub(crate) const HELPER_TEST: &str =
        "resource::ownership_lock::lock_probe::tests::fake_process_lock_helper";
    pub(crate) const LOCK_ACQUIRED: &str = "FAKE_LOCK_ACQUIRED";
    pub(crate) const LOCK_CONTENDED: &str = "FAKE_LOCK_CONTENDED";

    /// Separate test process that holds one exact lock file until released or dropped
    ///
    /// A `flock` lock is owned by the open file description, so a holder in another process
    /// behaves like the trainer worker that outlives its wrapper
    pub(crate) struct FakeLockProcess {
        child: Child,
        output: BufReader<ChildStdout>,
        released: bool,
    }

    impl FakeLockProcess {
        /// Let the holder release the lock and wait for it to exit
        pub(crate) fn release(&mut self) {
            self.child
                .stdin
                .as_mut()
                .expect("fake lock process stdin is piped")
                .write_all(b"x")
                .expect("fake lock process accepts release input");
            self.child.stdin.take();
            let status = self.child.wait().expect("fake lock process exits");
            let mut trailing = String::new();
            self.output
                .read_to_string(&mut trailing)
                .expect("read fake lock process output");
            assert!(status.success(), "fake lock process failed: {trailing}");
            self.released = true;
        }
    }

    impl Drop for FakeLockProcess {
        fn drop(&mut self) {
            if self.released {
                return;
            }

            if let Some(stdin) = self.child.stdin.as_mut() {
                let _ = stdin.write_all(b"x");
            }
            self.child.stdin.take();
            let _ = self.child.wait();
        }
    }

    /// Start a separate process that holds the existing lock file at `path`
    pub(crate) fn start_fake_lock_process(path: &Path) -> FakeLockProcess {
        let mut child = Command::new(env::current_exe().expect("find current test binary"))
            .arg("--exact")
            .arg(HELPER_TEST)
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, path)
            .env(CHILD_MODE_ENV, "hold")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start fake lock-holder process");
        let mut process = FakeLockProcess {
            output: BufReader::new(child.stdout.take().expect("capture helper stdout")),
            child,
            released: false,
        };

        let mut line = String::new();
        loop {
            line.clear();
            let bytes = process
                .output
                .read_line(&mut line)
                .expect("read fake lock-holder readiness");
            assert_ne!(bytes, 0, "fake lock-holder exited before acquiring lock");
            if line.trim() == LOCK_ACQUIRED {
                break;
            }
        }

        process
    }

    #[derive(Debug, PartialEq, Eq)]
    pub(crate) struct FileSnapshot {
        identity: OwnershipLockIdentity,
        contents: Vec<u8>,
        length: u64,
        modified: Option<SystemTime>,
    }

    pub(crate) fn lock_path(runtime_root: &Path) -> PathBuf {
        runtime_root.join(".segment.lock")
    }

    pub(crate) fn create_lock(runtime_root: &Path, contents: &[u8]) -> File {
        fs::create_dir_all(runtime_root).expect("create temporary runtime root");
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(lock_path(runtime_root))
            .expect("create fake lock file");
        file.write_all(contents).expect("write fake lock file");
        file
    }

    pub(crate) fn identity(file: &File) -> OwnershipLockIdentity {
        OwnershipLockIdentity::from_metadata(&file.metadata().expect("read file metadata"))
    }

    pub(crate) fn snapshot(path: &Path) -> FileSnapshot {
        let file = File::open(path).expect("open file for a read-only snapshot");
        let metadata = file.metadata().expect("read file snapshot metadata");
        let contents = fs::read(path).expect("read file snapshot contents");

        FileSnapshot {
            identity: OwnershipLockIdentity::from_metadata(&metadata),
            contents,
            length: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }

    pub(crate) fn directory_entries(path: &Path) -> Vec<PathBuf> {
        let mut entries = fs::read_dir(path)
            .expect("read runtime root entries")
            .map(|entry| entry.expect("read runtime root entry").path())
            .collect::<Vec<_>>();
        entries.sort();
        entries
    }

    pub(crate) fn try_fake_lock_process(path: &Path) -> String {
        let output = Command::new(env::current_exe().expect("find current test binary"))
            .arg("--exact")
            .arg(HELPER_TEST)
            .arg("--nocapture")
            .env(CHILD_PATH_ENV, path)
            .env(CHILD_MODE_ENV, "try")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("run fake nonblocking lock attempt");
        assert!(
            output.status.success(),
            "fake lock attempt failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        String::from_utf8(output.stdout).expect("fake lock helper output is UTF-8")
    }
}
