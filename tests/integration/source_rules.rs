//! Source scans that hold architecture rules no type can express

use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn counter_evidence_scan() {
    let src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let systemd = src.join("install/systemd.rs");
    let execstop: Vec<_> = fs::read_to_string(&systemd)
        .unwrap()
        .lines()
        .enumerate()
        .filter(|(_, line)| line.trim_start().starts_with("ExecStop="))
        .map(|(i, line)| format!("{}:{}:{line}", systemd.display(), i + 1))
        .collect();
    assert!(execstop.is_empty(), "ExecStop= in systemd.rs: {execstop:?}");
    let cli = src.join("cli");
    for flag in ["arg(\"--prompt", "arg(\"--agent"] {
        let flags = grep_src(&cli, flag);
        assert!(flags.is_empty(), "per-field submit flag {flag}: {flags:?}");
    }
    let report = src.join("report.rs");
    for token in ["UnixStream", "/v1/"] {
        let http = grep_src(&report, token);
        assert!(http.is_empty(), "report talks HTTP ({token}): {http:?}");
    }

    let daemon = src.join("daemon");
    // only the per-key lock type may use a mutex; the daemon holds no daemon-wide lock
    let mutex: Vec<_> = grep_src(&daemon, "Mutex")
        .into_iter()
        .filter(|line| !line.contains("/keyed_locks.rs:"))
        .collect();
    assert!(mutex.is_empty(), "Mutex in daemon: {mutex:?}");
    let rwlock = grep_src(&daemon, "RwLock");
    assert!(rwlock.is_empty(), "RwLock in daemon: {rwlock:?}");
    let sleep = grep_daemon_except_callback(&daemon, "thread::sleep");
    assert!(sleep.is_empty(), "thread::sleep in daemon: {sleep:?}");
    let cmd = grep_daemon_except_callback(&daemon, "Command::new");
    assert!(cmd.is_empty(), "Command::new in daemon: {cmd:?}");
    let open = grep_daemon_except_store(&daemon, "Store::open");
    assert!(open.is_empty(), "Store::open in daemon: {open:?}");
    let conn = grep_daemon_except_store(&daemon, "Connection::open");
    assert!(conn.is_empty(), "Connection::open in daemon: {conn:?}");
}

fn grep_daemon_except_callback(daemon: &Path, needle: &str) -> Vec<String> {
    grep_src(daemon, needle)
        .into_iter()
        .filter(|line| !line.contains("/actors/callback.rs:"))
        .collect()
}

fn grep_daemon_except_store(daemon: &Path, needle: &str) -> Vec<String> {
    grep_src(daemon, needle)
        .into_iter()
        .filter(|line| !line.contains("/actors/store.rs:"))
        .collect()
}

/// Scan production sections of `.rs` files under `path`
fn grep_src(path: &Path, needle: &str) -> Vec<String> {
    let mut matches = Vec::new();
    let files = if path.is_file() {
        vec![path.to_path_buf()]
    } else {
        walkdir_files(path)
    };
    for file in files {
        if file.extension().and_then(|s| s.to_str()) != Some("rs") {
            continue;
        }
        if is_test_module_file(&file) {
            continue;
        }
        if let Ok(text) = fs::read_to_string(&file) {
            // test modules may open stores and use synchronization to build fixtures
            for (i, line) in text
                .lines()
                .take_while(|line| line.trim() != "#[cfg(test)]")
                .enumerate()
            {
                if line.contains(needle) {
                    matches.push(format!("{}:{}:{line}", file.display(), i + 1));
                }
            }
        }
    }
    matches
}

/// Whether `file` is a child test module such as `foo/tests.rs`, `foo/tests/bar.rs`, or
/// `foo/test_support.rs`, which production code only declares behind `#[cfg(test)]`
fn is_test_module_file(file: &Path) -> bool {
    let stem = file.file_stem().and_then(|s| s.to_str());
    if matches!(stem, Some("tests" | "test_support")) {
        return true;
    }
    file.parent()
        .and_then(Path::file_name)
        .is_some_and(|dir| dir == "tests")
}

fn walkdir_files(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn rec(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(rd) = fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    rec(&p, out);
                } else {
                    out.push(p);
                }
            }
        }
    }
    rec(root, &mut out);
    out
}
