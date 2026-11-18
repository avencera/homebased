use super::*;

/// Variables that point direnv's allow list and config into the harness, so a
/// test never reads or writes the developer's own
///
/// `DIRENV_CONFIG` takes precedence over `XDG_CONFIG_HOME`, so a developer's
/// whitelist would otherwise allow the blocked fixture
fn direnv_env(h: &Harness) -> [(&'static str, String); 3] {
    let root = h.dir.path();
    [
        ("XDG_DATA_HOME", root.join("xdg-data").display().to_string()),
        (
            "XDG_CONFIG_HOME",
            root.join("xdg-config").display().to_string(),
        ),
        (
            "DIRENV_CONFIG",
            root.join("xdg-config/direnv").display().to_string(),
        ),
    ]
}

/// Whether direnv is on the harness `PATH`; CI installs it, so there a missing
/// direnv fails the test instead of skipping it
fn direnv_installed(h: &Harness) -> bool {
    let installed = Command::new("direnv")
        .arg("version")
        .env("PATH", &h.path)
        .output()
        .is_ok_and(|out| out.status.success());
    assert!(
        installed || std::env::var_os("CI").is_none(),
        "direnv is not on PATH; CI must install it so the .envrc tests run"
    );
    installed
}

/// A repository directory whose `.envrc` exports a value and prints it, so a
/// leak of the `.envrc`'s output into the task log would show
fn repo_with_envrc(h: &Harness, name: &str) -> PathBuf {
    let repo = h.dir.path().join(name);
    fs::create_dir_all(&repo).unwrap();
    fs::write(
        repo.join(".envrc"),
        "export HB_ENVRC_SECRET=from-envrc-value\necho \"leak:$HB_ENVRC_SECRET\"\n",
    )
    .unwrap();
    repo
}

/// Run a task in `cwd` that records what it sees of the variable, and return
/// that with the task log
fn run_and_read(h: &Harness, cwd: &Path, flags: &[&str]) -> (String, String) {
    let script = "printf '%s' \"${HB_ENVRC_SECRET-unset}\"";
    let out = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .arg("--quiet")
        .args(["task", "run", "--cwd"])
        .arg(cwd)
        .args(flags)
        .args(["--", "/bin/sh", "-c", script])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = String::from_utf8(out.stdout).unwrap().trim().to_owned();
    h.wait_status(&id, "succeeded");
    let log = fs::read_to_string(h.output_log(&id)).unwrap();
    let seen = log.lines().last().unwrap_or_default().to_owned();
    (seen, log)
}

#[test]
fn command_tasks_load_only_an_envrc_direnv_already_allows() {
    let mut h = Harness::new();
    if !direnv_installed(&h) {
        eprintln!("skipping: direnv is not installed");
        return;
    }
    let env = direnv_env(&h);
    let env_refs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    h.restart_daemon_with_env(&env_refs);

    let allowed = repo_with_envrc(&h, "allowed");
    let status = Command::new("direnv")
        .arg("allow")
        .arg(&allowed)
        .env("PATH", &h.path)
        .env("HOME", &h.user_home)
        .envs(env.iter().map(|(k, v)| (*k, v)))
        .status()
        .unwrap();
    assert!(status.success());
    let blocked = repo_with_envrc(&h, "blocked");

    // a subdirectory finds the repository's `.envrc`, as a terminal there would
    let nested = allowed.join("src");
    fs::create_dir_all(&nested).unwrap();
    let (seen, log) = run_and_read(&h, &nested, &[]);
    assert_eq!(seen, "from-envrc-value", "{log}");
    assert!(
        log.lines()
            .any(|line| line.starts_with("homebased: loaded") && line.contains("HB_ENVRC_SECRET")),
        "{log}"
    );
    // the log names variables, never values or what the `.envrc` printed
    assert_eq!(log.matches("from-envrc-value").count(), 1, "{log}");
    assert!(!log.contains("leak:"), "{log}");

    let (seen, log) = run_and_read(&h, &blocked, &[]);
    assert_eq!(seen, "unset", "{log}");
    assert!(log.contains("not loaded"), "{log}");
    assert!(!log.contains("leak:"), "{log}");

    let (seen, log) = run_and_read(&h, &allowed, &["--no-envrc"]);
    assert_eq!(seen, "unset", "{log}");
    assert!(!log.contains("homebased:"), "{log}");
}

#[test]
fn cancelling_while_an_envrc_loads_stops_it_and_never_starts_the_command() {
    let mut h = Harness::new();
    if !direnv_installed(&h) {
        eprintln!("skipping: direnv is not installed");
        return;
    }
    let env = direnv_env(&h);
    let env_refs: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    h.restart_daemon_with_env(&env_refs);

    let repo = h.dir.path().join("slow");
    fs::create_dir_all(&repo).unwrap();
    let sleeper = h.dir.path().join("sleeper.pid");
    let ran = h.dir.path().join("command-ran");
    fs::write(
        repo.join(".envrc"),
        format!("sleep 60 &\necho $! > '{}'\nwait\n", sleeper.display()),
    )
    .unwrap();
    let status = Command::new("direnv")
        .arg("allow")
        .arg(&repo)
        .env("PATH", &h.path)
        .env("HOME", &h.user_home)
        .envs(env.iter().map(|(k, v)| (*k, v)))
        .status()
        .unwrap();
    assert!(status.success());

    let out = h
        .cmd()
        .env("CODEX_THREAD_ID", THREAD)
        .arg("--quiet")
        .args(["task", "run", "--cwd"])
        .arg(&repo)
        .args(["--", "/usr/bin/touch"])
        .arg(&ran)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let id = String::from_utf8(out.stdout).unwrap().trim().to_owned();
    assert!(
        wait_until(Duration::from_secs(10), || fs::read_to_string(&sleeper)
            .is_ok_and(|pid| !pid.trim().is_empty())),
        "the .envrc never started"
    );
    let pid: i32 = fs::read_to_string(&sleeper)
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    let cancel = h
        .cmd()
        .args(["--json", "task", "cancel", &id])
        .output()
        .unwrap();
    assert!(
        cancel.status.success(),
        "{}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    h.wait_status(&id, "cancelled");
    assert!(
        wait_until(Duration::from_secs(5), || !process_is_live(pid)
            || is_zombie(pid)),
        "the .envrc's child survived the cancel"
    );
    assert!(!ran.exists(), "the command started after the cancel");
}
