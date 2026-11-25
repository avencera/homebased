//! Pool jobs on a private repository: the coordinator fetches each job's
//! commit with git config the machines do not have, and the machines check
//! it out from the one-commit bundle they download over the Fleet link

use super::*;

/// Remote name that only the coordinator's git config resolves, as a
/// private repository only the coordinator's credentials reach
const PRIVATE_REMOTE: &str = "private:acme/app.git";

/// Run git in `dir` with a fixed author, panicking on failure
fn git(dir: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .env("GIT_AUTHOR_NAME", "t")
        .env("GIT_AUTHOR_EMAIL", "t@example.com")
        .env("GIT_COMMITTER_NAME", "t")
        .env("GIT_COMMITTER_EMAIL", "t@example.com")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Commit `marker` with `text` in `work`, and return the commit id
fn commit(work: &std::path::Path, text: &str) -> String {
    fs::write(work.join("marker"), text).unwrap();
    git(work, &["add", "-A"]);
    git(work, &["commit", "-qm", text]);
    git(work, &["rev-parse", "HEAD"])
}

/// A bare repository in `origin`'s home with commits `one` through `four`
/// on `main`, and one with a submodule on its own branch, which only the
/// coordinator's git config reaches as [`PRIVATE_REMOTE`]
fn private_repository(origin: &Daemon) -> BTreeMap<&'static str, String> {
    let bare = origin.user_home.join("private.git");
    let work = origin.user_home.join("work");
    fs::create_dir_all(&work).unwrap();
    git(
        &origin.user_home,
        &["init", "-q", "--bare", &bare.display().to_string()],
    );
    git(&work, &["init", "-q", "-b", "main"]);
    let mut commits = BTreeMap::new();
    for name in ["one", "two", "three", "four"] {
        commits.insert(name, commit(&work, name));
    }
    git(&work, &["checkout", "-q", "-b", "with-submodule"]);
    fs::write(
        work.join(".gitmodules"),
        "[submodule \"lib\"]\n\tpath = lib\n\turl = private:acme/lib.git\n",
    )
    .unwrap();
    commits.insert("submodule", commit(&work, "submodule"));
    let bare_text = bare.display().to_string();
    git(&work, &["push", "-q", &bare_text, "main", "with-submodule"]);
    fs::write(
        origin.user_home.join(".gitconfig"),
        format!("[url \"{bare_text}\"]\n\tinsteadOf = {PRIVATE_REMOTE}\n"),
    )
    .unwrap();
    commits
}

/// Submit a pool job that checks out `commit` and runs `script` in it
fn git_job(
    origin: &Daemon,
    thread: ThreadId,
    commit: &str,
    script: &str,
) -> (JobId, std::process::Output) {
    let job = JobId::new();
    let job_text = job.to_string();
    let output = cli_output(
        origin,
        thread,
        &[
            "ci",
            "run",
            "--class",
            "afv",
            "--job-id",
            &job_text,
            "--repo",
            PRIVATE_REMOTE,
            "--commit",
            commit,
            "--kill-after",
            "10m",
            "--",
            "/bin/sh",
            "-c",
            script,
        ],
    );
    (job, output)
}

fn refusal(output: &std::process::Output) -> String {
    assert!(!output.status.success());
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The run logs on `machine` that contain `text`
fn logs_with(machine: &Daemon, text: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut dirs = vec![machine.home.join("tasks")];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if fs::read(&path)
                .is_ok_and(|bytes| String::from_utf8_lossy(&bytes).contains(text))
            {
                found.push(path);
            }
        }
    }
    found
}

/// The pool's one machine
fn only(machines: &Machines) -> &Daemon {
    machines.daemons.values().next().unwrap()
}

/// Flip one byte of the file at `path`, keeping its size
fn damage(path: &std::path::Path) -> Vec<u8> {
    let good = fs::read(path).unwrap();
    let mut bad = good.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0xff;
    fs::write(path, bad).unwrap();
    good
}

#[test]
fn pool_jobs_on_a_private_repository_run_from_one_commit_bundles() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    let commits = private_repository(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "afv",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
            // long enough that the machine, and its run logs, outlive the jobs
            "--idle-limit",
            "1m",
        ],
    );

    // a commit the coordinator cannot fetch, and one with submodules, are
    // refused at submit, before the pool asks for any machine
    let (_, missing) = git_job(&origin, thread, &"0".repeat(40), "true");
    let missing = refusal(&missing);
    assert!(missing.contains("commit_unavailable"), "{missing}");
    let (_, submodule) = git_job(&origin, thread, &commits["submodule"], "true");
    let submodule = refusal(&submodule);
    assert!(submodule.contains("submodules_unsupported"), "{submodule}");
    std::thread::sleep(Duration::from_secs(3));
    assert!(fake.leases().is_empty(), "a refused job got a machine");

    // the first job holds the only slot until `go` appears, so the next two
    // wait while their bundles are damaged on the coordinator
    let outputs = origin.user_home.join("outputs");
    fs::create_dir_all(&outputs).unwrap();
    let go = origin.user_home.join("go");
    let report = |extra: &str| {
        format!(
            "cat marker > {}/$HOMEBASED_JOB_ID; git rev-list --count HEAD >> {}/$HOMEBASED_JOB_ID{extra}",
            outputs.display(),
            outputs.display()
        )
    };
    let wait_for_go = format!("; while [ ! -f {} ]; do sleep 0.2; done", go.display());
    let (first, output) = git_job(&origin, thread, &commits["two"], &report(&wait_for_go));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (retried, output) = git_job(&origin, thread, &commits["three"], &report(""));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let (broken, output) = git_job(&origin, thread, &commits["four"], &report(""));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // the bundle holds the job's commit alone, under one ref
    let bundles = origin.home.join("pools/afv/bundles");
    let bundle = bundles.join(format!("{}.bundle", commits["two"]));
    let heads = git(
        &origin.user_home,
        &["bundle", "list-heads", &bundle.display().to_string()],
    );
    assert_eq!(heads.lines().count(), 1, "{heads}");
    assert!(heads.starts_with(&commits["two"]), "{heads}");
    let unpacked = origin.user_home.join("unpacked.git");
    git(
        &origin.user_home,
        &["init", "-q", "--bare", &unpacked.display().to_string()],
    );
    git(
        &unpacked,
        &["bundle", "unbundle", &bundle.display().to_string()],
    );
    let objects = git(
        &unpacked,
        &["cat-file", "--batch-all-objects", "--batch-check"],
    );
    let commit_objects: Vec<_> = objects
        .lines()
        .filter(|line| line.contains(" commit "))
        .collect();
    assert_eq!(commit_objects.len(), 1, "{objects}");
    assert!(commit_objects[0].starts_with(&commits["two"]));

    let retried_bundle = bundles.join(format!("{}.bundle", commits["three"]));
    let retried_good = damage(&retried_bundle);
    damage(&bundles.join(format!("{}.bundle", commits["four"])));

    let mut machines = Machines::default();
    assert!(
        machines.drive(&fake, Duration::from_secs(90), || outputs
            .join(first.to_string())
            .exists()),
        "the first job never ran: {}",
        cli(
            &origin,
            thread,
            &["resource", "job", "show", &first.to_string()]
        )
    );
    // only a joined machine may ask for a bundle, and only for its own jobs
    let lease = fake.leases().remove(0);

    // a job pinned to the pool's machine gets its bundle too
    let pinned = JobId::new();
    let pinned_text = pinned.to_string();
    let output = cli_output(
        &origin,
        thread,
        &[
            "ci",
            "run",
            "--class",
            "afv",
            "--machine",
            &lease.hostname,
            "--job-id",
            &pinned_text,
            "--repo",
            PRIVATE_REMOTE,
            "--commit",
            &commits["one"],
            "--kill-after",
            "10m",
            "--",
            "/bin/sh",
            "-c",
            &report(""),
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = format!("/v1/cluster/pool/jobs/{first}/bundle");
    assert_eq!(
        link_request(&lease, iroh::SecretKey::generate(), &get(&path)),
        Some(404)
    );
    fs::write(&go, "").unwrap();

    // a damaged transfer is tried again; restored, the next attempt succeeds
    assert!(
        wait_until(Duration::from_secs(60), || {
            !logs_with(only(&machines), "bundle download attempt 1 of 3 failed").is_empty()
        }),
        "no failed attempt was logged"
    );
    fs::write(&retried_bundle, retried_good).unwrap();
    let jobs = [first, retried, broken, pinned];
    assert!(
        machines.drive(&fake, Duration::from_secs(90), || {
            jobs.iter().all(|job| final_event(&origin, *job).is_some())
        }),
        "jobs never ended: {}",
        cli(&origin, thread, &["ci", "slots"])
    );
    for (job, marker) in [(first, "two"), (retried, "three"), (pinned, "one")] {
        assert_eq!(
            final_event(&origin, job).unwrap().event,
            JobEventKind::JobSucceeded,
            "{marker}"
        );
        // the checkout is the job's commit, with no history before it
        let written = fs::read_to_string(outputs.join(job.to_string())).unwrap();
        assert_eq!(written, format!("{marker}1\n"));
    }
    // a transfer that stays damaged fails the run, saying why
    assert_eq!(
        final_event(&origin, broken).unwrap().event,
        JobEventKind::JobFailed
    );
    assert!(!outputs.join(broken.to_string()).exists());
    let reason = "did not arrive in 3 attempts: the bundle arrived damaged";
    assert!(!logs_with(only(&machines), reason).is_empty());

    // releasing the pool removes its bundles and mirrors
    cli(&origin, thread, &["pool", "release", "afv"]);
    assert!(
        machines.drive(&fake, Duration::from_secs(60), || {
            pool(&origin, thread, "afv")["state"] == "released"
        }),
        "pool never released: {}",
        pool(&origin, thread, "afv")
    );
    assert!(!origin.home.join("pools/afv").exists());
}

#[test]
fn a_job_admitted_while_its_pool_releases_fails_with_the_pool() {
    let fake = FakeCloudlease::start(Duration::from_secs(4));
    let origin = coordinator(&fake);
    let thread = thread(&origin);
    let commits = private_repository(&origin);
    cli(
        &origin,
        thread,
        &[
            "pool",
            "create",
            "afv",
            "--machine-class",
            "do-c16",
            "--max",
            "1",
            "--deadline",
            "1h",
        ],
    );

    // the coordinator's mirror sets a ref while it makes the job's bundle; a
    // hook there holds admission, after the pool saw it open, until `go`. It
    // reads all its input, since git fails a transaction whose hook did not.
    // The release removes the pool's mirror, which would fail admission, so
    // the hook puts it back: as if the bundle was done just before the release
    let hooks = origin.user_home.join("hooks");
    fs::create_dir_all(&hooks).unwrap();
    let entered = origin.user_home.join("entered");
    let go = origin.user_home.join("go");
    let pool_dir = origin.home.join("pools/afv");
    let saved = origin.user_home.join("saved-pool");
    let hook = hooks.join("reference-transaction");
    fs::write(
        &hook,
        format!(
            "#!/bin/sh
refs=$(cat)
case \"$1 $refs\" in prepared*refs/homebased/bundle/*) ;; *) exit 0 ;; esac
cp -a {pool} {saved}
touch {entered}
while [ ! -f {go} ]; do sleep 0.1; done
[ -d {pool} ] || cp -a {saved} {pool}
",
            pool = pool_dir.display(),
            saved = saved.display(),
            entered = entered.display(),
            go = go.display()
        ),
    )
    .unwrap();
    fs::set_permissions(&hook, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
    let config = origin.user_home.join(".gitconfig");
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str(&format!("[core]\n\thooksPath = {}\n", hooks.display()));
    fs::write(&config, text).unwrap();

    let job = std::thread::scope(|scope| {
        let submit = scope.spawn(|| git_job(&origin, thread, &commits["two"], "true"));
        assert!(
            wait_until(Duration::from_secs(30), || entered.exists()),
            "admission never made the bundle"
        );
        cli(&origin, thread, &["pool", "release", "afv"]);
        assert!(
            wait_until(Duration::from_secs(30), || {
                pool(&origin, thread, "afv")["state"] == "released"
            }),
            "the pool never released: {}",
            pool(&origin, thread, "afv")
        );
        fs::write(&go, "").unwrap();
        let (job, output) = submit.join().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        job
    });

    // the released pool no longer ends its class's held jobs, so the job
    // ends with the pool at submit instead of waiting forever
    assert!(
        wait_until(Duration::from_secs(10), || final_event(&origin, job)
            .is_some()),
        "the job admitted while its pool released never ended"
    );
    let ended = final_event(&origin, job).unwrap();
    assert_eq!(ended.event, JobEventKind::JobFailed);
    assert_eq!(held_code(&ended), "pool_released", "{ended:?}");
}
