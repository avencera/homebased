use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use rusqlite::params;
use serde_json::{Value, json};
use tempfile::{TempDir, tempdir};

use super::{CancelResult, JobRecord, NewJob};
use crate::cleanup::CleanupFailure as ProcessCleanupFailure;
use crate::domain::{
    ExitReason, ProcessGroupExitEvidence, ProcessStatus, TaskEnv, TaskId, TaskState,
};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::gpu::DetectedResource;
use crate::queue::schedule::{NoticeThresholds, QueuedState, decide};
use crate::queue::spec::JobSpec;
use crate::queue::{
    ActiveRun, CleanupFailure, JobEventKind, JobId, JobState, LevelEnd, MoveRefusal, OperationId,
    Placement, Priority, QueueError, ResourceId, ResourceName, RunPhase, Side, StepIndex,
    StopCause, Target,
};
use crate::store::Store;

struct Fixture {
    _dir: TempDir,
    store: Store,
    machine: MachineId,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempdir().unwrap();
        let store = Store::open(&dir.path().join("db")).unwrap();
        let machine = MachineId::new();
        let fixture = Self {
            _dir: dir,
            store,
            machine,
        };
        fixture.detect(&["gpu0", "gpu1"]);
        fixture
    }

    fn detect(&self, names: &[&str]) {
        let detected: Vec<_> = names
            .iter()
            .enumerate()
            .map(|(index, name)| DetectedResource {
                name: ResourceName::parse(name).unwrap(),
                device: Some(u32::try_from(index).unwrap()),
            })
            .collect();
        self.store
            .ensure_detected_resources(self.machine, &detected)
            .unwrap();
    }

    fn resource(&self, name: &str) -> ResourceId {
        self.store
            .resolve_resource(
                self.machine,
                &crate::queue::ResourceSelector::Name(ResourceName::parse(name).unwrap()),
            )
            .unwrap()
            .resource
            .id
    }

    fn submit_spec(&self, spec: Value) -> JobId {
        let id = JobId::new();
        self.store
            .submit_job(&new_job(id, self.machine, spec))
            .unwrap();
        id
    }

    fn submit(&self, priority: Priority) -> JobId {
        self.submit_spec(spec(priority, 1))
    }

    fn job(&self, id: JobId) -> JobRecord {
        self.store.job(id).unwrap().unwrap()
    }

    /// The queue as `(job, level, position)` in serving order
    fn order(&self) -> Vec<(JobId, Priority, u32)> {
        self.store
            .machine_queue(self.machine)
            .unwrap()
            .into_iter()
            .map(|job| (job.id, job.priority, job.position.unwrap()))
            .collect()
    }

    fn ids(&self) -> Vec<JobId> {
        self.order().into_iter().map(|(id, ..)| id).collect()
    }

    fn move_job(&self, job: JobId, placement: Placement) -> Result<super::MoveResult, AppError> {
        self.store
            .move_job(self.machine, OperationId::new(), job, placement)
    }

    fn cancel(&self, job: JobId) -> CancelResult {
        self.store
            .cancel_job(self.machine, OperationId::new(), job, Utc::now())
            .unwrap()
    }

    fn reserve(&self, job: JobId, resource: &str) -> ActiveRun {
        self.store
            .reserve_run(
                self.machine,
                job,
                self.resource(resource),
                TaskId::new(),
                PathBuf::from("/usr/bin/true"),
                Utc::now(),
            )
            .unwrap()
    }

    /// Reserve a run, start its worker, and confirm it executing
    fn start(&self, job: JobId, resource: &str, started_at: DateTime<Utc>) -> ActiveRun {
        let reserved = self.reserve(job, resource);
        let task = reserved.task;
        self.store
            .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap()
            .unwrap();
        self.store
            .mark_run_executing(reserved.resource, task, started_at)
            .unwrap()
    }

    fn exit(&self, run: &ActiveRun, reason: ExitReason) {
        self.store
            .cas_exit_with_evidence(
                run.task,
                ProcessStatus::Running,
                &reason,
                ProcessGroupExitEvidence::ConfirmedExited,
                None,
            )
            .unwrap()
            .unwrap();
    }

    fn run_on(&self, resource: &str) -> Option<ActiveRun> {
        self.store
            .resource(self.resource(resource))
            .unwrap()
            .unwrap()
            .run
    }

    /// Finish cleanup of the run on `resource`
    fn clean(&self, resource: &str) {
        let run = self.run_on(resource).unwrap();
        let RunPhase::Cleaning { attempt } = run.phase else {
            panic!("{resource} is not cleaning: {run:?}");
        };
        self.store
            .apply_cleanup_result(run.resource, run.task, attempt, Ok(()))
            .unwrap();
    }

    fn events(&self, job: JobId) -> Vec<JobEventKind> {
        self.store
            .job_events(job)
            .unwrap()
            .into_iter()
            .map(|event| event.event)
            .collect()
    }
}

fn spec(priority: Priority, steps: usize) -> Value {
    json!({
        "api_version": 1,
        "thread": "77777777-7777-4777-8777-777777777777",
        "name": "bench",
        "cwd": "/tmp",
        "priority": priority,
        "preempt": { "mode": "yield" },
        "steps": vec![json!({ "type": "task", "command": ["python", "bench.py"] }); steps]
    })
}

fn new_job(id: JobId, machine: MachineId, spec: Value) -> NewJob {
    NewJob {
        id,
        machine,
        origin: machine,
        spec: JobSpec::parse_value(&spec).unwrap(),
        env: TaskEnv {
            path: "/usr/bin:/bin".into(),
            home: "/tmp".into(),
        },
    }
}

/// Check every queue invariant against the stored rows
fn assert_invariants(store: &Store) {
    let conn = &store.conn;
    let mut statement = conn
        .prepare(
            "SELECT machine, priority, position FROM resource_jobs
             WHERE position IS NOT NULL ORDER BY machine, priority, position",
        )
        .unwrap();
    let slots: Vec<(String, i64, i64)> = statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut expected = 0;
    let mut level: Option<(String, i64)> = None;
    for (machine, priority, position) in slots {
        let key = Some((machine, priority));
        if key != level {
            level = key;
            expected = 0;
        }
        expected += 1;
        assert_eq!(position, expected, "slots of {level:?} are dense");
    }

    // a job is active exactly when it owns a launching, executing, or stopping run
    let mismatched: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resource_jobs j
             LEFT JOIN resources r ON r.run_job = j.id
                AND r.run_phase IN ('launching', 'executing', 'stopping')
             WHERE (j.state = 'active') != (r.id IS NOT NULL)
                OR (j.state = 'active' AND j.active_resource != r.id)",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(mismatched, 0, "active jobs match their runs");

    // each run's task belongs to the run's job and number
    let foreign_runs: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM resources r JOIN tasks t ON t.id = r.run_task
             WHERE t.resource_job_id IS NOT r.run_job OR t.run_number IS NOT r.run_number
                OR t.step_index IS NOT r.run_step",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(foreign_runs, 0, "run tasks match their active runs");
}

#[test]
fn jobs_serve_fifo_within_a_level_and_high_first() {
    let fixture = Fixture::new();
    let low = fixture.submit(Priority::Low);
    let medium_a = fixture.submit(Priority::Medium);
    let high = fixture.submit(Priority::High);
    let medium_b = fixture.submit(Priority::Medium);
    assert_eq!(
        fixture.order(),
        vec![
            (high, Priority::High, 1),
            (medium_a, Priority::Medium, 1),
            (medium_b, Priority::Medium, 2),
            (low, Priority::Low, 1),
        ]
    );
    assert_invariants(&fixture.store);
}

#[test]
fn every_placement_lands_where_the_table_says() {
    use Priority::{High, Low, Medium};

    // the queue is H1 | M1 M2 M3 | L1; the job moved is index `job`
    struct Case {
        name: &'static str,
        job: usize,
        placement: fn(&[JobId]) -> Placement,
        expected: [(usize, Priority); 5],
    }
    let cases = [
        Case {
            name: "--front",
            job: 3,
            placement: |_| Placement::Edge {
                priority: None,
                end: LevelEnd::Front,
            },
            expected: [(0, High), (3, Medium), (1, Medium), (2, Medium), (4, Low)],
        },
        Case {
            name: "--back",
            job: 1,
            placement: |_| Placement::Edge {
                priority: None,
                end: LevelEnd::Back,
            },
            expected: [(0, High), (2, Medium), (3, Medium), (1, Medium), (4, Low)],
        },
        Case {
            name: "--priority high",
            job: 2,
            placement: |_| Placement::Edge {
                priority: Some(Priority::High),
                end: LevelEnd::Back,
            },
            expected: [(0, High), (2, High), (1, Medium), (3, Medium), (4, Low)],
        },
        Case {
            name: "--priority medium on its own level goes to the back",
            job: 1,
            placement: |_| Placement::Edge {
                priority: Some(Priority::Medium),
                end: LevelEnd::Back,
            },
            expected: [(0, High), (2, Medium), (3, Medium), (1, Medium), (4, Low)],
        },
        Case {
            name: "--priority high --front",
            job: 4,
            placement: |_| Placement::Edge {
                priority: Some(Priority::High),
                end: LevelEnd::Front,
            },
            expected: [(4, High), (0, High), (1, Medium), (2, Medium), (3, Medium)],
        },
        Case {
            name: "--before within a level",
            job: 3,
            placement: |ids| Placement::Relative {
                target: ids[2],
                side: Side::Before,
                expect: None,
            },
            expected: [(0, High), (1, Medium), (3, Medium), (2, Medium), (4, Low)],
        },
        Case {
            name: "--after within a level",
            job: 1,
            placement: |ids| Placement::Relative {
                target: ids[2],
                side: Side::After,
                expect: None,
            },
            expected: [(0, High), (2, Medium), (1, Medium), (3, Medium), (4, Low)],
        },
        Case {
            name: "--before across levels takes the target's level",
            job: 4,
            placement: |ids| Placement::Relative {
                target: ids[2],
                side: Side::Before,
                expect: None,
            },
            expected: [
                (0, High),
                (1, Medium),
                (4, Medium),
                (2, Medium),
                (3, Medium),
            ],
        },
        Case {
            name: "--after across levels takes the target's level",
            job: 1,
            placement: |ids| Placement::Relative {
                target: ids[0],
                side: Side::After,
                expect: None,
            },
            expected: [(0, High), (1, High), (2, Medium), (3, Medium), (4, Low)],
        },
        Case {
            name: "--before with a matching --priority",
            job: 0,
            placement: |ids| Placement::Relative {
                target: ids[4],
                side: Side::Before,
                expect: Some(Priority::Low),
            },
            expected: [(1, Medium), (2, Medium), (3, Medium), (0, Low), (4, Low)],
        },
    ];
    for case in cases {
        let fixture = Fixture::new();
        let ids = [
            fixture.submit(High),
            fixture.submit(Medium),
            fixture.submit(Medium),
            fixture.submit(Medium),
            fixture.submit(Low),
        ];
        let digest_before = fixture.job(ids[case.job]).digest;
        let result = fixture
            .move_job(ids[case.job], (case.placement)(&ids))
            .unwrap_or_else(|error| panic!("{}: {error}", case.name));

        let order: Vec<(JobId, Priority)> = fixture
            .order()
            .into_iter()
            .map(|(id, level, _)| (id, level))
            .collect();
        let expected: Vec<(JobId, Priority)> = case
            .expected
            .iter()
            .map(|(index, level)| (ids[*index], *level))
            .collect();
        assert_eq!(order, expected, "{}", case.name);
        let moved = fixture.job(ids[case.job]);
        assert_eq!(
            (result.priority, Some(result.position)),
            (moved.priority, moved.position),
            "{}",
            case.name
        );
        assert_eq!(
            moved.digest, digest_before,
            "{}: a move keeps the spec",
            case.name
        );
        assert_invariants(&fixture.store);
    }
}

#[test]
fn refused_moves_change_nothing() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let target = fixture.submit(Priority::High);
    let done = fixture.submit(Priority::Low);
    fixture.cancel(done);
    let foreign = other.submit(Priority::Medium);
    let missing = JobId::new();
    let before = fixture.order();

    let relative = |target, expect| Placement::Relative {
        target,
        side: Side::Before,
        expect,
    };
    let refusals = [
        (
            job,
            relative(missing, None),
            MoveRefusal::TargetNotFound { target: missing },
        ),
        (
            job,
            relative(foreign, None),
            MoveRefusal::TargetNotFound { target: foreign },
        ),
        (
            job,
            relative(done, None),
            MoveRefusal::TargetTerminal { target: done },
        ),
        (job, relative(job, None), MoveRefusal::SelfTarget),
        (
            job,
            relative(target, Some(Priority::Low)),
            MoveRefusal::LevelMismatch {
                target,
                requested: Priority::Low,
                actual: Priority::High,
            },
        ),
    ];
    for (moved, placement, reason) in refusals {
        let error = fixture.move_job(moved, placement).unwrap_err();
        assert!(
            matches!(&error, AppError::Queue(QueueError::MoveRefused { reason: got, .. }) if *got == reason),
            "{placement:?}: {error:?}"
        );
    }
    let front = Placement::Edge {
        priority: None,
        end: LevelEnd::Front,
    };
    assert!(matches!(
        fixture.move_job(done, front),
        Err(AppError::Queue(QueueError::JobTerminal { .. }))
    ));
    assert!(matches!(
        fixture.move_job(missing, front),
        Err(AppError::Queue(QueueError::JobNotFound { .. }))
    ));
    assert!(matches!(
        fixture.move_job(foreign, front),
        Err(AppError::Queue(QueueError::JobNotFound { .. })),
    ));
    assert_eq!(fixture.order(), before);
    assert_invariants(&fixture.store);
}

#[test]
fn moving_the_active_job_changes_the_level_preemption_compares() {
    let fixture = Fixture::new();
    let running = fixture.submit_spec(spec(Priority::Low, 1));
    fixture.start(running, "gpu0", Utc::now());
    let pinned = json!({ "resource": "gpu0" });
    let mut head = spec(Priority::Medium, 1);
    head.as_object_mut()
        .unwrap()
        .extend(pinned.as_object().unwrap().clone());
    fixture.submit_spec(head);

    let thresholds = NoticeThresholds::default();
    let snapshot = fixture
        .store
        .queue_snapshot(fixture.machine, Utc::now(), thresholds)
        .unwrap();
    assert!(
        decide(&snapshot).preemption.is_some(),
        "low yields to medium"
    );

    let result = fixture
        .move_job(
            running,
            Placement::Edge {
                priority: Some(Priority::High),
                end: LevelEnd::Back,
            },
        )
        .unwrap();
    assert_eq!((result.priority, result.position), (Priority::High, 1));
    assert!(matches!(
        fixture.job(running).state,
        JobState::Active { .. }
    ));
    let snapshot = fixture
        .store
        .queue_snapshot(fixture.machine, Utc::now(), thresholds)
        .unwrap();
    assert_eq!(
        decide(&snapshot).preemption,
        None,
        "a high run is not preempted for medium"
    );
    assert_invariants(&fixture.store);
}

#[test]
fn a_preempted_job_keeps_its_slot_ahead_of_later_arrivals() {
    let fixture = Fixture::new();
    let first = fixture.submit(Priority::Low);
    let run = fixture.start(first, "gpu0", Utc::now());
    let later = fixture.submit(Priority::Low);
    fixture
        .store
        .commit_stop(run.resource, run.task, StopCause::Yield, Utc::now())
        .unwrap();
    fixture.exit(&run, ExitReason::Exit { code: 75 });

    assert_eq!(fixture.job(first).state, JobState::Queued { resume: true });
    assert_eq!(fixture.job(first).step, StepIndex::FIRST);
    assert_eq!(fixture.ids(), vec![first, later]);
    assert_eq!(fixture.events(first), vec![JobEventKind::JobPreempted]);
    assert_invariants(&fixture.store);

    // after cleanup the preempted job, not the later arrival, runs next and resumes
    fixture.clean("gpu0");
    let snapshot = fixture
        .store
        .queue_snapshot(fixture.machine, Utc::now(), NoticeThresholds::default())
        .unwrap();
    let launches = decide(&snapshot).launches;
    assert_eq!(launches[0].job, first);
    let resumed = fixture.reserve(first, "gpu0");
    assert!(resumed.resume);
    assert_eq!(resumed.run_number.get(), 2);
    assert_ne!(resumed.task, run.task, "a resumed step gets a new task");
}

#[test]
fn slots_stay_dense_after_cancel_and_completion() {
    let fixture = Fixture::new();
    let jobs: Vec<JobId> = (0..4).map(|_| fixture.submit(Priority::Medium)).collect();
    fixture.cancel(jobs[1]);
    assert_eq!(
        fixture.order(),
        vec![
            (jobs[0], Priority::Medium, 1),
            (jobs[2], Priority::Medium, 2),
            (jobs[3], Priority::Medium, 3),
        ]
    );
    let run = fixture.start(jobs[0], "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });
    assert_eq!(fixture.job(jobs[0]).state, JobState::Succeeded);
    assert_eq!(fixture.job(jobs[0]).position, None);
    assert_eq!(
        fixture.order(),
        vec![
            (jobs[2], Priority::Medium, 1),
            (jobs[3], Priority::Medium, 2)
        ]
    );
    assert_invariants(&fixture.store);
}

/// How a classification case ends its run
#[derive(Debug, Clone, Copy)]
enum End {
    Exit(i32),
    Signal,
    WorkerCancel,
    Lost,
}

#[test]
fn every_classification_row_moves_the_stored_job() {
    use JobEventKind::{JobCancelled, JobFailed, JobPreempted, JobSucceeded};
    use StopCause::{Restart, UserCancel, Yield};

    struct Case {
        end: End,
        cause: Option<StopCause>,
        last_step: bool,
        status: ProcessStatus,
        job: fn(TaskId) -> JobState,
        event: Option<JobEventKind>,
    }
    let cases = [
        Case {
            end: End::Exit(0),
            cause: None,
            last_step: true,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Succeeded,
            event: Some(JobSucceeded),
        },
        Case {
            end: End::Exit(0),
            cause: Some(UserCancel),
            last_step: true,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Succeeded,
            event: Some(JobSucceeded),
        },
        Case {
            end: End::Exit(0),
            cause: None,
            last_step: false,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Queued { resume: false },
            event: None,
        },
        Case {
            end: End::Exit(0),
            cause: Some(Yield),
            last_step: false,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Queued { resume: false },
            event: None,
        },
        Case {
            end: End::Exit(0),
            cause: Some(Restart),
            last_step: false,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Queued { resume: false },
            event: None,
        },
        Case {
            end: End::Exit(0),
            cause: Some(UserCancel),
            last_step: false,
            status: ProcessStatus::Succeeded,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
        Case {
            end: End::Exit(75),
            cause: Some(Yield),
            last_step: true,
            status: ProcessStatus::Preempted,
            job: |_| JobState::Queued { resume: true },
            event: Some(JobPreempted),
        },
        Case {
            end: End::Exit(75),
            cause: Some(UserCancel),
            last_step: true,
            status: ProcessStatus::Cancelled,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
        Case {
            end: End::Exit(75),
            cause: None,
            last_step: true,
            status: ProcessStatus::Failed,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Exit(75),
            cause: Some(Restart),
            last_step: true,
            status: ProcessStatus::Failed,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::WorkerCancel,
            cause: Some(Restart),
            last_step: true,
            status: ProcessStatus::Preempted,
            job: |_| JobState::Queued { resume: false },
            event: Some(JobPreempted),
        },
        Case {
            end: End::WorkerCancel,
            cause: Some(UserCancel),
            last_step: true,
            status: ProcessStatus::Cancelled,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
        Case {
            end: End::Exit(1),
            cause: None,
            last_step: true,
            status: ProcessStatus::Failed,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Signal,
            cause: Some(Yield),
            last_step: true,
            status: ProcessStatus::Failed,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Exit(1),
            cause: Some(Restart),
            last_step: false,
            status: ProcessStatus::Failed,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Exit(1),
            cause: Some(UserCancel),
            last_step: true,
            status: ProcessStatus::Cancelled,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
        Case {
            end: End::Signal,
            cause: Some(UserCancel),
            last_step: false,
            status: ProcessStatus::Cancelled,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
        Case {
            end: End::Lost,
            cause: None,
            last_step: true,
            status: ProcessStatus::Lost,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Lost,
            cause: Some(Yield),
            last_step: false,
            status: ProcessStatus::Lost,
            job: |run| JobState::Failed { run },
            event: Some(JobFailed),
        },
        Case {
            end: End::Lost,
            cause: Some(UserCancel),
            last_step: true,
            status: ProcessStatus::Lost,
            job: |_| JobState::Cancelled,
            event: Some(JobCancelled),
        },
    ];

    for case in cases {
        let label = format!(
            "{:?} {:?} last_step={}",
            case.end, case.cause, case.last_step
        );
        let fixture = Fixture::new();
        let steps = if case.last_step { 1 } else { 2 };
        let job = fixture.submit_spec(spec(Priority::Medium, steps));
        let behind = fixture.submit(Priority::Medium);
        let run = fixture.start(job, "gpu0", Utc::now());
        match case.cause {
            Some(StopCause::UserCancel) => {
                assert!(
                    matches!(fixture.cancel(job), CancelResult::Stopping { task, .. } if task == run.task)
                );
            }
            Some(cause) => {
                fixture
                    .store
                    .commit_stop(run.resource, run.task, cause, Utc::now())
                    .unwrap();
            }
            None => {}
        }
        match case.end {
            End::Exit(code) => fixture.exit(&run, ExitReason::Exit { code }),
            End::Signal => fixture.exit(&run, ExitReason::Signal { signal: 9 }),
            End::WorkerCancel => fixture.exit(&run, ExitReason::Cancelled),
            End::Lost => {
                fixture
                    .store
                    .cas_status(run.task, ProcessStatus::Running, ProcessStatus::Lost)
                    .unwrap()
                    .unwrap();
            }
        }

        let task = fixture.store.require_task(run.task).unwrap();
        assert_eq!(task.status(), case.status, "{label}: task outcome");
        if case.status == ProcessStatus::Cancelled {
            assert_eq!(
                task.state,
                TaskState::Finished {
                    reason: ExitReason::Cancelled
                }
            );
        }
        let record = fixture.job(job);
        assert_eq!(
            record.state,
            (case.job)(run.task),
            "{label}: job transition"
        );
        // only a step that succeeds before the last moves the job to its next step
        let advanced = matches!(case.end, End::Exit(0))
            && !case.last_step
            && matches!(record.state, JobState::Queued { .. });
        assert_eq!(
            record.step,
            StepIndex::new(u32::from(advanced)),
            "{label}: job step"
        );
        let expected_events: Vec<_> = case.event.into_iter().collect();
        assert_eq!(fixture.events(job), expected_events, "{label}: job events");
        if let Some(event) = fixture.store.job_events(job).unwrap().first() {
            assert_eq!(event.seq, 1);
            assert_eq!(event.run.unwrap().task, run.task);
        }
        assert_eq!(
            fixture.run_on("gpu0").unwrap().phase,
            RunPhase::Cleaning { attempt: 1 },
            "{label}: the resource stays reserved for cleanup"
        );
        if record.state.is_terminal() {
            assert_eq!(
                fixture.order(),
                vec![(behind, Priority::Medium, 1)],
                "{label}"
            );
        } else {
            assert_eq!(
                fixture.ids(),
                vec![job, behind],
                "{label}: the job keeps its slot"
            );
        }
        assert_no_task_events(&fixture.store, run.task);
        assert_invariants(&fixture.store);
    }
}

fn assert_no_task_events(store: &Store, task: TaskId) {
    for table in ["executor_outbox", "origin_inbox"] {
        let rows: i64 = store
            .conn
            .query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE task_id = ?1"),
                [task.to_string()],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0, "a run task produces no {table} rows");
    }
}

#[test]
fn a_late_terminal_commit_of_an_old_run_changes_nothing() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });
    let duplicate = fixture
        .store
        .cas_exit_with_evidence(
            run.task,
            ProcessStatus::Running,
            &ExitReason::Exit { code: 1 },
            ProcessGroupExitEvidence::ConfirmedExited,
            None,
        )
        .unwrap();
    assert!(duplicate.is_none());
    assert_eq!(fixture.job(job).state, JobState::Succeeded);
    assert_eq!(fixture.events(job), vec![JobEventKind::JobSucceeded]);
}

#[test]
fn an_ordinary_exit_75_stays_a_failure() {
    let fixture = Fixture::new();
    let id = TaskId::new();
    let row = crate::store::new_queued_task(crate::store::NewTask {
        id,
        name: crate::domain::TaskName::parse("test task").unwrap(),
        thread: crate::domain::ThreadId(uuid::Uuid::now_v7()),
        workload: crate::domain::Workload::Task(crate::domain::TaskWorkload {
            command: crate::invocation::CommandLine::try_from_argv(vec!["true".into()]).unwrap(),
        }),
        cwd: PathBuf::from("/tmp"),
        timeout: std::time::Duration::from_secs(3600),
        env: TaskEnv {
            path: "/bin".into(),
            home: "/tmp".into(),
        },
        binary: PathBuf::from("/usr/bin/true"),
    });
    fixture.store.insert_task(&row).unwrap();
    fixture
        .store
        .cas_status(id, ProcessStatus::Queued, ProcessStatus::Running)
        .unwrap();
    fixture
        .store
        .cas_exit(id, ProcessStatus::Running, &ExitReason::Exit { code: 75 })
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture.store.require_task(id).unwrap().status(),
        ProcessStatus::Failed
    );
}

#[test]
fn an_abandoned_launch_returns_the_job_to_its_slot_without_a_failure() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let reserved = fixture.reserve(job, "gpu1");
    assert!(
        fixture
            .store
            .abandon_launch(reserved.resource, reserved.task)
            .unwrap()
    );
    let task = fixture.store.require_task(reserved.task).unwrap();
    assert_eq!(task.status(), ProcessStatus::Failed);
    assert_eq!(fixture.job(job).state, JobState::Queued { resume: false });
    assert_eq!(fixture.job(job).step, StepIndex::FIRST);
    assert!(fixture.events(job).is_empty());
    assert_eq!(
        fixture.run_on("gpu1").unwrap().phase,
        RunPhase::Cleaning { attempt: 1 }
    );
    assert_invariants(&fixture.store);

    // a launch the worker already started cannot be abandoned
    fixture.clean("gpu1");
    let run = fixture.start(job, "gpu1", Utc::now());
    assert!(
        fixture
            .store
            .abandon_launch(run.resource, run.task)
            .is_err()
    );
}

#[test]
fn a_spawn_failure_fails_the_job() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let reserved = fixture.reserve(job, "gpu0");
    fixture
        .store
        .cas_exit(
            reserved.task,
            ProcessStatus::Queued,
            &ExitReason::SpawnFailed {
                message: "fork".into(),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        fixture.job(job).state,
        JobState::Failed { run: reserved.task }
    );
    assert_eq!(fixture.events(job), vec![JobEventKind::JobFailed]);
}

/// A run reports through its job's events and owns no terminal callback, so
/// a finished run never reads as an undelivered callback. It used to read as
/// `pending` forever, which held `daemon stop --yes` until its time limit
#[test]
fn a_finished_run_owns_no_terminal_callback() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });
    let row = fixture.store.require_task(run.task).unwrap();
    assert!(row.state.is_terminal(), "{row:?}");

    assert!(!fixture.store.has_pending_terminal_callbacks().unwrap());
    let presentations = fixture.store.task_presentations(&[run.task]).unwrap();
    assert_eq!(presentations[&run.task].terminal_callback, None);
}

/// The CLI decodes queue responses into the shapes the store serialized
#[test]
fn queue_responses_decode_into_their_typed_shapes() {
    use super::interface::{JobDetail, JobList, QueueRequest, ResourceList};

    let fixture = Fixture::new();
    let job = fixture.submit_spec(spec(Priority::High, 2));
    let waiting = fixture.submit(Priority::Low);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture
        .store
        .commit_stop(run.resource, run.task, StopCause::Yield, Utc::now())
        .unwrap();
    let env = TaskEnv {
        path: "/bin".into(),
        home: "/tmp".into(),
    };
    let request = |request: QueueRequest| {
        fixture
            .store
            .queue_request(fixture.machine, &request, &env)
            .unwrap()
    };

    let resources: ResourceList = serde_json::from_value(request(QueueRequest::Resources)).unwrap();
    assert_eq!(
        resources.resources,
        fixture.store.resources_on(fixture.machine).unwrap()
    );
    let jobs: JobList = serde_json::from_value(request(QueueRequest::Jobs)).unwrap();
    assert_eq!(jobs.jobs, vec![fixture.job(job), fixture.job(waiting)]);
    let detail: JobDetail = serde_json::from_value(request(QueueRequest::Show { job })).unwrap();
    assert_eq!(detail.job, fixture.job(job));
    assert_eq!(detail.active_run, fixture.run_on("gpu0"));
    assert_eq!(detail.runs.len(), 1);
    assert_eq!(detail.runs[0].task, run.task);
}

#[test]
fn cancel_queued_and_terminal_jobs() {
    let fixture = Fixture::new();
    let queued = fixture.submit(Priority::Low);
    assert_eq!(fixture.cancel(queued), CancelResult::Cancelled);
    assert_eq!(fixture.job(queued).state, JobState::Cancelled);
    assert_eq!(fixture.events(queued), vec![JobEventKind::JobCancelled]);
    // a terminal result wins over a later cancel
    assert_eq!(
        fixture.cancel(queued),
        CancelResult::AlreadyTerminal {
            state: "cancelled".into()
        }
    );
    assert_eq!(fixture.events(queued), vec![JobEventKind::JobCancelled]);

    // a job requeued by preemption is cancelled at once
    let job = fixture.submit(Priority::Low);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture
        .store
        .commit_stop(run.resource, run.task, StopCause::Restart, Utc::now())
        .unwrap();
    fixture.exit(&run, ExitReason::Cancelled);
    assert_eq!(fixture.cancel(job), CancelResult::Cancelled);
    assert_eq!(fixture.job(job).state, JobState::Cancelled);
    assert_invariants(&fixture.store);
}

#[test]
fn a_user_cancel_upgrades_a_pending_yield_and_survives_later_requests() {
    for task_cancel in [false, true] {
        let fixture = Fixture::new();
        let job = fixture.submit(Priority::Low);
        let run = fixture.start(job, "gpu0", Utc::now());
        let yielded = fixture
            .store
            .commit_stop(run.resource, run.task, StopCause::Yield, Utc::now())
            .unwrap();
        let RunPhase::Stopping { requested_at, .. } = yielded.phase else {
            panic!("{yielded:?}");
        };
        if task_cancel {
            fixture.store.request_cancel(run.task).unwrap();
        } else {
            fixture.cancel(job);
        }
        let after = fixture
            .store
            .commit_stop(run.resource, run.task, StopCause::Yield, Utc::now())
            .unwrap();
        assert!(
            matches!(
                after.phase,
                RunPhase::Stopping { cause: StopCause::UserCancel, requested_at: at, .. } if at == requested_at
            ),
            "task cancel {task_cancel}"
        );
        fixture.exit(&run, ExitReason::Exit { code: 75 });
        assert_eq!(fixture.job(job).state, JobState::Cancelled);
    }
}
#[test]
fn a_launching_run_can_be_cancelled_but_not_preempted() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Low);
    let reserved = fixture.reserve(job, "gpu0");
    let run = reserved;
    assert!(matches!(
        fixture
            .store
            .commit_stop(run.resource, run.task, StopCause::Yield, Utc::now()),
        Err(AppError::Queue(QueueError::StaleRun { .. }))
    ));
    fixture.cancel(job);
    // stored times keep milliseconds
    let started = Utc::now().trunc_subsecs(3);
    let confirmed = fixture
        .store
        .mark_run_executing(run.resource, run.task, started)
        .unwrap();
    assert_eq!(
        confirmed.phase,
        RunPhase::Stopping {
            started_at: Some(started),
            cause: StopCause::UserCancel,
            requested_at: match fixture.run_on("gpu0").unwrap().phase {
                RunPhase::Stopping { requested_at, .. } => requested_at,
                other => panic!("{other:?}"),
            },
        }
    );
    // the queued run task is cancelled before its worker starts
    fixture.store.request_cancel(run.task).unwrap();
    assert_eq!(fixture.job(job).state, JobState::Cancelled);
}

#[test]
fn submit_replays_by_digest_and_refuses_a_changed_spec() {
    let fixture = Fixture::new();
    let id = JobId::new();
    let first = fixture
        .store
        .submit_job(&new_job(id, fixture.machine, spec(Priority::High, 1)))
        .unwrap();
    let replay = fixture
        .store
        .submit_job(&new_job(id, fixture.machine, spec(Priority::High, 1)))
        .unwrap();
    assert_eq!(first, replay);
    assert_eq!(fixture.ids(), vec![id]);

    let conflict = fixture
        .store
        .submit_job(&new_job(id, fixture.machine, spec(Priority::Low, 1)))
        .unwrap_err();
    assert!(matches!(conflict, AppError::Queue(QueueError::JobConflict { job }) if job == id));

    // a cancelled job's record stays, so a delayed submit cannot recreate it
    fixture.cancel(id);
    let delayed = fixture
        .store
        .submit_job(&new_job(id, fixture.machine, spec(Priority::High, 1)))
        .unwrap();
    assert_eq!(delayed.state, "cancelled");
    assert_eq!(delayed.position, None);
    assert!(fixture.ids().is_empty());
}

#[test]
fn submit_resolves_a_pinned_resource_and_refuses_an_unknown_one() {
    let fixture = Fixture::new();
    let mut pinned = spec(Priority::Medium, 1);
    pinned["resource"] = json!("gpu1");
    let id = fixture.submit_spec(pinned);
    assert_eq!(
        fixture.job(id).target,
        Target::Pinned(fixture.resource("gpu1"))
    );

    let mut unknown = spec(Priority::Medium, 1);
    unknown["resource"] = json!("gpu9");
    let error = fixture
        .store
        .submit_job(&new_job(JobId::new(), fixture.machine, unknown))
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Queue(QueueError::ResourceNotFound { .. })
    ));

    let refused = fixture.store.reserve_run(
        fixture.machine,
        id,
        fixture.resource("gpu0"),
        TaskId::new(),
        PathBuf::from("/usr/bin/true"),
        Utc::now(),
    );
    assert!(refused.is_err(), "a pinned job runs only on its resource");
    assert_invariants(&fixture.store);
}

#[test]
fn operations_replay_and_refuse_reused_ids() {
    let fixture = Fixture::new();
    let a = fixture.submit(Priority::Medium);
    let b = fixture.submit(Priority::Medium);
    let operation = OperationId::new();
    let front = Placement::Edge {
        priority: None,
        end: LevelEnd::Front,
    };
    let first = fixture
        .store
        .move_job(fixture.machine, operation, b, front)
        .unwrap();
    assert_eq!(fixture.ids(), vec![b, a]);

    // another mutation, then a replay: the stored result returns and nothing reapplies
    fixture.move_job(a, front).unwrap();
    assert_eq!(fixture.ids(), vec![a, b]);
    let replay = fixture
        .store
        .move_job(fixture.machine, operation, b, front)
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(fixture.ids(), vec![a, b]);

    let reused = fixture
        .store
        .move_job(
            fixture.machine,
            operation,
            b,
            Placement::Edge {
                priority: None,
                end: LevelEnd::Back,
            },
        )
        .unwrap_err();
    assert!(matches!(
        reused,
        AppError::Queue(QueueError::OperationConflict { .. })
    ));
    let reused_kind = fixture
        .store
        .cancel_job(fixture.machine, operation, b, Utc::now())
        .unwrap_err();
    assert!(matches!(
        reused_kind,
        AppError::Queue(QueueError::OperationConflict { .. })
    ));

    let cancel = OperationId::new();
    let cancelled = fixture
        .store
        .cancel_job(fixture.machine, cancel, a, Utc::now())
        .unwrap();
    assert_eq!(cancelled, CancelResult::Cancelled);
    let replayed = fixture
        .store
        .cancel_job(fixture.machine, cancel, a, Utc::now())
        .unwrap();
    assert_eq!(
        replayed,
        CancelResult::Cancelled,
        "a replay returns the stored result"
    );
    assert_eq!(fixture.events(a), vec![JobEventKind::JobCancelled]);
}

#[test]
fn a_second_active_run_on_a_resource_or_job_is_rejected() {
    let fixture = Fixture::new();
    let a = fixture.submit(Priority::Medium);
    let b = fixture.submit(Priority::Medium);
    fixture.reserve(a, "gpu0");
    let busy = fixture.store.reserve_run(
        fixture.machine,
        b,
        fixture.resource("gpu0"),
        TaskId::new(),
        PathBuf::from("/usr/bin/true"),
        Utc::now(),
    );
    assert!(matches!(
        busy,
        Err(AppError::Queue(QueueError::Invariant { .. }))
    ));
    let twice = fixture.store.reserve_run(
        fixture.machine,
        a,
        fixture.resource("gpu1"),
        TaskId::new(),
        PathBuf::from("/usr/bin/true"),
        Utc::now(),
    );
    assert!(matches!(
        twice,
        Err(AppError::Queue(QueueError::Invariant { .. }))
    ));

    // the constraints hold even against a write that skips the store's checks
    let run_job: String = fixture
        .store
        .conn
        .query_row(
            "SELECT run_job FROM resources WHERE id = ?1",
            [fixture.resource("gpu0").to_string()],
            |row| row.get(0),
        )
        .unwrap();
    let duplicate = fixture.store.conn.execute(
        "UPDATE resources SET run_job = ?1, run_task = ?2, run_number = 1, run_step = 0,
            run_phase = 'cleaning', run_cleanup_attempt = 1
         WHERE id = ?3",
        params![
            run_job,
            uuid::Uuid::now_v7().to_string(),
            fixture.resource("gpu1").to_string()
        ],
    );
    assert!(duplicate.is_err(), "one active run per job");
    let partial = fixture.store.conn.execute(
        "UPDATE resources SET run_phase = 'executing' WHERE id = ?1",
        [fixture.resource("gpu1").to_string()],
    );
    assert!(partial.is_err(), "a run phase needs its run");
    assert_invariants(&fixture.store);
}

#[test]
fn a_job_cannot_start_again_while_its_previous_run_is_cleaned_up() {
    let fixture = Fixture::new();
    let job = fixture.submit_spec(spec(Priority::Medium, 2));
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });
    let snapshot = fixture
        .store
        .queue_snapshot(fixture.machine, Utc::now(), NoticeThresholds::default())
        .unwrap();
    assert!(decide(&snapshot).launches.is_empty());
    assert!(
        snapshot
            .queue
            .iter()
            .all(|view| view.state == QueuedState::Queued)
    );
    let early = fixture.store.reserve_run(
        fixture.machine,
        job,
        fixture.resource("gpu1"),
        TaskId::new(),
        PathBuf::from("/usr/bin/true"),
        Utc::now(),
    );
    assert!(early.is_err());
    fixture.clean("gpu0");
    let next = fixture.reserve(job, "gpu1");
    assert_eq!(next.step, StepIndex::new(1));
    assert!(!next.resume);
}

#[test]
fn cleanup_results_apply_only_to_the_stored_run_and_attempt() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });

    let wrong_attempt = fixture
        .store
        .apply_cleanup_result(run.resource, run.task, 2, Ok(()));
    assert!(matches!(
        wrong_attempt,
        Err(AppError::Queue(QueueError::StaleRun { .. }))
    ));
    let wrong_task = fixture
        .store
        .apply_cleanup_result(run.resource, TaskId::new(), 1, Ok(()));
    assert!(matches!(
        wrong_task,
        Err(AppError::Queue(QueueError::StaleRun { .. }))
    ));

    // a crash during cleanup starts a new attempt; the old one is stale
    assert_eq!(
        fixture
            .store
            .begin_cleanup_attempt(run.resource, run.task)
            .unwrap(),
        2
    );
    assert!(
        fixture
            .store
            .apply_cleanup_result(run.resource, run.task, 1, Ok(()))
            .is_err()
    );

    let failure = CleanupFailure::Processes {
        failure: ProcessCleanupFailure::TargetsSurvived { survivors: vec![] },
    };
    let phase = fixture
        .store
        .apply_cleanup_result(run.resource, run.task, 2, Err(failure.clone()))
        .unwrap();
    let Some(RunPhase::Attention {
        id,
        failure: stored,
    }) = phase
    else {
        panic!("{phase:?}");
    };
    assert_eq!(stored, failure);
    assert_eq!(
        fixture.run_on("gpu0").unwrap().phase,
        RunPhase::Attention { id, failure }
    );
    let events = fixture.store.job_events(job).unwrap();
    assert_eq!(
        events.iter().map(|event| event.event).collect::<Vec<_>>(),
        vec![JobEventKind::JobSucceeded, JobEventKind::JobAttention]
    );
    assert_eq!(events[1].attention, Some(id));
    assert_eq!(events[1].seq, 2);

    // the attention blocks gpu0 while gpu1 keeps serving
    let other = fixture.submit(Priority::Medium);
    let snapshot = fixture
        .store
        .queue_snapshot(fixture.machine, Utc::now(), NoticeThresholds::default())
        .unwrap();
    let launches = decide(&snapshot).launches;
    assert_eq!(launches.len(), 1);
    assert_eq!(
        (launches[0].job, launches[0].resource),
        (other, fixture.resource("gpu1"))
    );
}

#[test]
fn release_names_the_attention_and_refuses_a_stale_one() {
    let fixture = Fixture::new();
    let job = fixture.submit(Priority::Medium);
    let run = fixture.start(job, "gpu0", Utc::now());
    fixture.exit(&run, ExitReason::Exit { code: 0 });
    let failure = CleanupFailure::ProcessGroupUnconfirmed;
    let Some(RunPhase::Attention { id, .. }) = fixture
        .store
        .apply_cleanup_result(run.resource, run.task, 1, Err(failure))
        .unwrap()
    else {
        panic!("attention expected");
    };

    let stale = crate::queue::AttentionId::new();
    let refused = fixture
        .store
        .release_resource_attention(fixture.machine, OperationId::new(), stale)
        .unwrap_err();
    assert!(matches!(
        refused,
        AppError::Queue(QueueError::AttentionNotFound { .. })
    ));
    assert!(fixture.run_on("gpu0").is_some());

    let operation = OperationId::new();
    let released = fixture
        .store
        .release_resource_attention(fixture.machine, operation, id)
        .unwrap();
    assert_eq!(released.resource, run.resource);
    assert!(fixture.run_on("gpu0").is_none());

    // a duplicate release replays; a new release of the same id is stale
    assert_eq!(
        fixture
            .store
            .release_resource_attention(fixture.machine, operation, id)
            .unwrap(),
        released
    );
    assert!(
        fixture
            .store
            .release_resource_attention(fixture.machine, OperationId::new(), id)
            .is_err()
    );
}

#[test]
fn detected_resources_are_created_once_and_keep_their_ids() {
    let fixture = Fixture::new();
    let before = fixture.store.resources_on(fixture.machine).unwrap();
    assert_eq!(before.len(), 2);
    fixture.detect(&["gpu0", "gpu1"]);
    fixture.detect(&["gpu0"]);
    let after = fixture.store.resources_on(fixture.machine).unwrap();
    assert_eq!(
        before, after,
        "repeated detection keeps UUIDs and never removes"
    );

    let unindexed = Fixture::new();
    let machine = MachineId::new();
    let created = unindexed
        .store
        .ensure_detected_resources(machine, &[DetectedResource::unindexed()])
        .unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].resource.name.as_str(), "gpu0");
    assert_eq!(created[0].resource.device, None);
}

#[test]
fn detection_moves_the_fallback_onto_the_first_detected_device() {
    for devices in [vec![1], vec![0, 1], vec![2, 3]] {
        let f = Fixture::new();
        let machine = MachineId::new();
        let unindexed = [DetectedResource::unindexed()];
        let fallback = f
            .store
            .ensure_detected_resources(machine, &unindexed)
            .unwrap()[0]
            .resource
            .id;
        let detected: Vec<_> = devices
            .iter()
            .map(|&device| DetectedResource {
                name: ResourceName::gpu(device),
                device: Some(device),
            })
            .collect();
        let resources = f
            .store
            .ensure_detected_resources(machine, &detected)
            .unwrap();
        assert_eq!(resources.len(), devices.len(), "{devices:?}");

        let first = devices[0];
        let adopted = f.store.resource(fallback).unwrap().unwrap();
        assert_eq!(adopted.resource.device, Some(first), "{devices:?}");
        assert_eq!(
            adopted.resource.name,
            ResourceName::gpu(first),
            "{devices:?}"
        );
        assert_eq!(adopted.origin, super::ResourceOrigin::DetectedDevice);
        assert_eq!(
            f.store
                .resolve_resource(
                    machine,
                    &crate::queue::ResourceSelector::Name(ResourceName::gpu(first))
                )
                .unwrap()
                .resource
                .id,
            fallback
        );

        // repeated detection is idempotent and never adds a fallback beside a device
        for again in [&detected[..], &unindexed[..]] {
            assert_eq!(
                f.store.ensure_detected_resources(machine, again).unwrap(),
                resources,
                "{devices:?}"
            );
        }
    }
}
#[test]
fn detection_never_renames_or_replaces_a_manual_resource() {
    let fixture = Fixture::new();
    let machine = MachineId::new();
    let fallback = fixture
        .store
        .ensure_detected_resources(machine, &[DetectedResource::unindexed()])
        .unwrap()[0]
        .resource
        .id;
    let manual = fixture
        .store
        .register_resource(machine, ResourceName::gpu(1), None)
        .unwrap();
    let detected = [DetectedResource {
        name: ResourceName::gpu(1),
        device: Some(1),
    }];
    let resources = fixture
        .store
        .ensure_detected_resources(machine, &detected)
        .unwrap();
    assert_eq!(resources.len(), 2);
    let fallback = fixture.store.resource(fallback).unwrap().unwrap();
    assert_eq!(fallback.resource.name, ResourceName::gpu(0));
    assert_eq!(fallback.resource.device, Some(1));
    assert_eq!(fallback.origin, super::ResourceOrigin::DetectedDevice);
    assert_eq!(
        fixture.store.resource(manual.resource.id).unwrap().unwrap(),
        manual
    );

    // without a fallback, a manual resource keeps a detected device's name
    let manual_machine = MachineId::new();
    let manual = fixture
        .store
        .register_resource(manual_machine, ResourceName::gpu(0), None)
        .unwrap();
    let detected = [0, 1].map(|device| DetectedResource {
        name: ResourceName::gpu(device),
        device: Some(device),
    });
    fixture
        .store
        .ensure_detected_resources(manual_machine, &detected)
        .unwrap();
    assert_eq!(
        fixture.store.resource(manual.resource.id).unwrap().unwrap(),
        manual
    );
}
#[test]
fn preemption_rechecks_the_first_job_with_an_eligible_victim() {
    for change in [
        "none",
        "cancel any",
        "victim raised",
        "preemption in flight",
    ] {
        let fixture = Fixture::new();
        let mut wait_spec = spec(Priority::Low, 1);
        wait_spec["preempt"] = json!({ "mode": "wait" });
        let wait = fixture.submit_spec(wait_spec);
        fixture.start(wait, "gpu0", Utc::now());
        let low = fixture.submit(Priority::Low);
        let victim = fixture.start(low, "gpu1", Utc::now());
        let mut pinned_spec = spec(Priority::High, 1);
        pinned_spec["resource"] = json!("gpu0");
        fixture.submit_spec(pinned_spec);
        let any = fixture.submit(Priority::High);
        let now = Utc::now();
        let stop = decide(
            &fixture
                .store
                .queue_snapshot(fixture.machine, now, NoticeThresholds::default())
                .unwrap(),
        )
        .preemption
        .unwrap();
        assert_eq!(stop.task, victim.task);
        match change {
            "cancel any" => {
                fixture.cancel(any);
            }
            "victim raised" => {
                fixture
                    .move_job(
                        low,
                        Placement::Edge {
                            priority: Some(Priority::High),
                            end: LevelEnd::Front,
                        },
                    )
                    .unwrap();
            }
            "preemption in flight" => {
                fixture
                    .store
                    .register_resource(fixture.machine, ResourceName::gpu(2), Some(2))
                    .unwrap();
                let other = fixture.submit(Priority::Low);
                let run = fixture.start(other, "gpu2", now);
                fixture
                    .store
                    .commit_stop(run.resource, run.task, StopCause::Yield, now)
                    .unwrap();
            }
            _ => {}
        }
        let committed = fixture.store.commit_preemption(stop, now).unwrap();
        assert_eq!(committed.is_some(), change == "none", "{change}");
        let phase = fixture.run_on("gpu1").unwrap().phase;
        if change == "none" {
            assert!(matches!(
                phase,
                RunPhase::Stopping {
                    cause: StopCause::Yield,
                    ..
                }
            ));
        } else {
            assert_eq!(phase, victim.phase, "{change}");
        }
    }
}

#[test]
fn register_refuses_a_taken_name_or_device() {
    let fixture = Fixture::new();
    let name = |raw| ResourceName::parse(raw).unwrap();
    let error = fixture
        .store
        .register_resource(fixture.machine, name("gpu0"), None)
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Queue(QueueError::ResourceNameTaken { .. })
    ));
    let error = fixture
        .store
        .register_resource(fixture.machine, name("spare"), Some(1))
        .unwrap_err();
    assert!(matches!(
        error,
        AppError::Queue(QueueError::DeviceTaken { device: 1, .. })
    ));
    let spare = fixture
        .store
        .register_resource(fixture.machine, name("spare"), None)
        .unwrap();
    assert_eq!(spare.resource.device, None);
    // a name is unique per machine, not across machines
    fixture
        .store
        .register_resource(MachineId::new(), name("spare"), Some(1))
        .unwrap();
}

#[test]
fn blocked_episodes_start_end_and_send_one_notice() {
    let fixture = Fixture::new();
    let running = fixture.submit(Priority::Low);
    fixture.start(running, "gpu0", Utc::now());
    let running = fixture.submit(Priority::Low);
    fixture.start(running, "gpu1", Utc::now());
    let thresholds = NoticeThresholds {
        after_yield: Duration::from_secs(1),
        after_wait: Duration::from_secs(1),
    };
    let head = fixture.submit(Priority::High);
    let other = fixture.submit(Priority::High);
    let since = Utc::now();
    let episode = fixture
        .store
        .record_blocked_head(fixture.machine, Some((head, since)))
        .unwrap()
        .unwrap();
    assert_eq!(episode.job, head);
    assert!(!episode.notified);

    // the same head keeps its start time
    let later = since + chrono::Duration::minutes(5);
    let same = fixture
        .store
        .record_blocked_head(fixture.machine, Some((head, later)))
        .unwrap()
        .unwrap();
    assert_eq!(same.blocked_since, episode.blocked_since);

    assert!(
        fixture
            .store
            .produce_job_blocked(fixture.machine, episode, thresholds, later)
            .unwrap()
    );
    assert!(
        !fixture
            .store
            .produce_job_blocked(fixture.machine, episode, thresholds, later)
            .unwrap()
    );
    assert!(
        fixture
            .store
            .blocked_episode(fixture.machine)
            .unwrap()
            .unwrap()
            .notified
    );

    // a new head starts a new episode; the old one cannot send
    let fresh = fixture
        .store
        .record_blocked_head(fixture.machine, Some((other, later)))
        .unwrap()
        .unwrap();
    assert_eq!(fresh.job, other);
    assert!(!fresh.notified);
    assert!(
        !fixture
            .store
            .produce_job_blocked(fixture.machine, episode, thresholds, later)
            .unwrap()
    );

    // an unsent notice is dropped when the episode ends
    assert_eq!(
        fixture
            .store
            .record_blocked_head(fixture.machine, None)
            .unwrap(),
        None
    );
    assert!(
        !fixture
            .store
            .produce_job_blocked(fixture.machine, fresh, thresholds, later)
            .unwrap()
    );
    assert_eq!(
        fixture.store.blocked_episode(fixture.machine).unwrap(),
        None
    );
}

#[test]
fn queue_state_survives_reopening_the_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let machine = MachineId::new();
    let (job, run) = {
        let store = Store::open(&path).unwrap();
        store
            .ensure_detected_resources(machine, &[DetectedResource::unindexed()])
            .unwrap();
        let job = JobId::new();
        store
            .submit_job(&new_job(job, machine, spec(Priority::Low, 2)))
            .unwrap();
        let resource = store.resources_on(machine).unwrap()[0].resource.id;
        let reserved = store
            .reserve_run(
                machine,
                job,
                resource,
                TaskId::new(),
                PathBuf::from("/bin/true"),
                Utc::now(),
            )
            .unwrap();
        store
            .cas_status(reserved.task, ProcessStatus::Queued, ProcessStatus::Running)
            .unwrap();
        let run = store
            .mark_run_executing(resource, reserved.task, Utc::now())
            .unwrap();
        store
            .commit_stop(resource, run.task, StopCause::Yield, Utc::now())
            .unwrap();
        (job, run)
    };
    let store = Store::open(&path).unwrap();
    let resources = store.resources_on(machine).unwrap();
    let stored = resources[0].run.clone().unwrap();
    assert_eq!(stored.task, run.task);
    assert!(matches!(
        stored.phase,
        RunPhase::Stopping {
            cause: StopCause::Yield,
            ..
        }
    ));
    let record = store.job(job).unwrap().unwrap();
    assert!(matches!(record.state, JobState::Active { .. }));
    let task = store.require_task(run.task).unwrap();
    let link: (String, i64, i64) = store
        .conn
        .query_row(
            "SELECT resource_job_id, run_number, step_index FROM tasks WHERE id = ?1",
            [run.task.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(link, (job.to_string(), 1, 0));
    assert_eq!(task.cwd, PathBuf::from("/tmp"));
}

#[test]
fn detection_defers_all_phases_of_an_active_fallback() {
    for phase in [
        "launching",
        "executing",
        "stopping",
        "cleaning",
        "attention",
    ] {
        let f = Fixture::new();
        let machine = MachineId::new();
        let resource = f
            .store
            .ensure_detected_resources(machine, &[DetectedResource::unindexed()])
            .unwrap()[0]
            .resource
            .id;
        let job = JobId::new();
        f.store
            .submit_job(&new_job(job, machine, spec(Priority::Low, 1)))
            .unwrap();
        let task = TaskId::new();
        let run = f
            .store
            .reserve_run(machine, job, resource, task, "/bin/true".into(), Utc::now())
            .unwrap();
        if phase != "launching" {
            f.store
                .cas_status(task, ProcessStatus::Queued, ProcessStatus::Running)
                .unwrap();
            f.store
                .mark_run_executing(resource, task, Utc::now())
                .unwrap();
        }
        if phase == "stopping" {
            f.store
                .commit_stop(resource, task, StopCause::Yield, Utc::now())
                .unwrap();
        }
        if phase == "cleaning" || phase == "attention" {
            f.exit(&run, ExitReason::Exit { code: 0 });
        }
        if phase == "attention" {
            f.store
                .apply_cleanup_result(
                    resource,
                    task,
                    1,
                    Err(CleanupFailure::ProcessGroupUnconfirmed),
                )
                .unwrap();
        }
        let detected = [0, 1].map(|device| DetectedResource {
            name: ResourceName::gpu(device),
            device: Some(device),
        });
        let resources = f
            .store
            .ensure_detected_resources(machine, &detected)
            .unwrap();
        assert_eq!(resources.len(), 1, "{phase}");
        assert_eq!(resources[0].resource.device, None, "{phase}");
        if phase == "cleaning" {
            f.store
                .apply_cleanup_result(resource, task, 1, Ok(()))
                .unwrap();
            assert_eq!(
                f.store
                    .ensure_detected_resources(machine, &detected)
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(
                f.store.resource(resource).unwrap().unwrap().resource.device,
                Some(0)
            );
        }
    }
}

#[test]
fn checkpoint_control_modes_ignore_umask() {
    const HELPER: &str = "HOMEBASED_TEST_CONTROL_UMASK";
    if std::env::var_os(HELPER).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "store::queue::tests::checkpoint_control_modes_ignore_umask",
                "--exact",
                "--nocapture",
            ])
            .env(HELPER, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{stdout}");
        // a stale test name filters out every test and still exits 0
        assert!(stdout.contains("1 passed"), "{stdout}");
        return;
    }
    // SAFETY: only this isolated test process creates files after changing its umask
    unsafe {
        nix::libc::umask(0o077);
    }
    use std::os::unix::fs::PermissionsExt;
    let f = Fixture::new();
    let home = crate::home::Home::resolve(Some(f._dir.path().to_path_buf())).unwrap();
    home.ensure().unwrap();
    let job = f.submit(Priority::Low);
    let run = f.reserve(job, "gpu0");
    let checkpoint = f.store.run_checkpoint(run.task).unwrap().unwrap();
    checkpoint.prepare(&home).unwrap();
    checkpoint.request_yield(&home).unwrap();
    checkpoint.prepare(&home).unwrap();
    assert_eq!(
        std::fs::metadata(checkpoint.control_dir(&home))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::metadata(checkpoint.control_dir(&home).join("yield"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
}

#[test]
fn ended_blocked_notice_is_suppressed_without_a_sequence_gap() {
    let f = Fixture::new();
    let low = f.submit(Priority::Low);
    let run = f.start(low, "gpu0", Utc::now());
    let mut queued = spec(Priority::High, 1);
    queued["resource"] = json!("gpu0");
    let head = f.submit_spec(queued);
    let now = Utc::now();
    let episode = f
        .store
        .record_blocked_head(f.machine, Some((head, now)))
        .unwrap()
        .unwrap();
    let thresholds = NoticeThresholds {
        after_yield: Duration::ZERO,
        after_wait: Duration::ZERO,
    };
    assert!(
        f.store
            .produce_job_blocked(f.machine, episode, thresholds, now)
            .unwrap()
    );
    assert!(f.store.job_notice_is_current(head, 1).unwrap());
    // the origin stays offline while the blocking episode ends and the job runs
    f.exit(&run, ExitReason::Exit { code: 0 });
    f.clean("gpu0");
    let head_run = f.start(head, "gpu0", Utc::now());
    f.exit(&head_run, ExitReason::Exit { code: 0 });
    f.store.record_blocked_head(f.machine, None).unwrap();
    assert!(!f.store.job_notice_is_current(head, 1).unwrap());
    assert_eq!(
        f.store
            .job_events(head)
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let suppressed: bool = f
        .store
        .conn
        .query_row(
            "SELECT suppressed_at IS NOT NULL FROM resource_job_events WHERE job_id=?1 AND seq=1",
            [head.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(suppressed);
}

#[test]
fn a_restart_marker_does_not_become_a_user_cancel() {
    let f = Fixture::new();
    let job = f.submit(Priority::Low);
    let run = f.start(job, "gpu0", Utc::now());
    f.store
        .commit_stop(run.resource, run.task, StopCause::Restart, Utc::now())
        .unwrap();
    f.store.signal_committed_run_stop(run.task).unwrap();
    f.exit(&run, ExitReason::Cancelled);
    assert_eq!(f.job(job).state, JobState::Queued { resume: false });
    assert_eq!(f.job(job).step, StepIndex::FIRST);
}

#[tokio::test(flavor = "multi_thread")]
async fn notice_ends_during_callback_preparation() {
    const LOCK_HELPER: &str = "HOMEBASED_TEST_NOTICE_LOCK";
    if let Some(path) = std::env::var_os(LOCK_HELPER) {
        let path = std::path::PathBuf::from(path);
        let _lock = crate::home::flock_exclusive(
            &path.join("delivery.lock"),
            crate::home::LockMode::Blocking,
        )
        .unwrap();
        std::fs::write(path.join("locked"), "").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !path.join("release").exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        return;
    }
    use crate::callback::send_check::{SendCheck, SendFailure};
    use crate::daemon::actors::StoreActor;
    use crate::daemon::actors::callback::{CallbackActor, CallbackArgs, deliver_job_event};
    use crate::queue::delivery::{JobRoute, JobSubmission, RoutedJobEvent};
    use crate::submission::{CallbackContext, CallbackExecutable};
    use ractor::Actor;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;

    let f = Fixture::new();
    let home = crate::home::Home::resolve(Some(f._dir.path().join("state"))).unwrap();
    home.ensure().unwrap();
    let low = f.submit(Priority::Low);
    let run = f.start(low, "gpu0", Utc::now());
    let mut queued = spec(Priority::High, 1);
    queued["resource"] = json!("gpu0");
    let head = f.submit_spec(queued);
    let episode = f
        .store
        .record_blocked_head(f.machine, Some((head, Utc::now())))
        .unwrap()
        .unwrap();
    f.store
        .produce_job_blocked(
            f.machine,
            episode,
            NoticeThresholds {
                after_yield: Duration::ZERO,
                after_wait: Duration::ZERO,
            },
            Utc::now(),
        )
        .unwrap();
    let binary = f._dir.path().join("codex");
    let received = f._dir.path().join("received");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n",
            received.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let job = f.job(head);
    let route = JobRoute {
        job: head,
        origin: f.machine,
        authority: f.machine,
        thread: job.spec.thread,
        callback: CallbackContext {
            env: TaskEnv {
                path: "/usr/bin:/bin".into(),
                home: f._dir.path().to_string_lossy().into_owned(),
            },
            cwd: f._dir.path().to_path_buf(),
            codex: CallbackExecutable::available(binary),
        },
        digest: job.spec.digest().unwrap(),
        spec: job.spec,
        target: None,
        submission: JobSubmission::Unknown,
    };
    f.store.insert_job_route(&route).unwrap();
    let notice = RoutedJobEvent {
        origin: f.machine,
        authority: f.machine,
        digest: route.digest.clone(),
        event: f.store.job_events(head).unwrap()[0].clone(),
    };
    f.store.accept_job_event(&notice).unwrap();
    let delivery = home
        .root()
        .join("jobs")
        .join(head.to_string())
        .join("delivery");
    std::fs::create_dir_all(&delivery).unwrap();
    let check_lock = delivery.join("delivery.lock");
    let checks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let check_count = checks.clone();
    let database = f._dir.path().join("db");
    let check: SendCheck = Arc::new(move || {
        if check_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
            assert!(
                matches!(
                    crate::home::flock_exclusive(&check_lock, crate::home::LockMode::NonBlocking),
                    Err(AppError::LockHeld { .. })
                ),
                "the final check must hold the delivery lock"
            );
        }
        let current = Store::open(&database)
            .and_then(|store| store.job_notice_is_current(head, 1))
            .map_err(|error| SendFailure::Failed(error.to_string()))?;
        if current {
            Ok(())
        } else {
            Err(SendFailure::Suppressed)
        }
    });
    // the first exact-notice response is true before callback preparation starts
    check().unwrap();
    let (store_actor, store_handle) = Actor::spawn(None, StoreActor, f._dir.path().join("db"))
        .await
        .unwrap();
    let (callback, callback_handle) = Actor::spawn(
        None,
        CallbackActor,
        CallbackArgs {
            store: store_actor.clone(),
            home: home.clone(),
            notifier: None,
            machine_name: "test".into(),
            claude_sessions: None,
        },
    )
    .await
    .unwrap();

    let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "store::queue::tests::notice_ends_during_callback_preparation",
            "--exact",
        ])
        .env(LOCK_HELPER, &delivery)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !delivery.join("locked").exists() {
        assert!(std::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let pending = deliver_job_event(&home, &route, &notice.event, &callback, Some(check));
    tokio::pin!(pending);
    // poll preparation while the sender cannot pass the held delivery lock
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 1);
    f.exit(&run, ExitReason::Exit { code: 0 });
    f.clean("gpu0");
    let head_run = f.start(head, "gpu0", Utc::now());
    f.exit(&head_run, ExitReason::Exit { code: 0 });
    f.store.record_blocked_head(f.machine, None).unwrap();
    let success = RoutedJobEvent {
        event: f.store.job_events(head).unwrap()[1].clone(),
        ..notice.clone()
    };
    f.store.accept_job_event(&success).unwrap();
    std::fs::write(delivery.join("release"), "").unwrap();
    assert!(holder.wait().unwrap().success());
    let outcome = pending.await.unwrap();
    assert!(outcome.is_none(), "the ended notice must be suppressed");
    assert!(!received.exists(), "JOB_BLOCKED reached the callback");
    assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 2);
    crate::daemon::event_sender::jobs::settle_callback(&store_actor, head, 1, outcome)
        .await
        .unwrap();
    assert_eq!(f.store.pending_job_inbox().unwrap()[0].1.event.seq, 2);
    let outcome = deliver_job_event(&home, &route, &success.event, &callback, None)
        .await
        .unwrap();
    crate::daemon::event_sender::jobs::settle_callback(&store_actor, head, 2, outcome)
        .await
        .unwrap();
    callback.stop(None);
    callback_handle.await.unwrap();
    store_actor.stop(None);
    store_handle.await.unwrap();
    let sent = std::fs::read_to_string(&received).unwrap();
    assert!(!sent.contains("JOB_BLOCKED"));
    assert!(sent.contains("JOB_SUCCEEDED"));
    assert_eq!(sent.lines().count(), 1);
    assert_eq!(f.store.job_route_cursors(head).unwrap().unwrap().settled, 2);
    assert!(f.store.pending_job_inbox().unwrap().is_empty());
    assert_eq!(f.store.job_events(head).unwrap().len(), 2);
}
