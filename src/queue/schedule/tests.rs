use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};

use super::{
    Blocker, JobView, Launch, NoticeThresholds, Preempt, QueuedState, ResourceView, Snapshot,
    StoredEpisode, decide,
};
use crate::cleanup::CleanupFailure as ProcessCleanupFailure;
use crate::domain::TaskId;
use crate::queue::{
    ActiveRun, AttentionId, CleanupFailure, JobId, Preemption, Priority, ResourceId, ResourceName,
    RestartWindow, RunNumber, RunPhase, StepIndex, StopCause, Target,
};

fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 3, 12, 0, 0).unwrap()
}

fn at(seconds: i64) -> DateTime<Utc> {
    t0() + chrono::Duration::seconds(seconds)
}

const YIELD: Preemption = Preemption::Yield {
    restart_within: None,
};
const WAIT: Preemption = Preemption::Wait {
    restart_within: None,
};

fn window(minutes: u64) -> Option<RestartWindow> {
    Some(RestartWindow::try_from(Duration::from_secs(minutes * 60)).unwrap())
}

/// A test machine: resources by name and jobs in serving order
struct Machine {
    resources: Vec<ResourceView>,
    queue: Vec<JobView>,
    episode: Option<StoredEpisode>,
}

impl Machine {
    fn new(names: &[&str]) -> Self {
        Self {
            resources: names
                .iter()
                .map(|name| ResourceView {
                    id: ResourceId::new(),
                    name: ResourceName::parse(name).unwrap(),
                    run: None,
                })
                .collect(),
            queue: Vec::new(),
            episode: None,
        }
    }

    fn resource(&self, name: &str) -> ResourceId {
        self.resources
            .iter()
            .find(|resource| resource.name.as_str() == name)
            .unwrap()
            .id
    }

    /// Queue a job at the back of the serving order
    fn queued(&mut self, priority: Priority, target: Target, preempt: Preemption) -> JobId {
        let id = JobId::new();
        self.queue.push(JobView {
            id,
            priority,
            target,
            preempt,
            state: QueuedState::Queued,
        });
        id
    }

    /// An active job whose run on `resource` is in `phase`
    fn running(
        &mut self,
        resource: &str,
        priority: Priority,
        preempt: Preemption,
        phase: RunPhase,
    ) -> (JobId, TaskId) {
        let resource = self.resource(resource);
        let job = JobId::new();
        let task = TaskId::new();
        self.queue.push(JobView {
            id: job,
            priority,
            target: Target::Any,
            preempt,
            state: QueuedState::Active(resource),
        });
        self.set_run(resource, job, task, phase);
        (job, task)
    }

    fn set_run(&mut self, resource: ResourceId, job: JobId, task: TaskId, phase: RunPhase) {
        let view = self
            .resources
            .iter_mut()
            .find(|view| view.id == resource)
            .unwrap();
        view.run = Some(ActiveRun {
            resource,
            job,
            task,
            run_number: RunNumber::FIRST,
            step: StepIndex::FIRST,
            phase,
        });
    }

    fn snapshot(&self, now: DateTime<Utc>) -> Snapshot {
        // the store hands the queue over in serving order
        let mut queue = self.queue.clone();
        queue.sort_by_key(|job| std::cmp::Reverse(job.priority));
        Snapshot {
            now,
            resources: self.resources.clone(),
            queue,
            episode: self.episode,
            thresholds: NoticeThresholds::default(),
        }
    }
}

fn executing(started_at: DateTime<Utc>) -> RunPhase {
    RunPhase::Executing { started_at }
}

#[test]
fn two_resources_run_two_jobs() {
    let mut machine = Machine::new(&["gpu0", "gpu1"]);
    let first = machine.queued(Priority::Medium, Target::Any, YIELD);
    let second = machine.queued(Priority::Medium, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(t0()));
    assert_eq!(
        decisions.launches,
        vec![
            Launch {
                job: first,
                resource: machine.resource("gpu0")
            },
            Launch {
                job: second,
                resource: machine.resource("gpu1")
            },
        ]
    );
    assert_eq!(decisions.preemption, None);
    assert_eq!(decisions.blocked, None);
}

#[test]
fn an_any_job_takes_the_free_resource_with_the_lowest_name() {
    let mut machine = Machine::new(&["gpu2", "gpu0", "gpu1"]);
    machine.running("gpu0", Priority::Low, WAIT, executing(t0()));
    let job = machine.queued(Priority::Low, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(t0()));
    assert_eq!(
        decisions.launches,
        vec![Launch {
            job,
            resource: machine.resource("gpu1")
        }]
    );
}

#[test]
fn a_pinned_job_waits_while_an_any_job_behind_it_starts_elsewhere() {
    let mut machine = Machine::new(&["gpu0", "gpu1"]);
    machine.running("gpu0", Priority::Medium, WAIT, executing(t0()));
    let pinned = machine.queued(
        Priority::Medium,
        Target::Pinned(machine.resource("gpu0")),
        YIELD,
    );
    let any = machine.queued(Priority::Medium, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(at(60)));
    assert_eq!(
        decisions.launches,
        vec![Launch {
            job: any,
            resource: machine.resource("gpu1")
        }]
    );
    let blocked = decisions.blocked.unwrap();
    assert_eq!(blocked.job, pinned);
    assert_eq!(
        blocked.blockers.len(),
        1,
        "only its pinned resource blocks it"
    );
}

#[test]
fn no_launch_on_a_resource_that_is_cleaning_or_in_attention() {
    let mut machine = Machine::new(&["gpu0", "gpu1", "gpu2"]);
    let (_, _) = machine.running(
        "gpu0",
        Priority::Low,
        YIELD,
        RunPhase::Cleaning { attempt: 1 },
    );
    let attention = AttentionId::new();
    machine.running(
        "gpu1",
        Priority::Low,
        YIELD,
        RunPhase::Attention {
            id: attention,
            failure: CleanupFailure::Processes {
                failure: ProcessCleanupFailure::EnumerationFailed {
                    message: "denied".into(),
                },
            },
        },
    );
    let first = machine.queued(Priority::High, Target::Any, YIELD);
    let second = machine.queued(Priority::High, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(t0()));
    assert_eq!(
        decisions.launches,
        vec![Launch {
            job: first,
            resource: machine.resource("gpu2")
        }]
    );
    let blocked = decisions.blocked.unwrap();
    assert_eq!(blocked.job, second);
    assert!(blocked.blockers.contains(&Blocker::Attention {
        resource: machine.resource("gpu1"),
        attention,
    }));
    // neither a cleaning run nor an attention is a preemption victim
    assert_eq!(decisions.preemption, None);
    assert_eq!(blocked.threshold, Duration::from_secs(30 * 60));
}

#[test]
fn a_job_waits_for_its_own_previous_run_to_be_cleaned_up() {
    let mut machine = Machine::new(&["gpu0", "gpu1"]);
    let job = machine.queued(Priority::High, Target::Any, YIELD);
    machine.set_run(
        machine.resource("gpu0"),
        job,
        TaskId::new(),
        RunPhase::Cleaning { attempt: 1 },
    );
    let behind = machine.queued(Priority::Low, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(t0()));
    assert_eq!(
        decisions.launches,
        vec![Launch {
            job: behind,
            resource: machine.resource("gpu1")
        }]
    );
    assert_eq!(decisions.blocked, None);
}

#[test]
fn victims_go_lowest_level_then_cheapest_stop_then_most_recent_start() {
    struct Case {
        name: &'static str,
        runs: Vec<(&'static str, Priority, Preemption, DateTime<Utc>)>,
        victim: &'static str,
        cause: StopCause,
    }
    let now = at(3600);
    let cases = [
        Case {
            name: "lowest level first",
            runs: vec![
                ("gpu0", Priority::Medium, Preemption::Restart, at(0)),
                ("gpu1", Priority::Low, YIELD, at(0)),
            ],
            victim: "gpu1",
            cause: StopCause::Yield,
        },
        Case {
            name: "restart mode before yield at one level",
            runs: vec![
                ("gpu0", Priority::Low, YIELD, at(3000)),
                ("gpu1", Priority::Low, Preemption::Restart, at(0)),
            ],
            victim: "gpu1",
            cause: StopCause::Restart,
        },
        Case {
            name: "inside a restart window before yield",
            runs: vec![
                ("gpu0", Priority::Low, YIELD, at(3500)),
                (
                    "gpu1",
                    Priority::Low,
                    Preemption::Wait {
                        restart_within: window(10),
                    },
                    at(3300),
                ),
            ],
            victim: "gpu1",
            cause: StopCause::Restart,
        },
        Case {
            name: "most recent start among equals",
            runs: vec![
                ("gpu0", Priority::Low, YIELD, at(100)),
                ("gpu1", Priority::Low, YIELD, at(200)),
                ("gpu2", Priority::Low, YIELD, at(50)),
            ],
            victim: "gpu1",
            cause: StopCause::Yield,
        },
        Case {
            name: "a wait run outside its window is never chosen",
            runs: vec![
                ("gpu0", Priority::Low, WAIT, at(3599)),
                ("gpu1", Priority::Medium, YIELD, at(0)),
            ],
            victim: "gpu1",
            cause: StopCause::Yield,
        },
    ];
    for case in cases {
        let names: Vec<&str> = case.runs.iter().map(|run| run.0).collect();
        let mut machine = Machine::new(&names);
        for (resource, priority, preempt, started) in &case.runs {
            machine.running(resource, *priority, *preempt, executing(*started));
        }
        machine.queued(Priority::High, Target::Any, YIELD);
        let decisions = decide(&machine.snapshot(now));
        let preempt = decisions
            .preemption
            .unwrap_or_else(|| panic!("{}: no victim", case.name));
        assert_eq!(
            preempt.resource,
            machine.resource(case.victim),
            "{}",
            case.name
        );
        assert_eq!(preempt.cause, case.cause, "{}", case.name);
    }
}

#[test]
fn equal_levels_never_preempt_and_wait_runs_outside_a_window_are_left_alone() {
    let mut machine = Machine::new(&["gpu0"]);
    machine.running("gpu0", Priority::High, Preemption::Restart, executing(t0()));
    machine.queued(Priority::High, Target::Any, YIELD);
    assert_eq!(decide(&machine.snapshot(at(10))).preemption, None);

    let mut machine = Machine::new(&["gpu0"]);
    machine.running("gpu0", Priority::Low, WAIT, executing(t0()));
    machine.queued(Priority::High, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(at(10)));
    assert_eq!(decisions.preemption, None);
    assert_eq!(
        decisions.blocked.unwrap().threshold,
        Duration::from_secs(30 * 60),
        "behind a wait run the longer threshold applies"
    );
}

#[test]
fn restart_window_decisions_before_at_and_after_the_bound() {
    for (preempt, age, expected) in [
        (
            Preemption::Wait {
                restart_within: window(5),
            },
            299,
            Some(StopCause::Restart),
        ),
        (
            Preemption::Wait {
                restart_within: window(5),
            },
            300,
            None,
        ),
        (
            Preemption::Wait {
                restart_within: window(5),
            },
            301,
            None,
        ),
        (
            Preemption::Yield {
                restart_within: window(5),
            },
            299,
            Some(StopCause::Restart),
        ),
        (
            Preemption::Yield {
                restart_within: window(5),
            },
            300,
            Some(StopCause::Yield),
        ),
        (
            Preemption::Yield {
                restart_within: window(5),
            },
            301,
            Some(StopCause::Yield),
        ),
    ] {
        let mut machine = Machine::new(&["gpu0"]);
        let (job, task) = machine.running("gpu0", Priority::Low, preempt, executing(t0()));
        machine.queued(Priority::Medium, Target::Any, YIELD);
        let decisions = decide(&machine.snapshot(at(age)));
        let expected = expected.map(|cause| Preempt {
            resource: machine.resource("gpu0"),
            job,
            task,
            cause,
        });
        assert_eq!(decisions.preemption, expected, "{preempt:?} at age {age}s");
    }
}

#[test]
fn at_most_one_preemption_is_in_flight_for_the_head() {
    let mut machine = Machine::new(&["gpu0", "gpu1"]);
    machine.running(
        "gpu0",
        Priority::Low,
        YIELD,
        RunPhase::Stopping {
            started_at: Some(t0()),
            cause: StopCause::Yield,
            requested_at: at(60),
        },
    );
    machine.running("gpu1", Priority::Low, Preemption::Restart, executing(t0()));
    machine.queued(Priority::High, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(at(120)));
    assert_eq!(decisions.preemption, None);
    let blocked = decisions.blocked.unwrap();
    assert_eq!(
        blocked.threshold,
        Duration::from_secs(15 * 60),
        "a run was asked to yield"
    );
}

#[test]
fn preemption_looks_only_at_the_heads_allowed_resources() {
    let mut machine = Machine::new(&["gpu0", "gpu1"]);
    machine.running("gpu0", Priority::Medium, WAIT, executing(t0()));
    machine.running("gpu1", Priority::Low, Preemption::Restart, executing(t0()));
    let head = machine.queued(
        Priority::High,
        Target::Pinned(machine.resource("gpu0")),
        YIELD,
    );
    let decisions = decide(&machine.snapshot(at(60)));
    assert_eq!(
        decisions.preemption, None,
        "gpu1 is not the head's resource"
    );
    assert_eq!(decisions.blocked.unwrap().job, head);
}

#[test]
fn preemption_serves_the_first_queued_job_with_an_eligible_victim() {
    struct Case {
        name: &'static str,
        head_run: Preemption,
        other_run: Preemption,
        other_priority: Priority,
        stopping: Option<StopCause>,
        any_target: bool,
        victim: Option<(&'static str, StopCause)>,
    }
    let cases = [
        Case {
            name: "pinned wait head does not block a later any job's restart",
            head_run: WAIT,
            other_run: Preemption::Restart,
            other_priority: Priority::Low,
            stopping: None,
            any_target: true,
            victim: Some(("gpu1", StopCause::Restart)),
        },
        Case {
            name: "pinned wait head does not block a later any job's yield",
            head_run: WAIT,
            other_run: YIELD,
            other_priority: Priority::Low,
            stopping: None,
            any_target: true,
            victim: Some(("gpu1", StopCause::Yield)),
        },
        Case {
            name: "head wins even when the later job has a cheaper lower level victim",
            head_run: YIELD,
            other_run: Preemption::Restart,
            other_priority: Priority::Low,
            stopping: None,
            any_target: true,
            victim: Some(("gpu0", StopCause::Yield)),
        },
        Case {
            name: "equal levels never preempt",
            head_run: WAIT,
            other_run: Preemption::Restart,
            other_priority: Priority::High,
            stopping: None,
            any_target: true,
            victim: None,
        },
        Case {
            name: "yield in flight outside queued targets blocks another preemption",
            head_run: YIELD,
            other_run: Preemption::Restart,
            other_priority: Priority::Low,
            stopping: Some(StopCause::Yield),
            any_target: false,
            victim: None,
        },
        Case {
            name: "restart in flight outside queued targets blocks another preemption",
            head_run: YIELD,
            other_run: Preemption::Restart,
            other_priority: Priority::Low,
            stopping: Some(StopCause::Restart),
            any_target: false,
            victim: None,
        },
        Case {
            name: "user cancellation is not a preemption in flight",
            head_run: YIELD,
            other_run: Preemption::Restart,
            other_priority: Priority::Low,
            stopping: Some(StopCause::UserCancel),
            any_target: false,
            victim: Some(("gpu0", StopCause::Yield)),
        },
    ];
    for case in cases {
        let mut machine = Machine::new(&["gpu0", "gpu1", "gpu2"]);
        machine.running("gpu0", Priority::Medium, case.head_run, executing(t0()));
        machine.running("gpu1", case.other_priority, case.other_run, executing(t0()));
        let phase = case
            .stopping
            .map_or(executing(t0()), |cause| RunPhase::Stopping {
                started_at: Some(t0()),
                cause,
                requested_at: at(30),
            });
        machine.running("gpu2", Priority::Low, WAIT, phase);
        let head = machine.queued(
            Priority::High,
            Target::Pinned(machine.resource("gpu0")),
            YIELD,
        );
        let target = if case.any_target {
            Target::Any
        } else {
            Target::Pinned(machine.resource("gpu1"))
        };
        machine.queued(Priority::High, target, YIELD);

        let decisions = decide(&machine.snapshot(at(60)));
        assert!(decisions.launches.is_empty(), "{}", case.name);
        let expected = case.victim.map(|(name, cause)| {
            let resource = machine.resource(name);
            let run = machine
                .resources
                .iter()
                .find(|view| view.id == resource)
                .unwrap()
                .run
                .as_ref()
                .unwrap();
            Preempt {
                resource,
                job: run.job,
                task: run.task,
                cause,
            }
        });
        assert_eq!(decisions.preemption, expected, "{}", case.name);
        let blocked = decisions.blocked.unwrap();
        assert_eq!(blocked.job, head, "{}", case.name);
        let threshold = if case.victim == Some(("gpu0", StopCause::Yield)) {
            Duration::from_secs(15 * 60)
        } else {
            Duration::from_secs(30 * 60)
        };
        assert_eq!(blocked.threshold, threshold, "{}", case.name);
    }
}

#[test]
fn blocked_notice_timing_follows_the_stored_episode() {
    let mut machine = Machine::new(&["gpu0"]);
    machine.running("gpu0", Priority::Low, WAIT, executing(t0()));
    let head = machine.queued(Priority::High, Target::Any, YIELD);

    // a new head starts an episode now
    let first = decide(&machine.snapshot(at(10))).blocked.unwrap();
    assert!(first.new_episode);
    assert_eq!(first.blocked_since, at(10));
    assert_eq!(first.due_at, at(10 + 30 * 60));
    assert!(!first.send_now);

    // the stored episode keeps its start across decisions and restarts
    machine.episode = Some(StoredEpisode {
        id: 1,
        job: head,
        blocked_since: at(10),
        notified: false,
    });
    let before = decide(&machine.snapshot(at(10 + 30 * 60 - 1)))
        .blocked
        .unwrap();
    assert!(!before.new_episode);
    assert!(!before.send_now);
    let due = decide(&machine.snapshot(at(10 + 30 * 60))).blocked.unwrap();
    assert!(due.send_now);

    // one notice per episode
    machine.episode = Some(StoredEpisode {
        id: 1,
        job: head,
        blocked_since: at(10),
        notified: true,
    });
    assert!(
        !decide(&machine.snapshot(at(10 + 60 * 60)))
            .blocked
            .unwrap()
            .send_now
    );

    // a different stored head means a new episode for this one
    machine.episode = Some(StoredEpisode {
        id: 1,
        job: JobId::new(),
        blocked_since: at(0),
        notified: true,
    });
    let fresh = decide(&machine.snapshot(at(100))).blocked.unwrap();
    assert!(fresh.new_episode);
    assert_eq!(fresh.blocked_since, at(100));
}

#[test]
fn a_new_yield_request_shortens_the_threshold() {
    let mut machine = Machine::new(&["gpu0"]);
    machine.running("gpu0", Priority::Low, YIELD, executing(t0()));
    machine.queued(Priority::High, Target::Any, YIELD);
    let decisions = decide(&machine.snapshot(at(60)));
    assert_eq!(decisions.preemption.unwrap().cause, StopCause::Yield);
    assert_eq!(
        decisions.blocked.unwrap().threshold,
        Duration::from_secs(15 * 60)
    );
}
