//! The one owner of host memory admission on this daemon
//!
//! Two places start work that declared a peak: the queue actor starts GPU
//! queue runs, and the release loop starts held tasks. Neither takes room by
//! itself. Each asks this actor for a grant, which it checks against the room
//! it sees now and records in the same message, so two passes never spend the
//! same room. A grant counts against the room until its launch settles; by
//! then the started item is a durable claim in the store, or it never started.
//! A task launch that timed out has not settled, so its grant lasts while the
//! task is still held, until a later release attempt gets an answer
//!
//! The order is the GPU queue's serving order, then tasks by priority and
//! submit order. A GPU job that waits for memory reserves its peak for every
//! task, and a task that waits reserves its peak for every later task. A job
//! whose resources are busy reserves nothing, since it cannot start anyway,
//! unless it asked to hold room: then, next in line, it reserves the part of
//! its peak that the job ahead of it will not free
//!
//! A started task keeps its whole peak until it ends, since a late phase may
//! need it. When the part of one task's peak it has not allocated is alone what
//! keeps the first waiting task from starting, for ten minutes, its owner
//! gets one `TASK_MEMORY_REVIEW` and decides whether the peak was right

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tracing::{info, warn};

use crate::config::MemoryConfig;
use crate::daemon::actors::{StoreMsg, call, send_reply};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::memory::{
    Claim, ClaimOwner, Gauge, Gib, MemPeak, MemoryHold, MemoryReview, MemoryWait, Room,
};
use crate::queue::JobId;
use crate::spec::{NormalizedSpec, NormalizedWorkload};

pub(crate) const MEMORY_NAME: &str = "homebased.memory";

// a lost job settle must not hold room for good, and a run writes its durable
// claim within seconds, far inside this bound; task grants are cleared by the
// release passes instead
const GRANT_BOUND: Duration = Duration::from_secs(120);

/// How long one running task's unallocated peak must alone keep the first
/// waiting task from starting before its owner is asked to review the peak
///
/// Long enough that a task still loading or between phases is not flagged,
/// and short against the hour or more an overstated peak can hold others back
const REVIEW_AFTER: Duration = Duration::from_secs(10 * 60);

/// Seconds instead of [`REVIEW_AFTER`], so integration tests see a review
/// without waiting minutes
const REVIEW_SECS_ENV: &str = "HOMEBASED_TEST_MEMORY_REVIEW_SECS";

/// The bar, the gauge, whether command tasks must declare a peak, and how
/// long a running task may keep the first waiting task out before its owner
/// reviews its peak, fixed for the daemon's life
#[derive(Debug, Clone)]
pub(crate) struct MemoryLimits {
    bar: u64,
    gauge: Gauge,
    require_peak: bool,
    review_after: Duration,
}

impl MemoryLimits {
    /// Limits from the daemon configuration
    pub(crate) fn new(config: &MemoryConfig) -> Self {
        Self {
            bar: config.bar,
            gauge: Gauge::new(config.cgroup.clone()),
            require_peak: config.require_peak,
            review_after: std::env::var(REVIEW_SECS_ENV)
                .ok()
                .and_then(|seconds| seconds.trim().parse().ok())
                .map_or(REVIEW_AFTER, Duration::from_secs),
        }
    }

    /// Refuse a command task that would run here without a peak, when this
    /// machine requires one
    ///
    /// A task with `machine` cannot carry a peak, since this gate measures
    /// only this machine, so it is not checked; agents and containers are
    /// exempt, as `memory.require_peak` explains
    pub(crate) fn check_undeclared(&self, spec: &NormalizedSpec) -> Result<(), AppError> {
        if !self.require_peak
            || spec.machine.is_some()
            || !matches!(spec.workload, NormalizedWorkload::Task(_))
        {
            return Ok(());
        }
        Err(AppError::InvalidSpec {
            pointer: "/mem_peak".into(),
            value: serde_json::Value::Null,
            message: "this machine requires mem_peak on command tasks ([memory] require_peak): \
                      pass --mem-peak <GiB> with the task's peak host memory plus about 10%, \
                      or 0.1 with --priority high for a task that only waits"
                .into(),
        })
    }

    /// Refuse a peak this machine can never admit
    ///
    /// A machine without a cgroup gauge cannot measure what is in use, and a
    /// peak over the bar never fits
    pub(crate) fn check_peak(&self, peak: MemPeak) -> Result<(), AppError> {
        let refuse = |message: String| AppError::InvalidSpec {
            pointer: "/mem_peak".into(),
            value: serde_json::json!(peak),
            message,
        };
        if self.gauge.cgroup().is_none() {
            return Err(refuse(
                "mem_peak needs a cgroup v2 memory controller, which this machine's daemon does \
                 not run under"
                    .into(),
            ));
        }
        if peak.bytes() > self.bar {
            return Err(refuse(format!(
                "mem_peak {peak} is over this machine's {} memory bar, so it could never start",
                Gib(self.bar)
            )));
        }
        Ok(())
    }
}

/// Work holding room it was granted, until its launch settles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Grant {
    /// A GPU queue job's run
    Job(JobId),
    /// A held task's release
    Task(TaskId),
}

/// A held task ready to start once its peak fits
#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskCandidate {
    /// The task
    pub task: TaskId,
    /// Its declared peak
    pub peak: MemPeak,
    /// Whether its last launch failed too recently to try now; it keeps its room
    pub backing_off: bool,
}

/// Requests to the memory actor
pub(crate) enum MemoryMsg {
    /// Room for GPU queue jobs: the bar less memory in use, the room started
    /// work has not allocated yet, and live grants
    JobRoom {
        reply: RpcReplyPort<Result<Room, AppError>>,
    },
    /// Grant a job's peak against the room now, less `held_back` for jobs
    /// ahead of it in this pass that were refused, or say why it waits
    GrantJob {
        job: JobId,
        peak: MemPeak,
        held_back: u64,
        reply: RpcReplyPort<Result<Result<(), MemoryWait>, AppError>>,
    },
    /// The queue pass's waiting jobs, which hold room back from tasks
    JobsWaiting { jobs: JobMemory },
    /// Grant the ready tasks that fit, in the given order, and record why the
    /// rest wait; replies with the granted tasks
    AdmitTasks {
        ready: Vec<TaskCandidate>,
        reply: RpcReplyPort<Result<Vec<TaskId>, AppError>>,
    },
    /// A granted launch ended: its claim is in the store, or it never started
    Settle { grant: Grant },
    /// Why each GPU queue job waits, and the room jobs hold, from the last
    /// queue pass
    JobWaits {
        reply: RpcReplyPort<Result<JobMemory, AppError>>,
    },
    /// Why a held task waits, from the last release pass
    TaskWait {
        task: TaskId,
        reply: RpcReplyPort<Result<Option<MemoryWait>, AppError>>,
    },
}

/// The room GPU queue jobs that have not started hold back from tasks
#[derive(Debug, Clone, Default)]
pub(crate) struct JobMemory {
    /// Jobs with a free resource that wait for memory, which reserve their peaks
    pub waits: BTreeMap<JobId, MemoryWait>,
    /// Jobs next in line for a busy resource that asked to hold room
    pub holds: BTreeMap<JobId, MemoryHold>,
}

impl JobMemory {
    /// Bytes these jobs keep from every task, given the jobs whose runs
    /// claim memory now
    ///
    /// An unreadable gauge already starts nothing, so its waits reserve
    /// nothing extra. A hold behind a job that waits for memory counts that
    /// job as claiming, since its wait reserves its peak already
    fn reserved(&self, claiming: &HashSet<JobId>) -> u64 {
        let waits: u64 = self
            .waits
            .values()
            .map(|wait| match wait {
                MemoryWait::Room { peak, .. } => peak.bytes(),
                MemoryWait::Unreadable { .. } => 0,
            })
            .sum();
        let still_claims = |job| claiming.contains(&job) || self.waits.contains_key(&job);
        let holds: u64 = self
            .holds
            .values()
            .map(|hold| hold.bytes(still_claims))
            .sum();
        waits + holds
    }
}

/// Fixed actor dependencies
pub(crate) struct MemoryArgs {
    pub limits: MemoryLimits,
    /// Owner of the store, which holds every started item's claim
    pub store: ActorRef<StoreMsg>,
    /// This daemon's machine
    pub machine: MachineId,
}

/// The memory admission actor
pub(crate) struct MemoryActor;

/// Grants, wait reasons, and the running tasks keeping the first waiting
/// task out; all are rebuilt by the next passes after a restart
pub(crate) struct MemoryState {
    args: MemoryArgs,
    grants: HashMap<Grant, (MemPeak, Instant)>,
    jobs: JobMemory,
    task_waits: HashMap<TaskId, MemoryWait>,
    /// Since when each running task has alone kept the first waiting task,
    /// named here, out; a new first waiting task starts the time again
    blocking: HashMap<TaskId, (TaskId, Instant)>,
    /// Tasks whose review the store already answered for, so a pass does not
    /// ask it again each second; the store keeps the durable once-only mark
    reviewed: HashSet<TaskId>,
}

/// The room under the bar now, what each running task held, and the jobs
/// whose runs claim memory, from one read of the claims
struct Measured {
    room: Room,
    holdings: Vec<Holding>,
    claiming: HashSet<JobId>,
}

/// A running task's share of what started work holds
#[derive(Debug, Clone, Copy)]
struct Holding {
    task: TaskId,
    peak: MemPeak,
    /// Bytes its process tree held at the measurement
    held: u64,
}

impl Holding {
    /// The running tasks among `claims` whose process has spawned, with what
    /// each held; a claim without a process holds its whole peak only until
    /// its spawn, which is no reason to review it
    fn running(claims: &[Claim], held: &[u64]) -> Vec<Self> {
        claims
            .iter()
            .zip(held)
            .filter_map(|(claim, held)| match (claim.owner, claim.root) {
                (ClaimOwner::Task(task), Some(_)) => Some(Self {
                    task,
                    peak: claim.peak,
                    held: *held,
                }),
                _ => None,
            })
            .collect()
    }

    /// The part of its peak it has not allocated
    fn unallocated(self) -> u64 {
        self.peak.bytes().saturating_sub(self.held)
    }
}

/// The first ready task refused for room, with the room it saw
struct HeadWait {
    task: TaskId,
    peak: MemPeak,
    room: Room,
}

impl Actor for MemoryActor {
    type Msg = MemoryMsg;
    type State = MemoryState;
    type Arguments = MemoryArgs;

    async fn pre_start(
        &self,
        _myself: ActorRef<MemoryMsg>,
        args: MemoryArgs,
    ) -> Result<MemoryState, ActorProcessingErr> {
        Ok(MemoryState {
            args,
            grants: HashMap::new(),
            jobs: JobMemory::default(),
            task_waits: HashMap::new(),
            blocking: HashMap::new(),
            reviewed: HashSet::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<MemoryMsg>,
        message: MemoryMsg,
        state: &mut MemoryState,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            MemoryMsg::JobRoom { reply } => {
                send_reply(reply, state.room().await.map(|measured| measured.room));
            }
            MemoryMsg::GrantJob {
                job,
                peak,
                held_back,
                reply,
            } => {
                send_reply(reply, state.grant_job(job, peak, held_back).await);
            }
            MemoryMsg::JobsWaiting { jobs } => state.jobs = jobs,
            MemoryMsg::AdmitTasks { ready, reply } => {
                send_reply(reply, state.admit_tasks(ready, Instant::now()).await);
            }
            MemoryMsg::Settle { grant } => {
                state.grants.remove(&grant);
            }
            MemoryMsg::JobWaits { reply } => send_reply(reply, Ok(state.jobs.clone())),
            MemoryMsg::TaskWait { task, reply } => {
                send_reply(reply, Ok(state.task_waits.get(&task).cloned()));
            }
        }
        Ok(())
    }
}

impl MemoryState {
    /// The room under the bar, after started work and live grants, what
    /// each running task held, and the jobs whose runs claim memory
    ///
    /// Blocking procfs and cgroupfs reads, each a few files
    async fn room(&mut self) -> Result<Measured, AppError> {
        let machine = self.args.machine;
        let claims = call(&self.args.store, |reply| StoreMsg::MemoryClaims {
            machine,
            reply,
        })
        .await?;
        let limits = &self.args.limits;
        let (mut room, holdings) = match limits.gauge.measure(&claims) {
            Ok(usage) => (
                Room::new(limits.bar, usage.used, usage.outstanding),
                Holding::running(&claims, &usage.held),
            ),
            Err(error) => (Room::Unreadable { error }, Vec::new()),
        };
        // a task grant ends only by a settle or the next release pass, which
        // drops it once the task is no longer ready; a timed-out launch may
        // still run after any bound
        self.grants.retain(|grant, (_, granted)| match grant {
            Grant::Job(_) => granted.elapsed() < GRANT_BOUND,
            Grant::Task(_) => true,
        });
        room.reserve(self.grants.values().map(|(peak, _)| peak.bytes()).sum());
        let claiming = claims
            .iter()
            .filter_map(|claim| match claim.owner {
                ClaimOwner::Job(job) => Some(job),
                ClaimOwner::Task(_) => None,
            })
            .collect();
        Ok(Measured {
            room,
            holdings,
            claiming,
        })
    }

    async fn grant_job(
        &mut self,
        job: JobId,
        peak: MemPeak,
        held_back: u64,
    ) -> Result<Result<(), MemoryWait>, AppError> {
        let mut room = self.room().await?.room;
        room.reserve(held_back);
        let granted = room.admit(peak);
        if granted.is_ok() {
            self.grants.insert(Grant::Job(job), (peak, Instant::now()));
        }
        Ok(granted)
    }

    async fn admit_tasks(
        &mut self,
        ready: Vec<TaskCandidate>,
        now: Instant,
    ) -> Result<Vec<TaskId>, AppError> {
        // a task grant outlives its pass only when its launch timed out and may
        // still run. Its row and its released route commit in one transaction,
        // so a task no longer in `ready` holds a claim or never will, and its
        // grant goes. One still in `ready` keeps its grant until a release
        // attempt gets an answer and settles it; the supervisor answers in
        // order, so that answer comes after the timed-out launch
        let ready_tasks: HashSet<TaskId> = ready.iter().map(|candidate| candidate.task).collect();
        self.grants.retain(|grant, _| match grant {
            Grant::Task(task) => ready_tasks.contains(task),
            Grant::Job(_) => true,
        });
        let mut waits = HashMap::new();
        let mut admitted = Vec::new();
        let mut head = None;
        let mut holdings = Vec::new();
        if !ready.is_empty() {
            let measured = self.room().await?;
            let mut room = measured.room;
            holdings = measured.holdings;
            room.reserve(self.jobs.reserved(&measured.claiming));
            for candidate in ready {
                // a pending launch's grant is already in the room, and its
                // retry needs no more
                if self.grants.contains_key(&Grant::Task(candidate.task)) {
                    if !candidate.backing_off {
                        admitted.push(candidate.task);
                    }
                    continue;
                }
                if candidate.backing_off {
                    room.reserve(candidate.peak.bytes());
                    continue;
                }
                let before = head.is_none().then(|| room.clone());
                match room.admit(candidate.peak) {
                    Ok(()) => {
                        let grant = (candidate.peak, Instant::now());
                        self.grants.insert(Grant::Task(candidate.task), grant);
                        admitted.push(candidate.task);
                    }
                    Err(wait) => {
                        if let Some(room) = before {
                            head = Some(HeadWait {
                                task: candidate.task,
                                peak: candidate.peak,
                                room,
                            });
                        }
                        waits.insert(candidate.task, wait);
                    }
                }
            }
        }
        self.task_waits = waits;
        self.review_blockers(head.as_ref(), &holdings, now).await;
        Ok(admitted)
    }

    /// Track the running tasks whose unallocated peak alone keeps `head` out,
    /// and ask the owner of each one that has for the review delay to review
    /// its peak
    ///
    /// Only the first refused task counts: every task behind it waits for it
    /// anyway. A blocker must free enough by itself, with the work ahead of the
    /// head still counted, so a task is never flagged for a wait that ending
    /// it would not have ended
    async fn review_blockers(
        &mut self,
        head: Option<&HeadWait>,
        holdings: &[Holding],
        now: Instant,
    ) {
        let blockers: Vec<Holding> = head.map_or_else(Vec::new, |head| {
            holdings
                .iter()
                .copied()
                .filter(|holding| head.room.fits_after(head.peak, holding.unallocated()))
                .collect()
        });
        self.blocking
            .retain(|task, _| blockers.iter().any(|holding| holding.task == *task));
        self.reviewed
            .retain(|task| holdings.iter().any(|holding| holding.task == *task));
        let Some(head) = head else {
            return;
        };

        for holding in blockers {
            let entry = self
                .blocking
                .entry(holding.task)
                .or_insert((head.task, now));
            if entry.0 != head.task {
                *entry = (head.task, now);
            }
            let blocked = now.saturating_duration_since(entry.1);
            if blocked < self.args.limits.review_after || self.reviewed.contains(&holding.task) {
                continue;
            }
            let review = MemoryReview {
                mem_peak: holding.peak,
                held_bytes: holding.held,
                waiting_task: head.task,
                waiting_peak: head.peak,
                blocked_secs: blocked.as_secs(),
            };
            let task = holding.task;
            match call(&self.args.store, |reply| StoreMsg::ProduceMemoryReview {
                id: task,
                review,
                reply,
            })
            .await
            {
                Ok(produced) => {
                    if produced {
                        info!(
                            "Asked for a memory peak review task={task} peak={} held={} waiting={}",
                            holding.peak,
                            Gib(holding.held),
                            head.task
                        );
                    }
                    self.reviewed.insert(task);
                }
                Err(error) => warn!("memory review of task={task}: {error}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ractor::Actor;
    use tempfile::tempdir;

    use std::collections::HashSet;

    use super::{
        JobMemory, MemoryActor, MemoryArgs, MemoryLimits, MemoryMsg, REVIEW_AFTER, TaskCandidate,
    };
    use crate::daemon::actors::{StoreActor, call};
    use crate::domain::TaskId;
    use crate::machine::MachineId;
    use crate::memory::{Gauge, MemPeak, MemoryHold, MemoryWait, Room};
    use crate::queue::JobId;

    const GIB: u64 = 1 << 30;

    fn candidate(task: TaskId, gib: &str, backing_off: bool) -> TaskCandidate {
        TaskCandidate {
            task,
            peak: MemPeak::parse(gib).unwrap(),
            backing_off,
        }
    }

    #[test]
    fn a_hold_counts_the_job_ahead_only_while_that_job_still_claims_its_peak() {
        let (predict, trace) = (JobId::new(), JobId::new());
        let mut jobs = JobMemory::default();
        jobs.holds.insert(
            trace,
            MemoryHold {
                peak: MemPeak::parse("8").unwrap(),
                held: 6 * GIB,
                behind: Some(predict),
            },
        );

        // the predict runs: its 2 GiB claim comes back to the trace when it ends
        assert_eq!(jobs.reserved(&HashSet::from([predict])), 6 * GIB);
        // the predict entered cleanup before the queue pass saw it: its claim
        // is gone, so a task cannot start into the 2 GiB the trace now needs
        assert_eq!(jobs.reserved(&HashSet::new()), 8 * GIB);
        // a job ahead that waits for memory reserves its peak already
        jobs.waits.insert(
            predict,
            MemoryWait::Room {
                peak: MemPeak::parse("2").unwrap(),
                free: 0,
                bar: 10 * GIB,
            },
        );
        assert_eq!(jobs.reserved(&HashSet::new()), 8 * GIB);
    }

    #[tokio::test]
    async fn a_timed_out_task_launch_keeps_its_room_once_until_the_task_leaves_the_ready_list() {
        let dir = tempdir().unwrap();
        let cgroup = dir.path().join("homebased.service");
        fs::create_dir(&cgroup).unwrap();
        fs::write(cgroup.join("memory.current"), "0\n").unwrap();
        fs::write(cgroup.join("memory.stat"), "file 0\nshmem 0\n").unwrap();
        let (store, _) = Actor::spawn(None, StoreActor, dir.path().join("homebased.db"))
            .await
            .unwrap();
        let (memory, _) = Actor::spawn(
            None,
            MemoryActor,
            MemoryArgs {
                limits: MemoryLimits {
                    bar: 10 * GIB,
                    gauge: Gauge::new(Some(cgroup)),
                    require_peak: false,
                    review_after: REVIEW_AFTER,
                },
                store,
                machine: MachineId::new(),
            },
        )
        .await
        .unwrap();
        let admit = |ready: Vec<TaskCandidate>| {
            let memory = memory.clone();
            async move {
                call(&memory, |reply| MemoryMsg::AdmitTasks { ready, reply })
                    .await
                    .unwrap()
            }
        };
        let job_room = || async {
            call(&memory, |reply| MemoryMsg::JobRoom { reply })
                .await
                .unwrap()
        };
        let known = |free: u64| Room::Known {
            free: i128::from(free),
            bar: 10 * GIB,
        };
        let (timed_out, behind) = (TaskId::new(), TaskId::new());

        // granted, then its launch timed out, so nothing settles the grant
        assert_eq!(
            admit(vec![candidate(timed_out, "6", false)]).await,
            [timed_out]
        );
        assert_eq!(job_room().await, known(4 * GIB));

        // still held and backing off: its grant holds the room, counted once
        let next = vec![
            candidate(timed_out, "6", true),
            candidate(behind, "6", false),
        ];
        assert!(admit(next).await.is_empty());
        let wait = call(&memory, |reply| MemoryMsg::TaskWait {
            task: behind,
            reply,
        })
        .await
        .unwrap();
        assert!(
            matches!(wait, Some(MemoryWait::Room { free, .. }) if free == 4 * GIB),
            "{wait:?}"
        );
        assert_eq!(job_room().await, known(4 * GIB));

        // its backoff ends while the launch is still queued: the retry keeps
        // the grant and takes no new room, so nothing behind it fits yet
        let retry = vec![
            candidate(timed_out, "6", false),
            candidate(behind, "6", false),
        ];
        assert_eq!(admit(retry).await, [timed_out]);
        assert_eq!(job_room().await, known(4 * GIB));

        // launched: its row is a claim now, so the grant goes
        assert_eq!(admit(vec![candidate(behind, "6", false)]).await, [behind]);
        assert_eq!(job_room().await, known(4 * GIB));
    }
}
