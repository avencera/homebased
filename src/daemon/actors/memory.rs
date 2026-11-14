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
//! The order is the GPU queue's serving order, then tasks by submit order. A
//! GPU job that waits for memory reserves its peak for every task, and a task
//! that waits reserves its peak for every later task. A job whose resources
//! are busy reserves nothing, since it cannot start anyway

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};

use crate::config::MemoryConfig;
use crate::daemon::actors::{StoreMsg, call, send_reply};
use crate::domain::TaskId;
use crate::error::AppError;
use crate::machine::MachineId;
use crate::memory::{Gauge, Gib, MemPeak, MemoryWait, Room, outstanding};
use crate::queue::JobId;

pub(crate) const MEMORY_NAME: &str = "homebased.memory";

// a lost job settle must not hold room for good, and a run writes its durable
// claim within seconds, far inside this bound; task grants are cleared by the
// release passes instead
const GRANT_BOUND: Duration = Duration::from_secs(120);

/// The bar and the gauge, fixed for the daemon's life
#[derive(Debug, Clone)]
pub(crate) struct MemoryLimits {
    bar: u64,
    gauge: Gauge,
}

impl MemoryLimits {
    /// Limits from the daemon configuration
    pub(crate) fn new(config: &MemoryConfig) -> Self {
        Self {
            bar: config.bar,
            gauge: Gauge::new(config.cgroup.clone()),
        }
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
    /// The queue pass's waiting jobs, which hold their peaks back from tasks
    JobsWaiting { waits: BTreeMap<JobId, MemoryWait> },
    /// Grant the ready tasks that fit, in the given order, and record why the
    /// rest wait; replies with the granted tasks
    AdmitTasks {
        ready: Vec<TaskCandidate>,
        reply: RpcReplyPort<Result<Vec<TaskId>, AppError>>,
    },
    /// A granted launch ended: its claim is in the store, or it never started
    Settle { grant: Grant },
    /// Why each GPU queue job waits, from the last queue pass
    JobWaits {
        reply: RpcReplyPort<Result<BTreeMap<JobId, MemoryWait>, AppError>>,
    },
    /// Why a held task waits, from the last release pass
    TaskWait {
        task: TaskId,
        reply: RpcReplyPort<Result<Option<MemoryWait>, AppError>>,
    },
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

/// Grants and wait reasons; both are rebuilt by the next passes after a restart
pub(crate) struct MemoryState {
    args: MemoryArgs,
    grants: HashMap<Grant, (MemPeak, Instant)>,
    job_waits: BTreeMap<JobId, MemoryWait>,
    task_waits: HashMap<TaskId, MemoryWait>,
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
            job_waits: BTreeMap::new(),
            task_waits: HashMap::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<MemoryMsg>,
        message: MemoryMsg,
        state: &mut MemoryState,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            MemoryMsg::JobRoom { reply } => send_reply(reply, state.room().await),
            MemoryMsg::GrantJob {
                job,
                peak,
                held_back,
                reply,
            } => {
                send_reply(reply, state.grant_job(job, peak, held_back).await);
            }
            MemoryMsg::JobsWaiting { waits } => state.job_waits = waits,
            MemoryMsg::AdmitTasks { ready, reply } => {
                send_reply(reply, state.admit_tasks(ready).await);
            }
            MemoryMsg::Settle { grant } => {
                state.grants.remove(&grant);
            }
            MemoryMsg::JobWaits { reply } => send_reply(reply, Ok(state.job_waits.clone())),
            MemoryMsg::TaskWait { task, reply } => {
                send_reply(reply, Ok(state.task_waits.get(&task).cloned()));
            }
        }
        Ok(())
    }
}

impl MemoryState {
    /// The room under the bar, after started work and live grants
    ///
    /// Blocking procfs and cgroupfs reads, each a few files
    async fn room(&mut self) -> Result<Room, AppError> {
        let machine = self.args.machine;
        let claims = call(&self.args.store, |reply| StoreMsg::MemoryClaims {
            machine,
            reply,
        })
        .await?;
        let limits = &self.args.limits;
        let mut room = match limits.gauge.read() {
            Ok(used) => Room::new(limits.bar, used, outstanding(&claims)),
            Err(error) => Room::Unreadable { error },
        };
        // a task grant ends only by a settle or the next release pass, which
        // drops it once the task is no longer ready; a timed-out launch may
        // still run after any bound
        self.grants.retain(|grant, (_, granted)| match grant {
            Grant::Job(_) => granted.elapsed() < GRANT_BOUND,
            Grant::Task(_) => true,
        });
        room.reserve(self.grants.values().map(|(peak, _)| peak.bytes()).sum());
        Ok(room)
    }

    async fn grant_job(
        &mut self,
        job: JobId,
        peak: MemPeak,
        held_back: u64,
    ) -> Result<Result<(), MemoryWait>, AppError> {
        let mut room = self.room().await?;
        room.reserve(held_back);
        let granted = room.admit(peak);
        if granted.is_ok() {
            self.grants.insert(Grant::Job(job), (peak, Instant::now()));
        }
        Ok(granted)
    }

    async fn admit_tasks(&mut self, ready: Vec<TaskCandidate>) -> Result<Vec<TaskId>, AppError> {
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
        if !ready.is_empty() {
            let mut room = self.room().await?;
            room.reserve(self.job_waits.values().map(reserved_bytes).sum());
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
                match room.admit(candidate.peak) {
                    Ok(()) => {
                        let grant = (candidate.peak, Instant::now());
                        self.grants.insert(Grant::Task(candidate.task), grant);
                        admitted.push(candidate.task);
                    }
                    Err(wait) => {
                        waits.insert(candidate.task, wait);
                    }
                }
            }
        }
        self.task_waits = waits;
        Ok(admitted)
    }
}

/// Room a waiting item holds back from the items behind it
///
/// An unreadable gauge already starts nothing, so it reserves nothing extra
fn reserved_bytes(wait: &MemoryWait) -> u64 {
    match wait {
        MemoryWait::Room { peak, .. } => peak.bytes(),
        MemoryWait::Unreadable { .. } => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ractor::Actor;
    use tempfile::tempdir;

    use super::{MemoryActor, MemoryArgs, MemoryLimits, MemoryMsg, TaskCandidate};
    use crate::daemon::actors::{StoreActor, call};
    use crate::domain::TaskId;
    use crate::machine::MachineId;
    use crate::memory::{Gauge, MemPeak, MemoryWait, Room};

    const GIB: u64 = 1 << 30;

    fn candidate(task: TaskId, gib: &str, backing_off: bool) -> TaskCandidate {
        TaskCandidate {
            task,
            peak: MemPeak::parse(gib).unwrap(),
            backing_off,
        }
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
