//! `status` and `ls`: one view over this machine's tasks, GPU queue, and
//! services, read from the local daemon
//!
//! Both end with the commands that show each part in full. A part the daemon
//! fails to give is reported in its place, so one broken endpoint still
//! leaves the rest of the view

use std::collections::HashSet;
use std::io;
use std::process::ExitCode;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::guide::{Guide, print_pointers};
use super::resource::resource_label;
use super::{Ctx, OutputMode};
use crate::client::Client;
use crate::daemon::api::views::TaskSummary;
use crate::domain::{ProcessStatus, TaskId, TaskStatus};
use crate::error::AppError;
use crate::queue::{JobState, Resource};
use crate::service::{ServiceState, ServiceView, ServicesResponse};
use crate::store::queue::JobRecord;
use crate::store::queue::interface::{JobList, ResourceList};

/// The part of `GET /v1/status` the view shows
#[derive(Debug, serde::Deserialize)]
struct DaemonStatus {
    version: String,
    pid: u32,
    web: Option<String>,
    in_flight: usize,
}

#[derive(Debug, serde::Deserialize)]
struct TaskList {
    tasks: Vec<TaskSummary>,
}

/// One part of the view, or why the daemon did not give it
type Part<T> = Result<T, AppError>;

/// What the local daemon reports
enum Snapshot {
    /// The socket is missing or does not answer
    Down(AppError),
    /// The daemon answered its status
    Up(Box<Live>),
}

struct Live {
    daemon: DaemonStatus,
    /// Held, queued, and running tasks, without the run tasks of GPU jobs,
    /// which show as their jobs
    tasks: Part<Vec<TaskSummary>>,
    resources: Part<ResourceList>,
    jobs: Part<JobList>,
    /// Every thread's services that have not ended
    services: Part<Vec<ServiceView>>,
}

async fn fetch<T: DeserializeOwned>(client: &Client, path: &str) -> Part<T> {
    let value = client.get(path).await?;
    serde_json::from_value(value).map_err(|error| AppError::Internal {
        message: format!("unexpected answer from {path}: {error}"),
    })
}

impl Snapshot {
    async fn read(ctx: &Ctx) -> Self {
        let client = Client::new(ctx.home.sock_path());
        let daemon = match fetch::<DaemonStatus>(&client, "/v1/status").await {
            Ok(daemon) => daemon,
            Err(err) => return Self::Down(err),
        };

        let (tasks, resources, jobs, services) = tokio::join!(
            fetch::<TaskList>(&client, "/v1/tasks?status=held,queued,running"),
            fetch::<ResourceList>(&client, "/v1/resources"),
            fetch::<JobList>(&client, "/v1/resource/jobs"),
            fetch::<ServicesResponse>(&client, "/v1/services?ended=false"),
        );

        // a GPU run is a task too; counting it beside its job would show the
        // same work twice
        let runs: HashSet<TaskId> = resources
            .iter()
            .flat_map(|list| &list.resources)
            .filter_map(|record| record.run.as_ref())
            .map(|run| run.task)
            .collect();
        let tasks = tasks.map(|list| {
            list.tasks
                .into_iter()
                .filter(|task| !runs.contains(&task.id))
                .collect()
        });

        Self::Up(Box::new(Live {
            daemon,
            tasks,
            resources,
            jobs,
            services: services.map(|response| response.services),
        }))
    }
}

fn part_json<T>(part: &Part<T>, view: impl FnOnce(&T) -> Value) -> Value {
    match part {
        Ok(value) => view(value),
        Err(err) => json!({ "error": err.code(), "message": err.to_string() }),
    }
}

/// Counts of active tasks by status
#[derive(Debug, Default, Serialize)]
struct TaskCounts {
    running: usize,
    queued: usize,
    held: usize,
}

impl TaskCounts {
    fn of(tasks: &[TaskSummary]) -> Self {
        let mut counts = Self::default();
        for task in tasks {
            match task.status {
                TaskStatus::Process(ProcessStatus::Running) => counts.running += 1,
                TaskStatus::Process(ProcessStatus::Queued) => counts.queued += 1,
                TaskStatus::Held => counts.held += 1,
                // the list asks only for active tasks
                TaskStatus::Process(_) => {}
            }
        }

        counts
    }

    fn human(&self) -> String {
        let Self {
            running,
            queued,
            held,
        } = self;
        format!("{running} running, {queued} queued, {held} held")
    }
}

/// Counts of non-terminal GPU queue jobs by state
#[derive(Debug, Default, Serialize)]
struct JobCounts {
    active: usize,
    queued: usize,
}

impl JobCounts {
    fn of(jobs: &[JobRecord]) -> Self {
        let mut counts = Self::default();
        for job in jobs {
            match job.state {
                JobState::Active { .. } => counts.active += 1,
                JobState::Queued { .. } => counts.queued += 1,
                // the queue lists only non-terminal jobs
                JobState::Succeeded | JobState::Failed { .. } | JobState::Cancelled => {}
            }
        }

        counts
    }
}

/// Counts of services that have not ended, by state
#[derive(Debug, Default, Serialize)]
struct ServiceCounts {
    ready: usize,
    starting: usize,
    stopping: usize,
}

impl ServiceCounts {
    fn of(services: &[ServiceView]) -> Self {
        let mut counts = Self::default();
        for service in services {
            match service.state {
                ServiceState::Ready => counts.ready += 1,
                ServiceState::Starting => counts.starting += 1,
                ServiceState::Stopping => counts.stopping += 1,
                // the list leaves out ended services
                ServiceState::Ended => {}
            }
        }

        counts
    }
}

/// One GPU resource and the job its run belongs to
#[derive(Debug, Serialize)]
struct GpuLine {
    resource: String,
    /// Run phase, or `idle`
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    job: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_name: Option<String>,
}

fn gpu_lines(resources: &ResourceList, jobs: Option<&JobList>) -> Vec<GpuLine> {
    let known: Vec<Resource> = resources
        .resources
        .iter()
        .map(|record| record.resource.clone())
        .collect();
    resources
        .resources
        .iter()
        .map(|record| {
            let run = record.run.as_ref();
            let job_name = run.and_then(|run| {
                jobs?
                    .jobs
                    .iter()
                    .find(|job| job.id == run.job)
                    .map(|job| job.spec.name.to_string())
            });
            GpuLine {
                resource: resource_label(record.resource.id, &known),
                state: run.map_or("idle", |run| run.phase.as_str()),
                job: run.map(|run| run.job.to_string()),
                job_name,
            }
        })
        .collect()
}

/// `homebased status`: counts of each kind of work on this machine
pub async fn status(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let snapshot = Snapshot::read(ctx).await;
    match ctx.output {
        OutputMode::Json => ctx.print_json(status_json(&snapshot))?,
        OutputMode::Quiet => match &snapshot {
            Snapshot::Down(_) => println!("down"),
            Snapshot::Up(live) => println!("{}", live.daemon.in_flight),
        },
        OutputMode::Human => {
            print_status(&snapshot);
            println!("\nMore:");
            print_pointers(&mut io::stdout(), Guide::Status.pointers())?;
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn status_json(snapshot: &Snapshot) -> Value {
    let commands = Guide::Status.pointers();
    let live = match snapshot {
        Snapshot::Down(err) => {
            return json!({
                "daemon": { "state": "down", "error": err.code(), "message": err.to_string() },
                "commands": commands,
            });
        }
        Snapshot::Up(live) => live,
    };

    let DaemonStatus {
        version,
        pid,
        web,
        in_flight,
    } = &live.daemon;
    json!({
        "daemon": { "state": "up", "version": version, "pid": pid, "web": web, "in_flight": in_flight },
        "tasks": part_json(&live.tasks, |tasks| json!(TaskCounts::of(tasks))),
        "gpus": part_json(&live.resources, |list| json!(gpu_lines(list, live.jobs.as_ref().ok()))),
        "jobs": part_json(&live.jobs, |list| json!(JobCounts::of(&list.jobs))),
        "services": part_json(&live.services, |services| json!(ServiceCounts::of(services))),
        "commands": commands,
    })
}

fn print_status(snapshot: &Snapshot) {
    let live = match snapshot {
        Snapshot::Down(err) => {
            println!("daemon    down: {err}");
            return;
        }
        Snapshot::Up(live) => live,
    };

    let DaemonStatus {
        version, pid, web, ..
    } = &live.daemon;
    let web = web.as_deref().unwrap_or("off");
    println!("daemon    up  v{version}  pid {pid}  web {web}");

    match &live.tasks {
        Ok(tasks) => println!("tasks     {}", TaskCounts::of(tasks).human()),
        Err(err) => println!("tasks     not read: {err}"),
    }

    match &live.resources {
        Ok(list) if list.resources.is_empty() => println!("gpus      none"),
        Ok(list) => {
            for line in gpu_lines(list, live.jobs.as_ref().ok()) {
                let GpuLine {
                    resource,
                    state,
                    job,
                    job_name,
                } = line;
                let job = job.map_or_else(String::new, |job| format!("  job {job}"));
                let name = job_name.map_or_else(String::new, |name| format!("  {name}"));
                println!("gpu       {resource}  {state}{job}{name}");
            }
        }
        Err(err) => println!("gpus      not read: {err}"),
    }

    match &live.jobs {
        Ok(list) => {
            let JobCounts { active, queued } = JobCounts::of(&list.jobs);
            println!("queue     {active} active, {queued} queued");
        }
        Err(err) => println!("queue     not read: {err}"),
    }

    match &live.services {
        Ok(services) => {
            let ServiceCounts {
                ready,
                starting,
                stopping,
            } = ServiceCounts::of(services);
            println!("services  {ready} ready, {starting} starting, {stopping} stopping");
        }
        Err(err) => println!("services  not read: {err}"),
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum Kind {
    Task,
    Job,
    Service,
}

impl Kind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Task => "task",
            Self::Job => "job",
            Self::Service => "service",
        }
    }
}

/// One active item in `ls`
#[derive(Debug, Serialize)]
struct Item {
    kind: Kind,
    id: String,
    state: &'static str,
    name: String,
    /// The resource an active GPU job runs on
    #[serde(skip_serializing_if = "Option::is_none")]
    resource: Option<String>,
}

fn items(live: &Live) -> Vec<Item> {
    let known: Vec<Resource> = live
        .resources
        .iter()
        .flat_map(|list| &list.resources)
        .map(|record| record.resource.clone())
        .collect();

    let tasks = live.tasks.iter().flatten().map(|task| Item {
        kind: Kind::Task,
        id: task.id.to_string(),
        state: task.status.as_str(),
        name: task.name.to_string(),
        resource: None,
    });
    let jobs = live
        .jobs
        .iter()
        .flat_map(|list| &list.jobs)
        .map(|job| Item {
            kind: Kind::Job,
            id: job.id.to_string(),
            state: job.state.as_str(),
            name: job.spec.name.to_string(),
            resource: match job.state {
                JobState::Active { resource } => Some(resource_label(resource, &known)),
                _ => None,
            },
        });
    let services = live.services.iter().flatten().map(|service| Item {
        kind: Kind::Service,
        id: service.id.to_string(),
        state: service.state.as_str(),
        name: service.name.clone(),
        resource: None,
    });

    tasks.chain(jobs).chain(services).collect()
}

/// The parts of the view the daemon did not give, by name
fn failures(live: &Live) -> Vec<(&'static str, &AppError)> {
    [
        ("tasks", live.tasks.as_ref().err()),
        ("gpus", live.resources.as_ref().err()),
        ("jobs", live.jobs.as_ref().err()),
        ("services", live.services.as_ref().err()),
    ]
    .into_iter()
    .filter_map(|(part, err)| err.map(|err| (part, err)))
    .collect()
}

/// `homebased ls`: every held, queued, or running task, GPU queue job, and
/// service on this machine
pub async fn list(ctx: &Ctx) -> Result<ExitCode, AppError> {
    let live = match Snapshot::read(ctx).await {
        Snapshot::Down(err) => return Err(err),
        Snapshot::Up(live) => live,
    };

    let items = items(&live);
    let failures = failures(&live);
    match ctx.output {
        OutputMode::Json => {
            let errors: serde_json::Map<String, Value> = failures
                .iter()
                .map(|(part, err)| {
                    let err = json!({ "error": err.code(), "message": err.to_string() });
                    ((*part).to_owned(), err)
                })
                .collect();
            ctx.print_json(json!({
                "items": items,
                "errors": errors,
                "commands": Guide::List.pointers(),
            }))?;
        }
        OutputMode::Quiet => items.iter().for_each(|item| println!("{}", item.id)),
        OutputMode::Human => {
            print_items(&items);
            for (part, err) in &failures {
                println!("{part} not read: {err}");
            }

            println!("\nEach kind in full:");
            print_pointers(&mut io::stdout(), Guide::List.pointers())?;
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn print_items(items: &[Item]) {
    if items.is_empty() {
        println!("nothing running, queued, or held");
        return;
    }

    println!("{:<8} {:<36} {:<9} NAME", "KIND", "ID", "STATE");
    for item in items {
        let on = item
            .resource
            .as_ref()
            .map_or_else(String::new, |resource| format!("  on {resource}"));
        println!(
            "{:<8} {:<36} {:<9} {}{on}",
            item.kind.as_str(),
            item.id,
            item.state,
            item.name
        );
    }
}
