//! Git bundles that carry pool jobs' commits from the coordinator to the
//! temporary machines that run them
//!
//! At submit, the coordinator makes the bundle of a pool job's commit (see
//! [`crate::pool::bundle`]), so a commit it cannot fetch refuses the job
//! before any machine is asked for. When the run starts on a machine, its
//! checkout wrapper runs `homebased ci fetch-bundle <job>`, which asks the
//! machine's daemon for one download attempt through `POST /v1/ci/bundles`
//! and tries again on a transient failure. The daemon downloads the bundle
//! over the Fleet link from the coordinator's member route, checks its size
//! and sha256, and keeps one copy per commit for every slot

use std::path::{Path as FsPath, PathBuf};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures_util::stream::unfold;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tracing::info;

use super::AppState;
use super::actors::{StoreMsg, call};
use super::fleet_link::LinkPeer;
use super::queue_api::QueueJson;
use crate::domain::API_VERSION;
use crate::error::AppError;
use crate::fleet::http::{ClusterClient, DEFAULT_MAX_BODY, Download};
use crate::pool::bundle::{BundleFile, FetchAnswer, PoolBundles, file_digest};
use crate::pool::{PoolName, PoolState};
use crate::queue::ci::{CiSpec, GitCheckout};
use crate::queue::{JobId, QueueError};

/// Header that carries a bundle's lowercase hex sha256
pub const SHA256_HEADER: &str = "x-homebased-bundle-sha256";

/// Path of a job's bundle on its coordinator, over the Fleet link
#[must_use]
pub fn member_path(job: JobId) -> String {
    format!("/v1/cluster/pool/jobs/{job}/bundle")
}

/// Route pattern of [`member_path`]
pub(super) const MEMBER_ROUTE: &str = "/v1/cluster/pool/jobs/{job}/bundle";

/// Time limit for the coordinator's answer to start
const HEAD_TIMEOUT: Duration = Duration::from_secs(60);

/// Longest the bundle's body may go without a byte before the attempt fails
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Bytes the coordinator reads from the bundle file at a time
const CHUNK: usize = 256 * 1024;

/// Socket route of a temporary machine: download one job's bundle
pub fn socket_routes() -> Router<AppState> {
    Router::new().route("/v1/ci/bundles", post(fetch))
}

/// The coordinator's member route that serves a job's bundle
pub(super) fn member_route() -> axum::routing::MethodRouter<AppState> {
    get(serve)
}

/// Make the bundle of a pool job's commit at submit, so the job is refused
/// now if the coordinator cannot fetch it
pub(super) async fn prepare(state: &AppState, ci: &CiSpec) -> Result<(), AppError> {
    let Some(git) = &ci.git else {
        return Ok(());
    };
    let pool = PoolName::from_class(ci.class.clone());
    // one mirror takes one fetch at a time, and a bundle's ref is per commit
    let _guard = state.locks.pool_bundles.lock(pool.clone()).await;
    let file = PoolBundles::new(&state.home, &pool).prepare(git).await?;
    info!(%pool, commit = git.commit.as_str(), bytes = file.len, "pool job bundle ready");
    Ok(())
}

/// Remove a released pool's bundles and mirrors
pub(super) fn remove(state: &AppState, pool: &PoolName) {
    if let Err(error) = PoolBundles::new(&state.home, pool).remove() {
        tracing::warn!(%pool, "remove the pool's bundles: {error}");
    }
}

/// Remove the bundles of pool `name` if it is still released
///
/// A submit that was making a bundle as the pool was released may have
/// written it after the release removed the rest. The pool may have been
/// created again since the caller read it released, and that pool's submits
/// make bundles under the same lock, so the state is read again under it
pub(super) async fn remove_if_released(state: &AppState, name: &PoolName) -> Result<(), AppError> {
    let _guard = state.locks.pool_bundles.lock(name.clone()).await;
    let pool = call(&state.store, |reply| StoreMsg::Pool {
        name: name.clone(),
        reply,
    })
    .await?;
    if pool.is_none_or(|pool| matches!(pool.state, PoolState::Released { .. })) {
        remove(state, name);
    }
    Ok(())
}

/// `GET /v1/cluster/pool/jobs/{job}/bundle` over the Fleet link: the bundle
/// of a job this coordinator placed on the asking machine, and no other
async fn serve(
    State(state): State<AppState>,
    Extension(peer): Extension<LinkPeer>,
    Path(job): Path<JobId>,
) -> Result<Response, AppError> {
    let route = call(&state.store, |reply| StoreMsg::JobRoute { job, reply })
        .await?
        .ok_or(QueueError::JobNotFound { job })?;
    peer.require_member(route.authority)?;
    let ci = route.spec.ci.as_ref();
    let Some((ci, git)) = ci.and_then(|ci| Some((ci, ci.git.as_ref()?))) else {
        return Err(AppError::Usage {
            message: format!("job {job} checks out no commit"),
        });
    };
    let pool = PoolName::from_class(ci.class.clone());
    let file = PoolBundles::new(&state.home, &pool)
        .bundle(&git.commit)?
        .ok_or_else(|| AppError::FileNotFound {
            message: format!(
                "the coordinator has no bundle of commit {} for job {job}",
                git.commit.as_str()
            ),
        })?;
    let reader = tokio::fs::File::open(&file.path).await?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/x-git-bundle"),
    );
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(file.len));
    headers.insert(
        SHA256_HEADER,
        HeaderValue::from_str(&file.sha256).map_err(|error| AppError::Internal {
            message: format!("bundle digest header: {error}"),
        })?,
    );
    info!(%job, commit = git.commit.as_str(), bytes = file.len, "serving pool job bundle");
    Ok((StatusCode::OK, headers, Body::from_stream(chunks(reader))).into_response())
}

fn chunks(
    file: tokio::fs::File,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send {
    unfold(Some(file), |file| async move {
        let mut file = file?;
        let mut buffer = vec![0_u8; CHUNK];
        match file.read(&mut buffer).await {
            Ok(0) => None,
            Ok(read) => {
                buffer.truncate(read);
                Some((Ok(Bytes::from(buffer)), Some(file)))
            }
            Err(error) => Some((Err(error), None)),
        }
    })
}

/// `POST /v1/ci/bundles` body
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FetchBody {
    job: JobId,
}

/// One download attempt of a job's bundle on a temporary machine
///
/// A failure another attempt cannot fix, such as a refusal from the
/// coordinator, is an error; one it may fix, such as a broken or damaged
/// transfer, answers [`FetchAnswer::Retry`]
async fn fetch(
    State(state): State<AppState>,
    QueueJson(body): QueueJson<FetchBody>,
) -> Result<Json<FetchAnswer>, AppError> {
    if !state.temporary.is_temporary() {
        return Err(AppError::Usage {
            message: "only a temporary machine gets commits from its coordinator's bundles".into(),
        });
    }
    let job = body.job;
    let record = call(&state.store, |reply| StoreMsg::QueueJob { id: job, reply })
        .await?
        .ok_or(QueueError::JobNotFound { job })?;
    let Some(git) = record.spec.ci.as_ref().and_then(|ci| ci.git.as_ref()) else {
        return Err(AppError::Usage {
            message: format!("job {job} checks out no commit"),
        });
    };
    let path = cache_path(state.home.root(), git);
    // slots that start jobs on one commit download it once
    let _guard = state.locks.bundle_fetches.lock(git.commit.clone()).await;
    if tokio::fs::try_exists(&path).await? {
        return Ok(Json(FetchAnswer::Ready {
            api_version: API_VERSION,
            path,
        }));
    }
    let answer = match download(&state, record.origin, job, &path).await {
        Ok(()) => FetchAnswer::Ready {
            api_version: API_VERSION,
            path,
        },
        Err(Fault::Transient(message)) => FetchAnswer::Retry {
            api_version: API_VERSION,
            message,
        },
        Err(Fault::Final(error)) => return Err(error),
    };
    Ok(Json(answer))
}

/// Where a machine keeps the bundle of `git`'s commit
fn cache_path(home: &FsPath, git: &GitCheckout) -> PathBuf {
    home.join("ci")
        .join("bundles")
        .join(format!("{}.bundle", git.commit.as_str()))
}

/// Why a download attempt failed
enum Fault {
    /// Another attempt may work
    Transient(String),
    /// Another attempt would fail the same way
    Final(AppError),
}

impl From<AppError> for Fault {
    fn from(error: AppError) -> Self {
        Self::Final(error)
    }
}

impl From<std::io::Error> for Fault {
    fn from(error: std::io::Error) -> Self {
        Self::Final(error.into())
    }
}

/// Download job `job`'s bundle from `coordinator` to `path`, through a
/// partial file that becomes `path` only once its size and sha256 match
async fn download(
    state: &AppState,
    coordinator: crate::machine::MachineId,
    job: JobId,
    path: &FsPath,
) -> Result<(), Fault> {
    let fleet = state.fleet.handle().ok_or_else(|| AppError::Usage {
        message: "a temporary machine needs the Fleet to reach its coordinator".into(),
    })?;
    let destination = fleet
        .connect(coordinator)
        .await
        .map_err(|error| Fault::Transient(format!("reach the coordinator: {error}")))?;
    if let Some(dir) = path.parent() {
        tokio::fs::create_dir_all(dir).await?;
    }
    let partial = path.with_extension("bundle.part");
    let mut file = tokio::fs::File::create(&partial).await?;
    let downloaded = ClusterClient::new(HEAD_TIMEOUT, DEFAULT_MAX_BODY)
        .download(
            &destination.address,
            &member_path(job),
            IDLE_TIMEOUT,
            &mut file,
        )
        .await;
    drop(file);
    let checked = match downloaded {
        Ok(download) => check(&partial, &download).await,
        Err(error) => Err(Fault::Transient(format!("download: {error}"))),
    };
    if checked.is_err() {
        let _ = tokio::fs::remove_file(&partial).await;
        return checked;
    }
    tokio::fs::rename(&partial, path).await?;
    info!(%job, path = %path.display(), "pool job bundle downloaded");
    Ok(())
}

/// Check what arrived at `partial` against the size and sha256 the
/// coordinator announced
async fn check(partial: &FsPath, download: &Download) -> Result<(), Fault> {
    if download.status.is_server_error() {
        return Err(Fault::Transient(format!(
            "the coordinator answered {}",
            download.status
        )));
    }
    if !download.status.is_success() {
        return Err(crate::client::map_error(download.status, &download.body).into());
    }
    let header = |name: &str| {
        download
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let (Some(len), Some(sha256)) = (
        header(header::CONTENT_LENGTH.as_str()),
        header(SHA256_HEADER),
    ) else {
        return Err(AppError::Internal {
            message: "the coordinator sent a bundle without its size and sha256".into(),
        }
        .into());
    };
    let path = partial.to_path_buf();
    let got: BundleFile = tokio::task::spawn_blocking(move || file_digest(&path))
        .await
        .map_err(|error| AppError::Internal {
            message: format!("bundle digest thread: {error}"),
        })??;
    if got.len.to_string() != len || got.sha256 != sha256 {
        return Err(Fault::Transient(format!(
            "the bundle arrived damaged: {} bytes with sha256 {}, expected {len} bytes with \
             sha256 {sha256}",
            got.len, got.sha256
        )));
    }
    Ok(())
}
