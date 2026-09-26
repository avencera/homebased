//! Watch the local T3 Code API contract after runtime metadata changes

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::notify::{Notice, NoticePriority, Notifier};
use crate::t3::{ProbeReport, ProbeStatus, T3Env, probe};

const PROBE_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Default)]
struct WatchState {
    last_runtime: Option<Vec<u8>>,
    last_alerted: Option<String>,
}

impl WatchState {
    fn changed_runtime(&mut self, content: Option<Vec<u8>>) -> bool {
        let changed = content.is_some() && content != self.last_runtime;
        self.last_runtime = content;
        changed
    }

    fn new_alert(&mut self, report: &ProbeReport) -> Option<String> {
        match report.status {
            ProbeStatus::Compatible => {
                self.last_alerted = None;
                None
            }
            ProbeStatus::Changed => {
                let fingerprint = report.fingerprint()?;
                if self.last_alerted.as_deref() == Some(&fingerprint) {
                    return None;
                }
                self.last_alerted = Some(fingerprint.clone());
                Some(fingerprint)
            }
            ProbeStatus::NotInstalled | ProbeStatus::NotRunning => None,
        }
    }
}

/// Probe a changed T3 runtime and alert once for each incompatible API fingerprint
pub(crate) async fn run(notifier: Option<Arc<Notifier>>, machine_name: String) {
    let env = T3Env::from_env();
    let runtime = env.home.join(".t3/userdata/server-runtime.json");
    let mut state = WatchState::default();
    let mut interval = tokio::time::interval(PROBE_INTERVAL);
    loop {
        interval.tick().await;
        let path = runtime.clone();
        let content = match tokio::task::spawn_blocking(move || read_runtime(path)).await {
            Ok(Ok(content)) => content,
            Ok(Err(error)) => {
                warn!("read T3 runtime metadata: {error}");
                continue;
            }
            Err(error) => {
                warn!("read T3 runtime metadata join: {error}");
                continue;
            }
        };
        if !state.changed_runtime(content) {
            continue;
        }
        let probe_env = env.clone();
        let report = match tokio::task::spawn_blocking(move || probe(&probe_env)).await {
            Ok(report) => report,
            Err(error) => {
                state.last_runtime = None;
                warn!("T3 API probe join: {error}");
                continue;
            }
        };
        match report.status {
            ProbeStatus::Compatible => info!("T3 Code API compatible"),
            ProbeStatus::Changed => warn!("T3 Code API changed"),
            ProbeStatus::NotInstalled | ProbeStatus::NotRunning => {
                debug!(status = ?report.status, "T3 Code API probe skipped");
            }
        }
        let Some(fingerprint) = state.new_alert(&report) else {
            continue;
        };
        let Some(notifier) = notifier.clone() else {
            continue;
        };
        let notice = Notice {
            title: format!("T3 Code API changed on {machine_name}"),
            message: format!(
                "{fingerprint}. Homebased cannot wake T3 Code threads until it is updated. Run homebased t3 check."
            ),
            tags: vec!["warning".into()],
            priority: NoticePriority::High,
        };
        match tokio::task::spawn_blocking(move || notifier.send(&notice)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!("T3 API change push failed: {error}"),
            Err(error) => warn!("T3 API change push join: {error}"),
        }
    }
}

fn read_runtime(path: PathBuf) -> io::Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::WatchState;
    use crate::t3::{ProbeCheck, ProbeReport, ProbeStatus};

    fn report(status: ProbeStatus, detail: &str) -> ProbeReport {
        ProbeReport {
            status,
            t3_version: None,
            checks: vec![ProbeCheck {
                name: "api",
                ok: false,
                detail: detail.into(),
            }],
        }
    }

    #[test]
    fn runtime_content_and_fingerprint_deduplicate_probes_and_alerts() {
        let mut state = WatchState::default();
        assert!(!state.changed_runtime(None));
        assert!(state.changed_runtime(Some(b"first".to_vec())));
        assert!(!state.changed_runtime(Some(b"first".to_vec())));
        assert_eq!(
            state.new_alert(&report(ProbeStatus::Changed, "one")),
            Some("api: one".into())
        );
        assert!(state.changed_runtime(Some(b"second".to_vec())));
        assert_eq!(state.new_alert(&report(ProbeStatus::Changed, "one")), None);
        assert!(state.changed_runtime(Some(b"third".to_vec())));
        assert_eq!(
            state.new_alert(&report(ProbeStatus::Changed, "two")),
            Some("api: two".into())
        );
        state.new_alert(&report(ProbeStatus::Compatible, ""));
        assert!(state.changed_runtime(Some(b"fourth".to_vec())));
        assert_eq!(
            state.new_alert(&report(ProbeStatus::Changed, "two")),
            Some("api: two".into())
        );
    }
}
