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
// a new server can still be starting when its runtime file appears
const PROBE_RETRIES: u8 = 5;

#[derive(Default)]
struct WatchState {
    last_runtime: Option<Vec<u8>>,
    last_alerted: Option<String>,
    retries_left: u8,
}

impl WatchState {
    fn changed_runtime(&mut self, content: Option<Vec<u8>>) -> bool {
        let changed = content.is_some() && content != self.last_runtime;
        self.last_runtime = content;
        changed
    }

    /// Probe after a runtime change, and again after a probe that could not finish
    fn probe_due(&mut self, content: Option<Vec<u8>>) -> bool {
        if self.changed_runtime(content) {
            self.retries_left = PROBE_RETRIES;
            return true;
        }
        if self.retries_left == 0 {
            return false;
        }
        self.retries_left -= 1;
        true
    }

    fn probe_finished(&mut self, status: ProbeStatus) {
        if matches!(status, ProbeStatus::Compatible | ProbeStatus::Changed) {
            self.retries_left = 0;
        }
    }

    fn alert_sent(&mut self, fingerprint: String) {
        self.last_alerted = Some(fingerprint);
        self.retries_left = 0;
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
                // retry on unchanged runtime metadata until the push succeeds
                self.retries_left = PROBE_RETRIES;
                Some(fingerprint)
            }
            ProbeStatus::NotInstalled | ProbeStatus::NotRunning | ProbeStatus::Unavailable => None,
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
        if !state.probe_due(content) {
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
        state.probe_finished(report.status);
        let failures = report
            .checks
            .iter()
            .filter(|check| !check.ok)
            .map(|check| format!("{}: {}", check.name, check.detail))
            .collect::<Vec<_>>()
            .join("; ");
        match report.status {
            ProbeStatus::Compatible => info!(
                protocol = report.orchestration_protocol,
                "T3 Code API compatible"
            ),
            ProbeStatus::Changed => warn!("T3 Code API changed: {failures}"),
            ProbeStatus::Unavailable => warn!("T3 Code API probe incomplete: {failures}"),
            ProbeStatus::NotInstalled | ProbeStatus::NotRunning => {
                debug!(status = ?report.status, "T3 Code API probe skipped: {failures}");
            }
        }
        let Some(notifier) = notifier.clone() else {
            continue;
        };
        let Some(fingerprint) = state.new_alert(&report) else {
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
            Ok(Ok(())) => state.alert_sent(fingerprint),
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

    #[test]
    fn bugfix_failed_alert_remains_eligible_on_the_same_runtime() {
        let mut state = WatchState::default();
        let runtime = Some(b"runtime".to_vec());
        assert!(state.probe_due(runtime.clone()));
        let changed = report(ProbeStatus::Changed, "one");
        state.probe_finished(changed.status);
        assert_eq!(state.new_alert(&changed), Some("api: one".into()));
        assert!(state.probe_due(runtime));
        assert_eq!(state.new_alert(&changed), Some("api: one".into()));
    }
    use super::WatchState;
    use crate::t3::{ProbeCheck, ProbeReport, ProbeStatus};

    fn report(status: ProbeStatus, detail: &str) -> ProbeReport {
        ProbeReport {
            status,
            t3_version: None,
            orchestration_protocol: None,
            checks: vec![ProbeCheck {
                name: "api",
                ok: false,
                detail: detail.into(),
            }],
        }
    }

    #[test]
    fn an_incomplete_probe_retries_a_bounded_number_of_times() {
        let mut state = WatchState::default();
        assert!(state.probe_due(Some(b"first".to_vec())));
        state.probe_finished(ProbeStatus::Unavailable);
        let retries = (0..10)
            .take_while(|_| state.probe_due(Some(b"first".to_vec())))
            .count();
        assert_eq!(retries, usize::from(super::PROBE_RETRIES));

        assert!(state.probe_due(Some(b"second".to_vec())));
        state.probe_finished(ProbeStatus::Compatible);
        assert!(!state.probe_due(Some(b"second".to_vec())));
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
        state.alert_sent("api: one".into());
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
