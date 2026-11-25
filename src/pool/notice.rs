//! The provider's notice that it ends a temporary machine soon
//!
//! AWS gives a Spot instance about two minutes' notice before it takes the
//! instance back, through the instance metadata service: with IMDSv2 the
//! machine gets a session token with a `PUT`, then reads
//! `spot/instance-action`, which is `404` until a notice exists. cloudlease
//! says in `lease.json` which notice a machine's provider gives, so a
//! machine on any other provider never polls anything

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::curl::{CurlMethod, CurlRequest, send};

/// How often a machine reads its provider's notice; AWS suggests every 5 seconds
pub const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The instance metadata service's address on EC2
pub const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254";

/// Time limit for one metadata call; the service is on the instance's own
/// link, so anything slower means it is not there
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);

/// Lifetime the machine asks for its IMDSv2 session token, the most AWS allows
const TOKEN_TTL_SECS: u64 = 21_600;

/// A token is renewed this long before it would expire
const TOKEN_MARGIN: Duration = Duration::from_secs(600);

/// Which notice a machine's provider gives before it ends the machine, as
/// cloudlease writes it in `lease.json`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoticeSource {
    /// EC2's instance metadata service, version 2: the Spot instance action
    AwsImds,
}

/// The provider's notice that it ends the machine
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderNotice {
    /// What the provider does then, such as `terminate`
    pub action: String,
    /// When
    pub ends_at: DateTime<Utc>,
}

/// EC2's answer at `spot/instance-action`
#[derive(Debug, Deserialize)]
struct InstanceAction {
    action: String,
    time: DateTime<Utc>,
}

/// Reads the Spot instance action from EC2's metadata service with IMDSv2
#[derive(Debug)]
pub struct AwsImds {
    endpoint: String,
    token: Option<(String, Instant)>,
}

impl AwsImds {
    /// A reader of the service at `endpoint`, such as [`DEFAULT_IMDS_ENDPOINT`]
    #[must_use]
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            token: None,
        }
    }

    /// The notice, or `None` while there is none
    ///
    /// Blocks for up to a few seconds; a failed call is an error the caller
    /// retries at its next poll
    pub fn instance_action(&mut self) -> Result<Option<ProviderNotice>, String> {
        let token = self.token()?;
        let url = format!("{}/latest/meta-data/spot/instance-action", self.endpoint);
        let response = send(&CurlRequest {
            method: CurlMethod::Get,
            url: &url,
            bearer: None,
            headers: &[("X-aws-ec2-metadata-token", &token)],
            json_body: None,
            timeout: REQUEST_TIMEOUT,
        })?;
        match response.status {
            200 => parse_instance_action(&response.body).map(Some),
            404 => Ok(None),
            // the token expired early, such as after the instance was stopped
            401 => {
                self.token = None;
                Err("the metadata service refused its session token".into())
            }
            status => Err(format!("{url} answered {status}")),
        }
    }

    fn token(&mut self) -> Result<String, String> {
        if let Some((token, expires)) = &self.token
            && Instant::now() + TOKEN_MARGIN < *expires
        {
            return Ok(token.clone());
        }
        let url = format!("{}/latest/api/token", self.endpoint);
        let ttl = TOKEN_TTL_SECS.to_string();
        let response = send(&CurlRequest {
            method: CurlMethod::Put,
            url: &url,
            bearer: None,
            headers: &[("X-aws-ec2-metadata-token-ttl-seconds", &ttl)],
            json_body: None,
            timeout: REQUEST_TIMEOUT,
        })?;
        if response.status != 200 || response.body.trim().is_empty() {
            return Err(format!("{url} answered {}", response.status));
        }
        let token = response.body.trim().to_owned();
        self.token = Some((
            token.clone(),
            Instant::now() + Duration::from_secs(TOKEN_TTL_SECS),
        ));
        Ok(token)
    }
}

fn parse_instance_action(body: &str) -> Result<ProviderNotice, String> {
    let action: InstanceAction = serde_json::from_str(body)
        .map_err(|error| format!("unreadable Spot instance action {body:?}: {error}"))?;
    Ok(ProviderNotice {
        action: action.action,
        ends_at: action.time,
    })
}

#[cfg(test)]
mod tests {
    use super::{NoticeSource, parse_instance_action};

    #[test]
    fn reads_ec2s_instance_action_and_cloudleases_notice_source() {
        let notice =
            parse_instance_action(r#"{"action": "terminate", "time": "2026-11-25T03:12:00Z"}"#)
                .unwrap();
        assert_eq!(notice.action, "terminate");
        assert_eq!(notice.ends_at.to_rfc3339(), "2026-11-25T03:12:00+00:00");
        assert!(parse_instance_action("<html>").is_err());

        let source: NoticeSource = serde_json::from_str("\"aws_imds\"").unwrap();
        assert_eq!(source, NoticeSource::AwsImds);
    }
}
