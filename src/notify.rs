//! Optional push notifications through ntfy

use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::Config;
use crate::curl::{self, CurlMethod, CurlRequest};

const TOKEN_FILE_LIMIT: usize = 4 * 1024;

/// Default server for ntfy notifications
pub const DEFAULT_NTFY_SERVER: &str = "https://ntfy.sh";

/// A validated ntfy topic name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct NtfyTopic(String);

impl NtfyTopic {
    /// Validate a topic with 1–64 ASCII letters, digits, underscores, or hyphens
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() || raw.len() > 64 {
            return Err("ntfy topic must contain 1 to 64 characters".into());
        }
        if raw
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || matches!(character, '_' | '-')))
        {
            return Err("ntfy topic may contain only ASCII letters, digits, '_' and '-'".into());
        }
        Ok(Self(raw.to_string()))
    }

    /// Borrow the validated topic name
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NtfyTopic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for NtfyTopic {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::parse(raw)
    }
}

impl<'de> Deserialize<'de> for NtfyTopic {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// Validated ntfy server, topic, and optional token-file settings
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NtfyConfig {
    server: String,
    topic: NtfyTopic,
    token_file: Option<PathBuf>,
}

impl NtfyConfig {
    /// Create ntfy settings and validate the server URL
    pub fn new(
        topic: NtfyTopic,
        server: Option<String>,
        token_file: Option<PathBuf>,
    ) -> Result<Self, String> {
        let server = normalize_server(server.unwrap_or_else(|| DEFAULT_NTFY_SERVER.into()))?;
        Ok(Self {
            server,
            topic,
            token_file,
        })
    }

    /// Borrow the normalized server URL
    #[must_use]
    pub fn server(&self) -> &str {
        &self.server
    }

    /// Borrow the validated topic
    #[must_use]
    pub fn topic(&self) -> &NtfyTopic {
        &self.topic
    }

    /// Borrow the optional token-file path
    #[must_use]
    pub fn token_file(&self) -> Option<&Path> {
        self.token_file.as_deref()
    }
}

fn normalize_server(raw: String) -> Result<String, String> {
    let server = raw.trim().trim_end_matches('/');
    let authority = server
        .strip_prefix("https://")
        .or_else(|| server.strip_prefix("http://"))
        .ok_or_else(|| "notify.ntfy.server must start with http:// or https://".to_string())?;
    if authority.is_empty() || authority.starts_with('/') {
        return Err("notify.ntfy.server must include a host".into());
    }
    Ok(server.to_string())
}

/// Optional notification providers
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Notify {
    /// ntfy settings, when push notifications are enabled
    pub ntfy: Option<NtfyConfig>,
}

/// Priority levels supported by the test notice and reusable notifier
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum NoticePriority {
    /// Normal ntfy priority, level 3
    #[default]
    Default,
    /// High ntfy priority, level 4
    High,
}

impl NoticePriority {
    fn ntfy_level(self) -> u8 {
        match self {
            Self::Default => 3,
            Self::High => 4,
        }
    }
}

/// Content and priority for one ntfy notification
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// Notification title
    pub title: String,
    /// Notification message
    pub message: String,
    /// ntfy tags such as emoji names
    pub tags: Vec<String>,
    /// Notification priority
    pub priority: NoticePriority,
}

/// Blocking ntfy notification sender
#[derive(Debug, Clone)]
pub struct Notifier {
    config: NtfyConfig,
}

impl Notifier {
    /// Create a notifier when ntfy is configured
    #[must_use]
    pub fn from_config(config: &Config) -> Option<Self> {
        config.notify.ntfy.clone().map(|config| Self { config })
    }

    /// Borrow the configured topic
    #[must_use]
    pub fn topic(&self) -> &NtfyTopic {
        self.config.topic()
    }

    /// Send one notice and report a useful error if ntfy rejects it
    pub fn send(&self, notice: &Notice) -> Result<(), String> {
        let token = self.config.token_file().map(read_token).transpose()?;

        let body = serde_json::json!({
            "topic": self.config.topic.as_str(),
            "title": notice.title,
            "message": notice.message,
            "tags": notice.tags,
            "priority": notice.priority.ntfy_level(),
        });
        let url = format!("{}/", self.config.server());
        let response = curl::send(&CurlRequest {
            method: CurlMethod::Post,
            url: &url,
            bearer: token.as_deref(),
            json_body: Some(&body),
            timeout: Duration::from_secs(30),
        })?;

        if !(200..300).contains(&response.status) {
            let excerpt = response_excerpt(&response.body, token.as_deref());
            if excerpt.is_empty() {
                return Err(format!("ntfy returned HTTP {}", response.status));
            }
            return Err(format!("ntfy returned HTTP {}: {excerpt}", response.status));
        }
        Ok(())
    }
}

fn read_token(path: &Path) -> Result<String, String> {
    let path = expand_home(path)?;
    // follow symlinks so dotfile managers work; a FIFO or directory target is
    // still rejected before any blocking open
    let metadata = fs::metadata(&path).map_err(|error| {
        format!(
            "could not inspect ntfy token file {}: {error}",
            path.display()
        )
    })?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "ntfy token file {} must be a regular file",
            path.display()
        ));
    }
    if metadata.len() > TOKEN_FILE_LIMIT as u64 {
        return Err(token_file_too_large(&path));
    }

    let file = fs::File::open(&path)
        .map_err(|error| format!("could not read ntfy token file {}: {error}", path.display()))?;
    let mut token = String::new();
    file.take((TOKEN_FILE_LIMIT + 1) as u64)
        .read_to_string(&mut token)
        .map_err(|error| format!("could not read ntfy token file {}: {error}", path.display()))?;
    if token.len() > TOKEN_FILE_LIMIT {
        return Err(token_file_too_large(&path));
    }

    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(format!(
            "ntfy token file {} must contain a non-empty token",
            path.display()
        ));
    }
    if token.chars().any(char::is_control) {
        return Err("ntfy token file must contain a token without control characters".into());
    }

    Ok(token)
}

fn token_file_too_large(path: &Path) -> String {
    format!(
        "ntfy token file {} exceeds the {TOKEN_FILE_LIMIT}-byte limit",
        path.display()
    )
}

fn expand_home(path: &Path) -> Result<PathBuf, String> {
    let raw = path.to_string_lossy();
    let Some(rest) = raw.strip_prefix("~/") else {
        return Ok(path.to_path_buf());
    };
    let home = std::env::var_os("HOME")
        .filter(|home| !home.is_empty())
        .ok_or_else(|| "HOME is unset; cannot expand the ntfy token path".to_string())?;
    Ok(PathBuf::from(home).join(rest))
}

fn response_excerpt(body: &str, token: Option<&str>) -> String {
    let body = token.filter(|token| !token.is_empty()).map_or_else(
        || body.to_string(),
        |token| body.replace(token, "[redacted]"),
    );
    let excerpt = body.chars().take(256).collect::<String>();
    excerpt.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_accepts_only_one_to_sixty_four_allowed_characters() {
        assert!(NtfyTopic::parse("homebased_1-2").is_ok());
        assert!(NtfyTopic::parse("").is_err());
        assert!(NtfyTopic::parse(&"a".repeat(65)).is_err());
        assert!(NtfyTopic::parse("topic.name").is_err());
        assert!(NtfyTopic::parse("topic café").is_err());
    }

    #[test]
    fn normalizes_server_and_maps_notice_priorities() {
        let config = NtfyConfig::new(
            NtfyTopic::parse("homebased_test").unwrap(),
            Some(" https://ntfy.example/// ".into()),
            None,
        )
        .unwrap();
        assert_eq!(config.server(), "https://ntfy.example");
        assert_eq!(NoticePriority::Default.ntfy_level(), 3);
        assert_eq!(NoticePriority::High.ntfy_level(), 4);
    }

    #[test]
    fn rejects_bad_server_schemes() {
        let error = NtfyConfig::new(
            NtfyTopic::parse("homebased_test").unwrap(),
            Some("ftp://ntfy.example".into()),
            None,
        )
        .unwrap_err();
        assert!(error.contains("http:// or https://"));
    }

    #[test]
    fn send_rejects_empty_or_whitespace_token_files_with_the_path() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("token");
        let config = NtfyConfig::new(
            NtfyTopic::parse("homebased_test").unwrap(),
            None,
            Some(path.clone()),
        )
        .unwrap();
        let notifier = Notifier { config };
        let notice = Notice {
            title: "test".into(),
            message: "test".into(),
            tags: Vec::new(),
            priority: NoticePriority::Default,
        };

        for contents in [b"".as_slice(), b" \t\n".as_slice()] {
            fs::write(&path, contents).unwrap();

            let error = notifier.send(&notice).unwrap_err();

            assert!(error.contains(&path.display().to_string()));
            assert!(error.contains("non-empty token"));
        }
    }

    #[test]
    fn read_token_rejects_oversized_files_and_non_regular_paths() {
        let directory = tempfile::tempdir().unwrap();
        let oversized = directory.path().join("oversized-token");
        fs::write(&oversized, vec![b'a'; TOKEN_FILE_LIMIT + 1]).unwrap();

        let error = read_token(&oversized).unwrap_err();

        assert!(error.contains(&oversized.display().to_string()));
        assert!(error.contains("exceeds the 4096-byte limit"));

        let non_regular = directory.path().join("token-directory");
        fs::create_dir(&non_regular).unwrap();

        let error = read_token(&non_regular).unwrap_err();

        assert!(error.contains(&non_regular.display().to_string()));
        assert!(error.contains("must be a regular file"));
    }

    #[test]
    fn read_token_follows_a_symlink_to_a_regular_file_only() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("token-target");
        fs::write(&target, "linked-token\n").unwrap();
        let link = directory.path().join("token");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert_eq!(read_token(&link).unwrap(), "linked-token");

        let fifo = directory.path().join("token-fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
        let fifo_link = directory.path().join("fifo-link");
        std::os::unix::fs::symlink(&fifo, &fifo_link).unwrap();

        let error = read_token(&fifo_link).unwrap_err();

        assert!(error.contains("must be a regular file"));
    }
}
