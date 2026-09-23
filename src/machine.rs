//! Machine identity: the stable installation UUID, the per-daemon boot UUID,
//! and the validated display name.
//!
//! A machine is one Homebased installation. Its [`MachineId`] lives in the
//! state directory and never changes for that installation. Each daemon start
//! allocates a fresh [`BootId`], which lets peers tell a restart of one daemon
//! apart from two live daemons that claim the same installation UUID.

use std::fmt;
use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::AppError;
use crate::home::Home;

/// Stable identity of one Homebased installation.
///
/// Created once per state directory. A cloned installation must start with a
/// new state directory, so it gets a new value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MachineId(Uuid);

impl MachineId {
    /// Allocate a new identity.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID, such as one read from a peer.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Underlying UUID.
    #[must_use]
    pub const fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for MachineId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MachineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MachineId {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s.trim())
            .map(Self)
            .map_err(|_| AppError::Usage {
                message: format!("invalid machine id (full UUID required): {s}"),
            })
    }
}

/// Identity of one daemon process lifetime. New on every `serve` start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BootId(Uuid);

impl BootId {
    /// Allocate the boot identity for this daemon start.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wrap an existing UUID, such as one read from a peer.
    #[must_use]
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl Default for BootId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Maximum length of a machine name. Matches one DNS label.
pub const MACHINE_NAME_MAX_LEN: usize = 63;

/// Unique display name of a machine, such as `main`, `code`, or `training`.
///
/// One lowercase DNS label: ASCII letters, digits, and inner hyphens. The
/// same rules make the name usable as an mDNS instance and as a host name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct MachineName(String);

impl MachineName {
    /// Validate a name exactly as given.
    pub fn parse(raw: &str) -> Result<Self, MachineNameError> {
        if raw.is_empty() {
            return Err(MachineNameError::Empty);
        }
        if raw.len() > MACHINE_NAME_MAX_LEN {
            return Err(MachineNameError::TooLong { len: raw.len() });
        }
        if let Some(ch) = raw
            .chars()
            .find(|ch| !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || *ch == '-'))
        {
            return Err(MachineNameError::InvalidChar { ch });
        }
        if raw.starts_with('-') || raw.ends_with('-') {
            return Err(MachineNameError::EdgeHyphen);
        }
        Ok(Self(raw.to_string()))
    }

    /// Derive a name from a host name: first label, lowercased, with invalid
    /// characters replaced by hyphens. `None` when nothing usable remains.
    #[must_use]
    pub fn from_hostname(hostname: &str) -> Option<Self> {
        let label = hostname.split('.').next().unwrap_or_default();
        let mapped: String = label
            .chars()
            .map(|ch| {
                let ch = ch.to_ascii_lowercase();
                if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
                    ch
                } else {
                    '-'
                }
            })
            .take(MACHINE_NAME_MAX_LEN)
            .collect();
        Self::parse(mapped.trim_matches('-')).ok()
    }

    /// Name used when neither the config nor the host name gives one.
    #[must_use]
    pub fn fallback() -> Self {
        Self("homebased".to_string())
    }

    /// Borrow the validated name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MachineName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for MachineName {
    type Err = MachineNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl<'de> Deserialize<'de> for MachineName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// Why a machine name was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MachineNameError {
    /// Empty string.
    #[error("machine name must not be empty")]
    Empty,
    /// Longer than one DNS label.
    #[error("machine name must be at most {max} bytes (got {len})", max = MACHINE_NAME_MAX_LEN)]
    TooLong {
        /// Byte length.
        len: usize,
    },
    /// Contains a character other than `a-z`, `0-9`, or `-`.
    #[error("machine name may contain only a-z, 0-9, and '-' (found {ch:?})")]
    InvalidChar {
        /// First offending character.
        ch: char,
    },
    /// Starts or ends with a hyphen.
    #[error("machine name must not start or end with '-'")]
    EdgeHyphen,
}

/// Identity of the running daemon: stable installation UUID plus this boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalIdentity {
    /// Stable installation UUID from the state directory.
    pub machine: MachineId,
    /// UUID of this daemon start.
    pub boot: BootId,
}

impl LocalIdentity {
    /// Load or create the machine UUID and allocate a new boot UUID.
    pub fn start(home: &Home) -> Result<Self, AppError> {
        Ok(Self {
            machine: load_or_create_machine_id(home)?,
            boot: BootId::new(),
        })
    }

    /// Whether an advertisement came from this exact daemon. Only this case is
    /// a self-advertisement; the same machine UUID with another boot UUID is a
    /// second live daemon that claims this installation.
    #[must_use]
    pub fn is_self(&self, machine: MachineId, boot: BootId) -> bool {
        self.machine == machine && self.boot == boot
    }

    /// Receiver-side check for a cluster request addressed to `destination`.
    /// Run this before reading task data or changing state.
    pub fn check_destination(&self, destination: MachineId) -> Result<(), AppError> {
        if destination == self.machine {
            return Ok(());
        }
        Err(AppError::MachineIdentityMismatch {
            expected: destination,
            found: Some(self.machine),
        })
    }
}

/// Read `machine-id` from the state directory, creating it on first use.
///
/// Creation is atomic and never replaces an existing file, so a CLI command and
/// the daemon that race on a fresh state directory agree on one UUID. A file
/// with invalid contents is an error, not a reason to mint a new identity.
pub fn load_or_create_machine_id(home: &Home) -> Result<MachineId, AppError> {
    let path = home.machine_id_path();
    if let Some(id) = read_machine_id(&path)? {
        return Ok(id);
    }
    fs::create_dir_all(home.root())?;
    let candidate = MachineId::new();
    let tmp = home
        .root()
        .join(format!(".machine-id.{}.tmp", Uuid::now_v7()));
    write_synced(&tmp, format!("{candidate}\n").as_bytes())?;
    // a hard link fails when the target exists, which gives create-if-absent
    // without replacing an identity another process just wrote
    let linked = fs::hard_link(&tmp, &path);
    // the temporary name is garbage in every outcome
    let _ = fs::remove_file(&tmp);
    match linked {
        Ok(()) => Ok(candidate),
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            read_machine_id(&path)?.ok_or_else(|| AppError::Internal {
                message: format!("{} vanished after creation", path.display()),
            })
        }
        Err(err) => Err(err.into()),
    }
}

fn read_machine_id(path: &Path) -> Result<Option<MachineId>, AppError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let uuid = Uuid::parse_str(text.trim()).map_err(|err| AppError::Internal {
        message: format!(
            "{} does not hold a machine UUID ({err}); restore it or remove it to create a new machine identity",
            path.display()
        ),
    })?;
    Ok(Some(MachineId(uuid)))
}

/// Write bytes to a new file and flush them to disk.
pub(crate) fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let mut file = fs::File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

/// Best-effort host name for a default machine name.
#[must_use]
pub fn host_machine_name() -> Option<MachineName> {
    let host = nix::unistd::gethostname().ok()?;
    MachineName::from_hostname(&host.to_string_lossy())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home() -> (tempfile::TempDir, Home) {
        let dir = tempfile::tempdir().unwrap();
        let home = Home::resolve(Some(dir.path().join("state"))).unwrap();
        (dir, home)
    }

    #[test]
    fn machine_id_is_stable_across_starts() {
        let (_dir, home) = home();
        let first = LocalIdentity::start(&home).unwrap();
        let second = LocalIdentity::start(&home).unwrap();
        assert_eq!(first.machine, second.machine);
        assert_ne!(first.boot, second.boot);
        let text = fs::read_to_string(home.machine_id_path()).unwrap();
        assert_eq!(text.trim(), first.machine.to_string());
    }

    #[test]
    fn corrupt_machine_id_is_an_error_not_a_new_identity() {
        let (_dir, home) = home();
        fs::create_dir_all(home.root()).unwrap();
        fs::write(home.machine_id_path(), "not a uuid").unwrap();
        let err = load_or_create_machine_id(&home).unwrap_err();
        assert!(matches!(err, AppError::Internal { .. }), "{err:?}");
        assert_eq!(
            fs::read_to_string(home.machine_id_path()).unwrap(),
            "not a uuid"
        );
    }

    #[test]
    fn concurrent_creation_agrees_on_one_identity() {
        let (_dir, home) = home();
        let ids: Vec<MachineId> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| load_or_create_machine_id(&home).unwrap()))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]), "{ids:?}");
    }

    #[test]
    fn self_requires_both_uuids() {
        let identity = LocalIdentity {
            machine: MachineId::new(),
            boot: BootId::new(),
        };
        assert!(identity.is_self(identity.machine, identity.boot));
        assert!(!identity.is_self(identity.machine, BootId::new()));
        assert!(!identity.is_self(MachineId::new(), identity.boot));
    }

    #[test]
    fn destination_check_rejects_other_machine() {
        let identity = LocalIdentity {
            machine: MachineId::new(),
            boot: BootId::new(),
        };
        identity.check_destination(identity.machine).unwrap();
        let other = MachineId::new();
        let err = identity.check_destination(other).unwrap_err();
        assert_eq!(err.code(), "machine_identity_mismatch");
    }

    #[test]
    fn names_follow_dns_label_rules() {
        assert_eq!(MachineName::parse("code").unwrap().as_str(), "code");
        assert_eq!(MachineName::parse("ai-5090").unwrap().as_str(), "ai-5090");
        assert_eq!(MachineName::parse(""), Err(MachineNameError::Empty));
        assert_eq!(
            MachineName::parse("Code"),
            Err(MachineNameError::InvalidChar { ch: 'C' })
        );
        assert_eq!(MachineName::parse("-x"), Err(MachineNameError::EdgeHyphen));
        assert!(matches!(
            MachineName::parse(&"a".repeat(64)),
            Err(MachineNameError::TooLong { len: 64 })
        ));
    }

    #[test]
    fn hostname_derivation() {
        assert_eq!(
            MachineName::from_hostname("Praveens-MacBook.local")
                .unwrap()
                .as_str(),
            "praveens-macbook"
        );
        assert_eq!(
            MachineName::from_hostname("ai_5090").unwrap().as_str(),
            "ai-5090"
        );
        assert_eq!(MachineName::from_hostname("---"), None);
    }
}
