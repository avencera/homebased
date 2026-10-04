//! Eligibility at the callback write boundary

use std::sync::Arc;

/// A send can fail without suppressing the numbered notice
#[derive(Debug, thiserror::Error)]
pub(crate) enum SendFailure {
    /// The notice episode ended while delivery was being prepared
    #[error("notice episode ended")]
    Suppressed,
    /// Eligibility or transport could not be confirmed
    #[error("{0}")]
    Failed(String),
}

impl From<String> for SendFailure {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

/// Recheck the exact notice after all preparation waits, while holding the delivery lock
pub(crate) type SendCheck = Arc<dyn Fn() -> Result<(), SendFailure> + Send + Sync>;

/// Delivery lock and optional eligibility check for one transport attempt
#[derive(Clone, Copy)]
pub(crate) struct SendGate<'a> {
    pub(crate) path: &'a std::path::Path,
    pub(crate) check: Option<&'a SendCheck>,
}

impl SendGate<'_> {
    pub(crate) fn check(self) -> Result<(), SendFailure> {
        self.check.map_or(Ok(()), |check| check())
    }

    pub(crate) fn lock(self) -> Result<Option<std::fs::File>, SendFailure> {
        if self.check.is_none() {
            return Ok(None);
        }
        let deadline = std::time::Instant::now() + super::QUEUE_ATTEMPT_TIMEOUT;
        loop {
            match crate::home::flock_exclusive(self.path, crate::home::LockMode::NonBlocking) {
                Ok(lock) => return Ok(Some(lock)),
                Err(crate::error::AppError::LockHeld { .. })
                    if std::time::Instant::now() < deadline =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(error) => return Err(SendFailure::Failed(error.to_string())),
            }
        }
    }
}
