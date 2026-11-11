//! Services and the one event each may owe its owner
//!
//! Both the daemon and a service's `service-run` worker write a row. Every
//! change that could race, a stop request against the end the worker
//! records, runs in one immediate transaction and leaves an ended row as it
//! is, so a service ends exactly once and its event is decided with its end

use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Row, params};

use super::{Store, fmt_time, parse_time};
use crate::cleanup::ProcessIdentity;
use crate::domain::ThreadId;
use crate::error::AppError;
use crate::service::{Ending, Service, ServiceId, ServiceRecord, StopRequest};

const SERVICE_COLUMNS: &str = "service_json, worker_pid, child_json, started_at, ready_at, \
     stop_request, ending_json";

impl Store {
    /// Save a new service before its worker starts
    pub fn insert_service(&self, service: &Service) -> Result<(), AppError> {
        self.conn.execute(
            "INSERT INTO services (id, owner_thread, service_json, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                service.id.to_string(),
                service.owner.to_string(),
                serde_json::to_string(service)?,
                fmt_time(service.created_at)
            ],
        )?;
        Ok(())
    }

    /// One service, live or ended
    pub fn service_record(&self, id: ServiceId) -> Result<Option<ServiceRecord>, AppError> {
        self.conn
            .query_row(
                &format!("SELECT {SERVICE_COLUMNS} FROM services WHERE id = ?1"),
                [id.to_string()],
                read_record,
            )
            .optional()?
            .transpose()
    }

    /// Services oldest first: the live ones, or every one with `ended`, of
    /// `owner` or of every owner
    pub fn service_records(
        &self,
        owner: Option<ThreadId>,
        ended: bool,
    ) -> Result<Vec<ServiceRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {SERVICE_COLUMNS} FROM services
             WHERE (?1 IS NULL OR owner_thread = ?1) AND (?2 OR ended_at IS NULL)
             ORDER BY created_at, id"
        ))?;
        let rows = statement
            .query_map(
                params![owner.map(|owner| owner.to_string()), ended],
                read_record,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().collect()
    }

    /// Record the worker the daemon spawned
    pub fn set_service_worker(&self, id: ServiceId, pid: i32) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE services SET worker_pid = ?2 WHERE id = ?1 AND ended_at IS NULL",
            params![id.to_string(), pid],
        )?;
        Ok(())
    }

    /// Record the spawned service process, ready at once when `ready`
    pub fn set_service_started(
        &self,
        id: ServiceId,
        child: Option<ProcessIdentity>,
        ready: bool,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        let child = child
            .map(|child| serde_json::to_string(&child))
            .transpose()?;
        let at = fmt_time(now);
        self.conn.execute(
            "UPDATE services
             SET child_json = ?2, started_at = ?3, ready_at = CASE WHEN ?4 THEN ?3 END
             WHERE id = ?1 AND ended_at IS NULL",
            params![id.to_string(), child, at, ready],
        )?;
        Ok(())
    }

    /// Mark a starting service ready
    pub fn set_service_ready(&self, id: ServiceId, now: DateTime<Utc>) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE services SET ready_at = ?2
             WHERE id = ?1 AND ended_at IS NULL AND ready_at IS NULL",
            params![id.to_string(), fmt_time(now)],
        )?;
        Ok(())
    }

    /// Ask a live service to stop, keeping the first request, and return the
    /// service as it is afterwards
    pub fn request_service_stop(
        &self,
        id: ServiceId,
        request: StopRequest,
    ) -> Result<ServiceRecord, AppError> {
        self.immediate(|| {
            self.conn.execute(
                "UPDATE services SET stop_request = ?2
                 WHERE id = ?1 AND ended_at IS NULL AND stop_request IS NULL",
                params![id.to_string(), request.as_str()],
            )?;
            self.service_record(id)?
                .ok_or(AppError::ServiceNotFound { service: id })
        })
    }

    /// End a live service, and return whether this call ended it
    ///
    /// A stop that was asked for wins over what the worker saw, so a server
    /// that exits while it is being stopped still counts as stopped and
    /// sends no event. The event is due when the end sends one and the
    /// service has an owner to send it to
    pub fn end_service(&self, id: ServiceId, ending: Ending) -> Result<bool, AppError> {
        self.immediate(|| {
            let record = self
                .service_record(id)?
                .ok_or(AppError::ServiceNotFound { service: id })?;
            if record.ending.is_some() {
                return Ok(false);
            }
            let ending = match record.stop_request {
                Some(request) => Ending {
                    end: request.into(),
                    ..ending
                },
                None => ending,
            };
            // a service that never became ready failed `start`, which already
            // told the caller, so its end wakes nobody
            let due = ending.end.sends_event()
                && record.ready_at.is_some()
                && !record.service.owner.is_detached();
            self.conn.execute(
                "UPDATE services SET ended_at = ?2, ending_json = ?3, event_due = ?4
                 WHERE id = ?1",
                params![
                    id.to_string(),
                    fmt_time(ending.at),
                    serde_json::to_string(&ending)?,
                    due
                ],
            )?;
            Ok(true)
        })
    }

    /// Ended services whose event has not settled, oldest end first
    pub fn pending_service_events(&self) -> Result<Vec<ServiceRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "SELECT {SERVICE_COLUMNS} FROM services
             WHERE event_due = 1 AND event_settled_at IS NULL
             ORDER BY ended_at, id"
        ))?;
        let rows = statement
            .query_map([], read_record)?
            .collect::<Result<Vec<_>, _>>()?;
        rows.into_iter().collect()
    }

    /// Settle a service's event as delivered, suppressed, or failed
    pub fn settle_service_event(
        &self,
        id: ServiceId,
        delivery: &str,
        now: DateTime<Utc>,
    ) -> Result<(), AppError> {
        self.conn.execute(
            "UPDATE services SET event_settled_at = ?2, event_delivery = ?3
             WHERE id = ?1 AND event_due = 1 AND event_settled_at IS NULL",
            params![id.to_string(), fmt_time(now), delivery],
        )?;
        Ok(())
    }
}

fn read_record(row: &Row<'_>) -> rusqlite::Result<Result<ServiceRecord, AppError>> {
    let service: String = row.get(0)?;
    let worker_pid: Option<i32> = row.get(1)?;
    let child: Option<String> = row.get(2)?;
    let started_at: Option<String> = row.get(3)?;
    let ready_at: Option<String> = row.get(4)?;
    let stop_request: Option<String> = row.get(5)?;
    let ending: Option<String> = row.get(6)?;
    Ok((|| {
        Ok(ServiceRecord {
            service: serde_json::from_str(&service)?,
            worker_pid,
            child: child.as_deref().map(serde_json::from_str).transpose()?,
            started_at: started_at.as_deref().map(parse_time).transpose()?,
            ready_at: ready_at.as_deref().map(parse_time).transpose()?,
            stop_request: stop_request
                .as_deref()
                .map(StopRequest::from_storage)
                .transpose()?,
            ending: ending.as_deref().map(serde_json::from_str).transpose()?,
        })
    })())
}
