//! Queue resources: GPU detection, manual registration, and lookup

use chrono::Utc;
use rusqlite::{OptionalExtension, params};

use super::rows::{RESOURCE_SELECT, RawResource};
use super::{ResourceOrigin, ResourceRecord};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::gpu::DetectedResource;
use crate::queue::{QueueError, ResourceId, ResourceName, ResourceSelector};
use crate::store::{Store, fmt_time};

impl Store {
    /// Ensure one resource per detected GPU on `machine`
    ///
    /// Idempotent: an existing resource keeps its UUID, and a detected GPU
    /// whose name is taken by a hand-registered resource is left alone.
    /// An idle fallback adopts its first device's name unless that name is taken
    /// Detected resources are never removed, so a GPU that disappears leaves
    /// its resource in place
    pub fn ensure_detected_resources(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<Vec<ResourceRecord>, AppError> {
        self.immediate(|| {
            let existing = self.resources_on(machine)?;
            let first_device = detected.iter().find_map(|resource| resource.device);
            let Some(device) = first_device else {
                // a fallback is never added beside an indexed resource
                if existing
                    .iter()
                    .any(|record| record.resource.device.is_some())
                {
                    return Ok(existing);
                }
                self.insert_detected(machine, detected)?;
                return self.resources_on(machine);
            };

            let fallback = existing
                .iter()
                .find(|record| record.origin == ResourceOrigin::DetectedFallback);
            if let Some(fallback) = fallback
                && !self.adopt_fallback_device(machine, &existing, fallback, device)?
            {
                return Ok(existing);
            }
            self.insert_detected(machine, detected)?;
            self.resources_on(machine)
        })
    }

    /// Give an idle fallback the first detected device, keeping its UUID
    ///
    /// Returns `false` when detection must wait for a later start: the fallback
    /// has an active run, or another resource already holds the device
    fn adopt_fallback_device(
        &self,
        machine: MachineId,
        existing: &[ResourceRecord],
        fallback: &ResourceRecord,
        device: u32,
    ) -> Result<bool, AppError> {
        let id = fallback.resource.id;
        if fallback.run.is_some() {
            tracing::warn!(
                %machine,
                resource = %id,
                "GPU detection deferred while the fallback has an active run"
            );
            return Ok(false);
        }
        if existing
            .iter()
            .any(|record| record.resource.device == Some(device))
        {
            tracing::warn!(
                %machine,
                device,
                "GPU detection deferred because the fallback device is already registered"
            );
            return Ok(false);
        }

        let detected_name = ResourceName::gpu(device);
        let name_taken = existing
            .iter()
            .any(|record| record.resource.id != id && record.resource.name == detected_name);
        let name = if name_taken {
            tracing::warn!(
                %machine,
                resource = %id,
                name = %detected_name,
                "GPU fallback keeps its name because the detected device name is already registered"
            );
            &fallback.resource.name
        } else {
            &detected_name
        };
        self.conn.execute(
            "UPDATE resources SET device = ?2, name = ?3, origin = ?4 WHERE id = ?1",
            params![
                id.to_string(),
                i64::from(device),
                name.as_str(),
                ResourceOrigin::DetectedDevice.as_str()
            ],
        )?;
        Ok(true)
    }

    /// Insert each detected resource whose name and device are both free
    fn insert_detected(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<(), AppError> {
        for resource in detected {
            let origin = match resource.device {
                Some(_) => ResourceOrigin::DetectedDevice,
                None => ResourceOrigin::DetectedFallback,
            };
            self.conn.execute(
                "INSERT INTO resources (id, machine, name, device, created_at, origin)
                 SELECT ?1, ?2, ?3, ?4, ?5, ?6
                 WHERE NOT EXISTS (
                     SELECT 1 FROM resources WHERE machine = ?2
                       AND (name = ?3 OR (?4 IS NOT NULL AND device = ?4))
                 )",
                params![
                    ResourceId::new().to_string(),
                    machine.to_string(),
                    resource.name.as_str(),
                    resource.device.map(i64::from),
                    fmt_time(Utc::now()),
                    origin.as_str()
                ],
            )?;
        }
        Ok(())
    }

    /// Register a resource on `machine` by hand
    pub fn register_resource(
        &self,
        machine: MachineId,
        name: ResourceName,
        device: Option<u32>,
    ) -> Result<ResourceRecord, AppError> {
        self.immediate(|| {
            let existing = self.resources_on(machine)?;
            if existing.iter().any(|record| record.resource.name == name) {
                return Err(QueueError::ResourceNameTaken { name }.into());
            }
            if let Some(device) = device
                && let Some(holder) = existing
                    .iter()
                    .find(|record| record.resource.device == Some(device))
            {
                return Err(QueueError::DeviceTaken {
                    device,
                    resource: holder.resource.name.clone(),
                }
                .into());
            }
            let id = ResourceId::new();
            self.conn.execute(
                "INSERT INTO resources (id, machine, name, device, created_at, origin)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.to_string(),
                    machine.to_string(),
                    name.as_str(),
                    device.map(i64::from),
                    fmt_time(Utc::now()),
                    ResourceOrigin::Manual.as_str(),
                ],
            )?;
            self.require_resource(id)
        })
    }

    /// Every resource of `machine`, by name
    pub fn resources_on(&self, machine: MachineId) -> Result<Vec<ResourceRecord>, AppError> {
        let mut statement = self.conn.prepare(&format!(
            "{RESOURCE_SELECT} WHERE machine = ?1 ORDER BY name"
        ))?;
        let raws = statement
            .query_map([machine.to_string()], RawResource::read)?
            .collect::<Result<Vec<_>, _>>()?;
        raws.into_iter().map(RawResource::parse).collect()
    }

    /// One resource by id
    pub fn resource(&self, id: ResourceId) -> Result<Option<ResourceRecord>, AppError> {
        self.conn
            .query_row(
                &format!("{RESOURCE_SELECT} WHERE id = ?1"),
                [id.to_string()],
                RawResource::read,
            )
            .optional()?
            .map(RawResource::parse)
            .transpose()
    }

    pub(super) fn require_resource(&self, id: ResourceId) -> Result<ResourceRecord, AppError> {
        self.resource(id)?.ok_or_else(|| {
            QueueError::ResourceNotFound {
                resource: id.to_string(),
            }
            .into()
        })
    }

    /// Resolve a resource of `machine` by name or id
    pub fn resolve_resource(
        &self,
        machine: MachineId,
        selector: &ResourceSelector,
    ) -> Result<ResourceRecord, AppError> {
        self.resources_on(machine)?
            .into_iter()
            .find(|record| match selector {
                ResourceSelector::Id(id) => record.resource.id == *id,
                ResourceSelector::Name(name) => record.resource.name == *name,
            })
            .ok_or_else(|| {
                QueueError::ResourceNotFound {
                    resource: selector.to_string(),
                }
                .into()
            })
    }
}
