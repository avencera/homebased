//! Queue resources: GPU detection, manual registration, and lookup

use chrono::Utc;
use rusqlite::{OptionalExtension, params};

use super::rows::{RESOURCE_SELECT, RawResource};
use super::{ResourceOrigin, ResourceRecord};
use crate::error::AppError;
use crate::machine::MachineId;
use crate::queue::gpu::DetectedResource;
use crate::queue::{QueueError, ResourceId, ResourceLabel, ResourceName, ResourceSelector};
use crate::store::{Store, fmt_time};

impl Store {
    /// Ensure one resource per detected GPU on `machine`
    ///
    /// Idempotent: an existing resource keeps its UUID, and a detected GPU
    /// whose name is taken by a hand-registered resource is left alone.
    /// An idle fallback adopts its first device's name unless that name is taken
    /// Detected resources are never removed, so a GPU that disappears leaves
    /// its resource in place
    ///
    /// Every resource of `machine` with a device index, hand-registered ones
    /// included, then takes the UUID that detection listed at that index, or
    /// none when the index is not listed. An empty `detected`, from a probe
    /// that failed, keeps the UUIDs that were saved
    pub fn ensure_detected_resources(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<Vec<ResourceRecord>, AppError> {
        self.immediate(|| {
            self.reconcile_detected(machine, detected)?;
            if !detected.is_empty() {
                self.save_detected_gpus(machine, detected)?;
            }
            self.resources_on(machine)
        })
    }

    fn reconcile_detected(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<(), AppError> {
        let existing = self.resources_on(machine)?;
        let first_device = detected.iter().find_map(|resource| resource.device);
        let Some(device) = first_device else {
            // a fallback is never added beside an indexed resource
            if existing
                .iter()
                .all(|record| record.resource.device.is_none())
            {
                self.insert_detected(machine, detected)?;
            }
            return Ok(());
        };

        let fallback = existing
            .iter()
            .find(|record| record.origin == ResourceOrigin::DetectedFallback);
        if let Some(fallback) = fallback
            && !self.adopt_fallback_device(machine, &existing, fallback, device)?
        {
            return Ok(());
        }
        self.insert_detected(machine, detected)
    }

    /// Replace the machine's detected GPU list and give each indexed resource
    /// the UUID listed at its device index
    fn save_detected_gpus(
        &self,
        machine: MachineId,
        detected: &[DetectedResource],
    ) -> Result<(), AppError> {
        self.conn.execute(
            "DELETE FROM detected_gpus WHERE machine = ?1",
            [machine.to_string()],
        )?;
        for resource in detected {
            if let (Some(device), Some(uuid)) = (resource.device, &resource.uuid) {
                self.conn.execute(
                    "INSERT INTO detected_gpus (machine, device, uuid) VALUES (?1, ?2, ?3)",
                    params![machine.to_string(), i64::from(device), uuid.as_str()],
                )?;
            }
        }
        self.conn.execute(
            "UPDATE resources SET gpu_uuid = (
                 SELECT uuid FROM detected_gpus g
                 WHERE g.machine = resources.machine AND g.device = resources.device
             )
             WHERE machine = ?1",
            [machine.to_string()],
        )?;
        Ok(())
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
            // the last detection that listed devices names the GPU at this index
            self.conn.execute(
                "INSERT INTO resources (id, machine, name, device, created_at, origin, gpu_uuid)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, (
                     SELECT uuid FROM detected_gpus WHERE machine = ?2 AND device = ?4
                 ))",
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

    /// Set or clear the display label of a resource of `machine`
    ///
    /// Setting the label it already has is a no-op, so a retried request is safe
    pub fn set_resource_label(
        &self,
        machine: MachineId,
        selector: &ResourceSelector,
        label: Option<&ResourceLabel>,
    ) -> Result<ResourceRecord, AppError> {
        self.immediate(|| {
            let id = self.resolve_resource(machine, selector)?.resource.id;
            self.conn.execute(
                "UPDATE resources SET label = ?2 WHERE id = ?1",
                params![id.to_string(), label.map(ResourceLabel::as_str)],
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
