//! Typed Docker container workloads and the witness that their container stopped
//!
//! Homebased never runs a `docker` command line from a submitter. It builds the
//! Docker CLI calls from [`ContainerWorkload`], starts and watches the container
//! itself, and proves that the exact container stopped before it releases a
//! resource. The container runs under `dockerd`, outside the task-run process
//! group, so the container's own state is the witness, not a client process
//!
//! Version 1 targets Docker Engine on a Linux resource authority

pub mod docker;
pub mod lifecycle;
pub mod spec;

pub use spec::{
    ByteSize, ContainerEntrypoint, ContainerMount, ContainerUser, ContainerWorkload, EnvName,
    GpuRequest, ImageReference, check_container_host,
};
