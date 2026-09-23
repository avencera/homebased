//! Duplicate machine identity detection.
//!
//! One installation UUID must map to one live daemon. Two fresh probes that
//! report different boot UUIDs for one machine UUID can mean a live clone, but
//! they can also mean one daemon restarted between the two probes. A different
//! name can also mean an ordinary rename across a restart. So a first round
//! only marks a machine as suspect, and a second probe round of the same
//! addresses decides. Cached metadata is never evidence: only answers from the
//! current rounds count.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::fleet::address::MachineAddress;
use crate::fleet::advertisement::MachineHeader;
use crate::machine::{BootId, LocalIdentity, MachineId, MachineName};

/// One successful probe in a round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeObservation {
    /// Address that answered.
    pub address: MachineAddress,
    /// Identity the address reported.
    pub header: MachineHeader,
    /// When the answer arrived.
    pub observed_at: DateTime<Utc>,
}

/// Identity state that the peer directory keeps per machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum IdentityStatus {
    /// One live daemon answers for this machine UUID.
    Consistent,
    /// Distinct live daemons report different boot UUIDs for this machine
    /// UUID. Operations must not route to it until this clears.
    DuplicateMachineIdentity {
        /// Boot UUIDs seen in the confirming round.
        boots: BTreeSet<BootId>,
        /// When the second round confirmed the conflict.
        detected_at: DateTime<Utc>,
    },
}

/// Decision for one suspect machine after the confirming round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityVerdict {
    /// Every first-round responder answered again with one boot UUID. The
    /// earlier difference was a restart or rename.
    Consistent {
        /// Current boot UUID.
        boot: BootId,
        /// Current name.
        name: MachineName,
    },
    /// Distinct live daemons still claim the machine UUID.
    Duplicate {
        /// Boot UUIDs seen in the confirming round.
        boots: BTreeSet<BootId>,
    },
    /// Some first-round responders did not answer again. Keep the previous
    /// status and decide in a later round.
    Inconclusive,
}

/// Machines that need a confirming round.
///
/// A machine is suspect when fresh answers disagree on its boot UUID or name,
/// when another daemon claims the local machine UUID, or when the directory
/// already marks it as a duplicate and the round can clear it.
#[must_use]
pub fn suspects(
    round: &[ProbeObservation],
    local: &LocalIdentity,
    already_duplicate: &BTreeSet<MachineId>,
) -> BTreeSet<MachineId> {
    let mut out = BTreeSet::new();
    for (machine, group) in group_by_machine(round) {
        if machine == local.machine {
            if group.iter().any(|obs| obs.header.boot != local.boot) {
                out.insert(machine);
            }
            continue;
        }
        let boots: BTreeSet<BootId> = group.iter().map(|obs| obs.header.boot).collect();
        let names: BTreeSet<&MachineName> = group.iter().map(|obs| &obs.header.name).collect();
        if boots.len() > 1 || names.len() > 1 || already_duplicate.contains(&machine) {
            out.insert(machine);
        }
    }
    out
}

/// Decide one suspect machine from the first and confirming rounds.
///
/// For the local machine UUID, any other boot in the confirming round is a
/// duplicate: this daemon holds the state-directory lock, so an earlier boot
/// of this installation cannot still answer.
#[must_use]
pub fn confirm(
    machine: MachineId,
    first: &[ProbeObservation],
    second: &[ProbeObservation],
    local: &LocalIdentity,
) -> IdentityVerdict {
    let first_addresses: BTreeSet<&MachineAddress> = first
        .iter()
        .filter(|obs| obs.header.machine == machine)
        .map(|obs| &obs.address)
        .collect();
    let second: Vec<&ProbeObservation> = second
        .iter()
        .filter(|obs| obs.header.machine == machine)
        .collect();
    let mut boots: BTreeSet<BootId> = second.iter().map(|obs| obs.header.boot).collect();
    if machine == local.machine {
        boots.remove(&local.boot);
        if !boots.is_empty() {
            return IdentityVerdict::Duplicate { boots };
        }
        return IdentityVerdict::Inconclusive;
    }
    if boots.len() > 1 {
        return IdentityVerdict::Duplicate { boots };
    }
    let second_addresses: BTreeSet<&MachineAddress> =
        second.iter().map(|obs| &obs.address).collect();
    let Some(latest) = second.iter().max_by_key(|obs| obs.observed_at) else {
        return IdentityVerdict::Inconclusive;
    };
    if !first_addresses.is_subset(&second_addresses) {
        return IdentityVerdict::Inconclusive;
    }
    IdentityVerdict::Consistent {
        boot: latest.header.boot,
        name: latest.header.name.clone(),
    }
}

/// Addresses to probe again for the confirming round.
#[must_use]
pub fn recheck_addresses(
    round: &[ProbeObservation],
    suspects: &BTreeSet<MachineId>,
) -> BTreeSet<MachineAddress> {
    round
        .iter()
        .filter(|obs| suspects.contains(&obs.header.machine))
        .map(|obs| obs.address.clone())
        .collect()
}

fn group_by_machine(round: &[ProbeObservation]) -> BTreeMap<MachineId, Vec<&ProbeObservation>> {
    let mut groups: BTreeMap<MachineId, Vec<&ProbeObservation>> = BTreeMap::new();
    for obs in round {
        groups.entry(obs.header.machine).or_default().push(obs);
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet::protocol::SUPPORTED_PROTOCOLS;

    fn obs(address: &str, machine: MachineId, boot: BootId, name: &str) -> ProbeObservation {
        ProbeObservation {
            address: address.parse().unwrap(),
            header: MachineHeader {
                machine,
                boot,
                name: MachineName::parse(name).unwrap(),
                version: "0.3.0".into(),
                protocol: SUPPORTED_PROTOCOLS,
            },
            observed_at: Utc::now(),
        }
    }

    fn local() -> LocalIdentity {
        LocalIdentity {
            machine: MachineId::new(),
            boot: BootId::new(),
        }
    }

    #[test]
    fn restart_between_probes_is_not_a_duplicate() {
        let local = local();
        let machine = MachineId::new();
        let (old, new) = (BootId::new(), BootId::new());
        // lan address answered before the restart, tailscale address after it
        let first = vec![
            obs("http://10.0.0.2:7677", machine, old, "code"),
            obs("http://100.64.0.2:7677", machine, new, "code"),
        ];
        let suspect = suspects(&first, &local, &BTreeSet::new());
        assert_eq!(suspect, BTreeSet::from([machine]));
        let second = vec![
            obs("http://10.0.0.2:7677", machine, new, "code"),
            obs("http://100.64.0.2:7677", machine, new, "code"),
        ];
        assert_eq!(
            confirm(machine, &first, &second, &local),
            IdentityVerdict::Consistent {
                boot: new,
                name: MachineName::parse("code").unwrap()
            }
        );
    }

    #[test]
    fn rename_across_restart_is_not_a_duplicate() {
        let local = local();
        let machine = MachineId::new();
        let (old, new) = (BootId::new(), BootId::new());
        let first = vec![
            obs("http://10.0.0.2:7677", machine, old, "code"),
            obs("http://100.64.0.2:7677", machine, new, "builder"),
        ];
        let second = vec![
            obs("http://10.0.0.2:7677", machine, new, "builder"),
            obs("http://100.64.0.2:7677", machine, new, "builder"),
        ];
        assert!(matches!(
            confirm(machine, &first, &second, &local),
            IdentityVerdict::Consistent { boot, .. } if boot == new
        ));
    }

    #[test]
    fn one_answer_per_round_is_never_suspect() {
        let local = local();
        let machine = MachineId::new();
        // a restart seen by sequential rounds against one address
        let first = vec![obs("http://10.0.0.2:7677", machine, BootId::new(), "code")];
        assert!(suspects(&first, &local, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn live_clones_are_duplicates() {
        let local = local();
        let machine = MachineId::new();
        let (a, b) = (BootId::new(), BootId::new());
        let round = vec![
            obs("http://10.0.0.2:7677", machine, a, "code"),
            obs("http://10.0.0.3:7677", machine, b, "code"),
        ];
        assert_eq!(
            suspects(&round, &local, &BTreeSet::new()),
            BTreeSet::from([machine])
        );
        assert_eq!(
            confirm(machine, &round, &round, &local),
            IdentityVerdict::Duplicate {
                boots: BTreeSet::from([a, b])
            }
        );
    }

    #[test]
    fn partial_confirming_round_is_inconclusive() {
        let local = local();
        let machine = MachineId::new();
        let (a, b) = (BootId::new(), BootId::new());
        let first = vec![
            obs("http://10.0.0.2:7677", machine, a, "code"),
            obs("http://10.0.0.3:7677", machine, b, "code"),
        ];
        let second = vec![obs("http://10.0.0.2:7677", machine, a, "code")];
        assert_eq!(
            confirm(machine, &first, &second, &local),
            IdentityVerdict::Inconclusive
        );
    }

    #[test]
    fn second_daemon_claiming_local_identity_is_a_duplicate() {
        let local = local();
        let intruder = BootId::new();
        let round = vec![
            obs("http://10.0.0.1:7677", local.machine, local.boot, "main"),
            obs("http://10.0.0.9:7677", local.machine, intruder, "main"),
        ];
        assert_eq!(
            suspects(&round, &local, &BTreeSet::new()),
            BTreeSet::from([local.machine])
        );
        assert_eq!(
            confirm(local.machine, &round, &round, &local),
            IdentityVerdict::Duplicate {
                boots: BTreeSet::from([intruder])
            }
        );
    }

    #[test]
    fn own_advertisement_is_not_suspect() {
        let local = local();
        let round = vec![obs(
            "http://10.0.0.1:7677",
            local.machine,
            local.boot,
            "main",
        )];
        assert!(suspects(&round, &local, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn known_duplicate_is_rechecked_and_can_clear() {
        let local = local();
        let machine = MachineId::new();
        let boot = BootId::new();
        let round = vec![obs("http://10.0.0.2:7677", machine, boot, "code")];
        let known = BTreeSet::from([machine]);
        assert_eq!(suspects(&round, &local, &known), known);
        assert!(matches!(
            confirm(machine, &round, &round, &local),
            IdentityVerdict::Consistent { .. }
        ));
    }
}
