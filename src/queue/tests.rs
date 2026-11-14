use std::time::Duration;

use chrono::{TimeZone, Utc};
use serde_json::json;

use super::{
    HOLD_AFTER, JobId, LevelEnd, MoveFlags, MoveRefusal, Placement, PreemptMode, PreemptStop,
    Preemption, Priority, QueueError, ResourceName, ResourceSelector, RestartWindow, Side,
    StopCause,
};

fn minutes(count: u64) -> Duration {
    Duration::from_secs(count * 60)
}

#[test]
fn restart_window_accepts_only_one_minute_through_a_day() {
    for window in [
        Duration::ZERO,
        Duration::from_secs(59),
        minutes(24 * 60) + Duration::from_secs(1),
    ] {
        assert_eq!(
            RestartWindow::try_from(window),
            Err(QueueError::InvalidRestartWindow { window }),
            "{window:?}"
        );
    }
    for window in [minutes(1), minutes(5), minutes(24 * 60)] {
        assert!(RestartWindow::try_from(window).is_ok(), "{window:?}");
    }
}

#[test]
fn only_a_held_run_past_thirty_minutes_keeps_its_resource() {
    let start = Utc.with_ymd_and_hms(2026, 10, 3, 12, 0, 0).unwrap();
    let at = |seconds: i64| start + chrono::Duration::seconds(seconds);
    let bound = i64::try_from(HOLD_AFTER.as_secs()).unwrap();
    let restart = Preemption::new(PreemptMode::Restart);
    let yields = Preemption::new(PreemptMode::Yield);
    let wait = Preemption::new(PreemptMode::Wait);
    let cases = [
        (restart, at(bound * 10), Some(PreemptStop::Restart)),
        (yields, at(bound * 10), Some(PreemptStop::Yield)),
        // `wait` from an earlier release no longer keeps its resource
        (wait, at(bound * 10), Some(PreemptStop::Restart)),
        (restart.held(), at(bound - 1), Some(PreemptStop::Restart)),
        (yields.held(), at(bound - 1), Some(PreemptStop::Yield)),
        (restart.held(), at(bound), None),
        (yields.held(), at(bound + 1), None),
        // a clock that went backwards counts as age zero
        (restart.held(), at(-10), Some(PreemptStop::Restart)),
    ];
    for (mode, now, expected) in cases {
        assert_eq!(mode.stop_at(start, now), expected, "{mode:?} at {now}");
    }
}

#[test]
fn preempt_forms_parse_and_refuse() {
    let ten = Some(RestartWindow::try_from(minutes(10)).unwrap());
    let accepted = [
        (
            json!({"mode": "restart"}),
            Preemption::new(PreemptMode::Restart),
        ),
        (
            json!({"mode": "restart", "hold": true}),
            Preemption::new(PreemptMode::Restart).held(),
        ),
        (
            json!({"mode": "yield"}),
            Preemption::new(PreemptMode::Yield),
        ),
        (json!({"mode": "wait"}), Preemption::new(PreemptMode::Wait)),
        // an earlier release's window is kept, so the spec digest stays the same
        (
            json!({"mode": "wait", "restart_within": "10m"}),
            Preemption {
                restart_within: ten,
                ..Preemption::new(PreemptMode::Wait)
            },
        ),
    ];
    for (value, expected) in accepted {
        let parsed: Preemption = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(parsed, expected, "{value}");
        assert_eq!(serde_json::to_value(parsed).unwrap(), value, "round trip");
    }
    // `hold: false` is the default, so it is not written back
    let unheld: Preemption =
        serde_json::from_value(json!({"mode": "yield", "hold": false})).unwrap();
    assert_eq!(
        serde_json::to_value(unheld).unwrap(),
        json!({"mode": "yield"})
    );
    let refused = [
        json!({"mode": "restart", "restart_within": "5m"}),
        json!({"mode": "yield", "restart_within": "30s"}),
        json!({"mode": "wait", "restart_within": "25h"}),
        json!({"mode": "yield", "hold": "yes"}),
        json!({"mode": "watch"}),
        json!({"mode": "yield", "extra": true}),
        json!({}),
    ];
    for value in refused {
        assert!(
            serde_json::from_value::<Preemption>(value.clone()).is_err(),
            "{value}"
        );
    }
}

#[test]
fn stop_causes_only_upgrade() {
    use StopCause::{Restart, UserCancel, Yield};
    let cases = [
        (Yield, Restart, Restart),
        (Yield, UserCancel, UserCancel),
        (Restart, Yield, Restart),
        (Restart, UserCancel, UserCancel),
        (UserCancel, Yield, UserCancel),
        (UserCancel, Restart, UserCancel),
        (Yield, Yield, Yield),
    ];
    for (earlier, later, expected) in cases {
        assert_eq!(
            earlier.upgrade(later),
            expected,
            "{earlier:?} then {later:?}"
        );
    }
}

#[test]
fn resource_names_and_selectors() {
    for good in ["gpu0", "gpu12", "a", "rtx-5090"] {
        assert_eq!(ResourceName::parse(good).unwrap().as_str(), good);
    }
    for bad in [
        "",
        "GPU0",
        "gpu 0",
        "-gpu",
        "gpu-",
        "gpu_0",
        &"a".repeat(64),
    ] {
        assert!(ResourceName::parse(bad).is_err(), "{bad:?}");
    }
    let uuid = "01a0ab97-a7aa-7463-a5b0-8d500e40e431";
    assert!(ResourceName::parse(uuid).is_err(), "a UUID is not a name");
    assert!(matches!(
        uuid.parse::<ResourceSelector>().unwrap(),
        ResourceSelector::Id(_)
    ));
    assert!(matches!(
        "gpu1".parse::<ResourceSelector>().unwrap(),
        ResourceSelector::Name(name) if name == ResourceName::gpu(1)
    ));
}

#[test]
fn move_flags_name_one_placement() {
    let target = JobId::new();
    let flags = |front, back, priority, before, after| MoveFlags {
        front,
        back,
        priority,
        before,
        after,
    };
    let high = Some(Priority::High);
    let accepted = [
        (
            flags(true, false, None, None, None),
            Placement::Edge {
                priority: None,
                end: LevelEnd::Front,
            },
        ),
        (
            flags(false, true, None, None, None),
            Placement::Edge {
                priority: None,
                end: LevelEnd::Back,
            },
        ),
        (
            flags(false, false, high, None, None),
            Placement::Edge {
                priority: high,
                end: LevelEnd::Back,
            },
        ),
        (
            flags(true, false, high, None, None),
            Placement::Edge {
                priority: high,
                end: LevelEnd::Front,
            },
        ),
        (
            flags(false, false, None, Some(target), None),
            Placement::Relative {
                target,
                side: Side::Before,
                expect: None,
            },
        ),
        (
            flags(false, false, None, None, Some(target)),
            Placement::Relative {
                target,
                side: Side::After,
                expect: None,
            },
        ),
        (
            flags(false, false, high, Some(target), None),
            Placement::Relative {
                target,
                side: Side::Before,
                expect: high,
            },
        ),
    ];
    for (given, expected) in accepted {
        assert_eq!(Placement::from_flags(given), Ok(expected), "{given:?}");
    }
    let refused = [
        (
            flags(false, false, None, None, None),
            MoveRefusal::NoPlacement,
        ),
        (
            flags(true, true, None, None, None),
            MoveRefusal::ConflictingFlags,
        ),
        (
            flags(false, false, None, Some(target), Some(target)),
            MoveRefusal::ConflictingFlags,
        ),
        (
            flags(true, false, None, Some(target), None),
            MoveRefusal::ConflictingFlags,
        ),
        (
            flags(false, true, None, None, Some(target)),
            MoveRefusal::ConflictingFlags,
        ),
        (
            flags(true, true, high, None, None),
            MoveRefusal::ConflictingFlags,
        ),
    ];
    for (given, expected) in refused {
        assert_eq!(Placement::from_flags(given), Err(expected), "{given:?}");
    }
}

#[test]
fn bugfix_run_number_deserialization_rejects_zero() {
    assert!(super::RunNumber::new(u32::MAX).unwrap().next().is_err());
    assert_eq!(super::RunNumber::FIRST.next().unwrap().get(), 2);
    assert!(serde_json::from_str::<super::RunNumber>("0").is_err());
    assert_eq!(
        serde_json::from_str::<super::RunNumber>("1").unwrap(),
        super::RunNumber::FIRST
    );
    assert_eq!(
        serde_json::to_string(&super::RunNumber::FIRST).unwrap(),
        "1"
    );
}
