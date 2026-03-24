use mater::model::{Acquisition, SubstanceData, UsageEntry};
use mater::state::compute_state;

#[test]
fn compute_state_pattern_and_event() {
    let data = SubstanceData {
        acquisition: vec![Acquisition {
            acquire_date: Some("2026-01-01".to_string()),
            value_g: 10.0,
            unit: Some("g".to_string()),
            cost: None,
        }],
        usage_log: vec![
            UsageEntry {
                r#type: "pattern".to_string(),
                start_date: Some("2026-01-01".to_string()),
                end_date: Some("2026-01-03".to_string()),
                timestamp: None,
                amount: 1.0,
                amount_unit: Some("g".to_string()),
            },
            UsageEntry {
                r#type: "event".to_string(),
                start_date: None,
                end_date: None,
                timestamp: Some("2026-01-02".to_string()),
                amount: 1.0,
                amount_unit: Some("g".to_string()),
            },
        ],
        ..Default::default()
    };

    let state = compute_state(&data, None);
    assert_eq!(state.total_acquired, 10.0);
    // pattern eats 3g (Jan 1-3) + event 1g = 4g total
    assert_eq!(state.remaining, 6.0);

    let state_on_2026_01_02 = compute_state(
        &data,
        Some(chrono::NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
    );
    // pattern consumed 2 days (2g) + event 1g = 3g
    assert_eq!(state_on_2026_01_02.remaining_at_date, Some(7.0));
}

#[test]
fn compute_state_uses_glass_container_tare() {
    let data = SubstanceData {
        acquisition: vec![Acquisition {
            acquire_date: Some("2026-01-01".to_string()),
            value_g: 2.0,
            unit: Some("g".to_string()),
            cost: None,
        }],
        containers: vec![mater::model::Container {
            id: "glass".to_string(),
            label: Some("glass vial".to_string()),
            tare_weight_g: 3.4,
        }],
        ..Default::default()
    };

    let state = compute_state(&data, None);
    assert_eq!(state.tare, 3.4);
    assert_eq!(state.current_weight, 5.4);
}
