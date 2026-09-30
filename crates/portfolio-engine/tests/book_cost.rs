//! One book-cost rule: the fold's account total and the valuation's cost
//! basis are the same figure, and a lot converts by its stored rate, else by
//! its acquisition date's rate with minor units applied.

mod support;

use rust_decimal::Decimal;
use support::*;
use wealthfolio_portfolio_engine::model::*;

/// On every keyframe day, the keyframe's cost basis is the valuation's.
#[test]
fn p_book_keyframes_and_valuations_agree_on_book_cost() {
    let mut compared = 0;
    let mut failures = Vec::new();
    for scenario in load_all_scenarios()
        .into_iter()
        .filter(|s| !s.markers.iter().any(|m| m == "S") && scenario_selected(&s.id))
    {
        let Ok(pipeline) = Pipeline::run(scenario.raw_facts()) else {
            continue;
        };
        for (account, frames) in &pipeline.bundle.keyframes {
            let Some(series) = pipeline.series.get(account) else {
                continue;
            };
            for frame in frames {
                let Some(day) = series.days.iter().find(|d| d.date == frame.date) else {
                    continue;
                };
                compared += 1;
                if day.cost_basis != frame.state.cost_basis {
                    failures.push(format!(
                        "{} {account} {}: keyframe {} vs valuation {}",
                        scenario.id, frame.date, frame.state.cost_basis, day.cost_basis
                    ));
                }
            }
        }
    }
    assert!(compared > 0, "no keyframe compared");
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A pence-quoted position moved into a USD account keeps its lot's stored
/// rates to GBP (its account) and EUR (the base), neither of which is USD:
/// the lot converts at its acquisition date's rate, pence to pounds first.
#[test]
fn a_minor_unit_lot_without_a_stored_rate_converts_through_its_major_unit() {
    let scenario: Scenario = serde_yaml::from_str(
        r#"
id: BOOK-GBX-01
policy: { base_currency: EUR, timezone: UTC, as_of: 2025-01-08 }
accounts:
  - { id: acc-gbp, currency: GBP }
  - { id: acc-usd, currency: USD }
assets:
  - { id: vod, quote_ccy: GBp }
activities:
  - { id: dep-1, account: acc-gbp, type: DEPOSIT, date: 2025-01-02T10:00:00Z, amount: 1000 }
  - { id: buy-1, account: acc-gbp, type: BUY, date: 2025-01-03T10:00:00Z, asset: vod, quantity: 100, unit_price: 2.50, amount: 250 }
  - { id: out-1, account: acc-gbp, type: TRANSFER_OUT, date: 2025-01-06T10:00:00Z, asset: vod, quantity: 100, unit_price: 2.50, source_group_id: g1 }
  - { id: in-1, account: acc-usd, type: TRANSFER_IN, date: 2025-01-06T11:00:00Z, asset: vod, quantity: 100, unit_price: 2.50, source_group_id: g1 }
quotes:
  - { asset: vod, day: 2025-01-03, close: 250 }
  - { asset: vod, day: 2025-01-06, close: 260 }
fx_rates:
  - { from: GBP, to: USD, day: 2025-01-02, rate: 1.25 }
  - { from: GBP, to: EUR, day: 2025-01-02, rate: 1.2 }
"#,
    )
    .expect("scenario");
    let pipeline = Pipeline::run(scenario.raw_facts()).expect("pipeline");
    let frame = pipeline.bundle.keyframes[&AccountId::new("acc-usd")]
        .last()
        .expect("keyframe");
    let position = &frame.state.positions[&AssetId::new("vod")];
    // 100 x 250 pence = 25000 pence = 250 GBP = 312.5 USD.
    assert_eq!(position.total_cost_basis, Decimal::from(25000));
    assert_eq!(position.cost_basis_account, Some(Decimal::new(3125, 1)));
    assert_eq!(frame.state.cost_basis, Decimal::new(3125, 1));
}
