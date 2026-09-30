//! Coordinator over the in-memory doubles: marker-driven refolds and
//! revalues, windowed persistence, and parity with the kernel goldens.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use super::*;
use crate::accounts::{AccountAccountingSettings, AccountRepositoryTrait, CostBasisMethod};
use crate::activities::{Activity, ActivityRepositoryTrait};
use crate::assets::AssetRepositoryTrait;
use crate::fx::{FxRepositoryTrait, FxService};
use crate::lots::LotRepositoryTrait;
use crate::portfolio::projection::GENESIS;
use crate::portfolio::snapshot::{
    AccountStateSnapshot, SnapshotRepositoryTrait, SnapshotService, SnapshotSource,
};
use crate::portfolio::valuation::ValuationRepositoryTrait;
use crate::quotes::{Quote, QuoteServiceTrait};
use crate::test_support::in_memory::*;
use crate::test_support::scenario::{as_of_instant, load_all_scenarios, Scenario, ScenarioFacts};

struct Harness {
    coordinator: PortfolioCoordinator,
    account_repo: Arc<InMemoryAccountRepository>,
    activity_repo: Arc<InMemoryActivityRepository>,
    quote_service: Arc<InMemoryQuoteService>,
    fx_repo: Arc<InMemoryFxRepository>,
    valuation_repo: Arc<dyn ValuationRepositoryTrait>,
    snapshot_repo: Arc<dyn SnapshotRepositoryTrait>,
    lot_repo: Arc<dyn LotRepositoryTrait>,
    projections: Arc<dyn ProjectionStoreTrait>,
    store: Arc<InMemoryProjectionStore>,
    _clock: crate::utils::clock::FrozenClock,
}

fn scenario(id: &str) -> Scenario {
    load_all_scenarios()
        .into_iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("scenario {id} not found"))
}

/// Short fixtures: two-day windows exercise every window boundary.
async fn harness(facts: ScenarioFacts) -> Harness {
    harness_with(facts, WindowCadence::Days(2)).await
}

async fn harness_with(facts: ScenarioFacts, cadence: WindowCadence) -> Harness {
    let clock = crate::utils::clock::freeze(as_of_instant(facts.as_of, &facts.timezone));
    let base_currency = Arc::new(RwLock::new(facts.base_currency.clone()));
    let timezone = Arc::new(RwLock::new(facts.timezone.clone()));
    let archived: HashSet<String> = facts
        .accounts
        .iter()
        .filter(|a| a.is_archived)
        .map(|a| a.id.clone())
        .collect();
    let activity_ids: HashSet<String> = facts.activities.iter().map(|a| a.id.clone()).collect();
    let asset_ids: HashSet<String> = facts.assets.iter().map(|a| a.id.clone()).collect();

    let account_repo = Arc::new(InMemoryAccountRepository::new(facts.accounts.clone()));
    let account_repo_dyn: Arc<dyn AccountRepositoryTrait> = account_repo.clone();
    let asset_repo: Arc<dyn AssetRepositoryTrait> =
        Arc::new(InMemoryAssetRepository::new(facts.assets.clone()));
    let activity_repo = Arc::new(InMemoryActivityRepository::new(
        facts.activities.clone(),
        archived.clone(),
    ));
    let activity_repo_dyn: Arc<dyn ActivityRepositoryTrait> = activity_repo.clone();
    let snapshot_repo: Arc<dyn SnapshotRepositoryTrait> =
        Arc::new(InMemorySnapshotRepository::new(archived));
    let valuation_repo: Arc<dyn ValuationRepositoryTrait> =
        Arc::new(InMemoryValuationRepository::default());
    let lot_repo: Arc<dyn LotRepositoryTrait> =
        Arc::new(InMemoryLotRepository::new(activity_ids, asset_ids));
    let quote_service = Arc::new(InMemoryQuoteService::new(
        facts.quotes.clone(),
        facts.assets.clone(),
    ));
    let quote_service_dyn: Arc<dyn QuoteServiceTrait> = quote_service.clone();
    let fx_repo = Arc::new(InMemoryFxRepository::new(facts.fx_rates.clone()));
    let fx_repo_dyn: Arc<dyn FxRepositoryTrait> = fx_repo.clone();
    let fx_service = Arc::new(FxService::new(fx_repo.clone()));
    fx_service.initialize().expect("fx converter initializes");
    let snapshot_service = Arc::new(SnapshotService::new(
        timezone.clone(),
        account_repo_dyn.clone(),
        snapshot_repo.clone(),
    ));
    let store = Arc::new(InMemoryProjectionStore::new(
        snapshot_repo.clone(),
        lot_repo.clone(),
        valuation_repo.clone(),
    ));
    let projections: Arc<dyn ProjectionStoreTrait> = store.clone();
    let sources = FactSources {
        accounts: account_repo_dyn,
        activities: activity_repo_dyn,
        assets: asset_repo,
        quotes: quote_service_dyn,
        fx_rates: fx_repo_dyn,
        snapshots: snapshot_repo.clone(),
        projections: projections.clone(),
    };
    let coordinator = PortfolioCoordinator::new(CoordinatorDeps {
        base_currency,
        timezone,
        sources,
        fx_service,
        snapshot_service,
        projections: projections.clone(),
        lots: lot_repo.clone(),
        window_cadence: cadence,
    });
    snapshot_repo
        .save_snapshots(&facts.observed_snapshots)
        .await
        .expect("observed snapshots seeded");
    Harness {
        coordinator,
        account_repo,
        activity_repo,
        quote_service,
        fx_repo,
        valuation_repo,
        snapshot_repo,
        lot_repo,
        projections,
        store,
        _clock: clock,
    }
}

impl Harness {
    /// Applies activity changes to the doubles and records what the SQLite
    /// triggers would: each changed row's account from the day before its
    /// UTC date, its transfer partners from theirs, and a split's asset.
    /// `before` holds the rows as they were (updates and deletions mark their
    /// old date too).
    fn change_activities(
        &self,
        added: Vec<Activity>,
        updated: Vec<Activity>,
        removed: &[String],
        before: &[Activity],
    ) {
        let mut changed: Vec<Activity> = added.iter().chain(&updated).cloned().collect();
        changed.extend(
            before
                .iter()
                .filter(|a| removed.contains(&a.id) || updated.iter().any(|u| u.id == a.id))
                .cloned(),
        );
        self.activity_repo.apply(added, updated, removed);
        let mut all = before.to_vec();
        all.extend(changed.iter().cloned());
        for activity in &changed {
            self.mark_activity(activity);
            if let (Some(asset), "SPLIT") = (&activity.asset_id, activity.effective_type()) {
                self.store.mark(MarkerScope::Asset(asset.clone()), GENESIS);
            }
            if let Some(group) = &activity.source_group_id {
                for partner in all.iter().filter(|a| {
                    a.source_group_id.as_ref() == Some(group) && a.account_id != activity.account_id
                }) {
                    self.mark_activity(partner);
                }
            }
        }
    }

    fn mark_activity(&self, activity: &Activity) {
        let day = activity.activity_date.date_naive().pred_opt().unwrap();
        self.store
            .mark(MarkerScope::Account(activity.account_id.clone()), day);
    }

    fn add_quotes(&self, quotes: Vec<Quote>) {
        for quote in &quotes {
            self.store.mark(
                MarkerScope::Prices(quote.asset_id.clone()),
                quote.timestamp.date_naive(),
            );
        }
        self.quote_service.add_quotes(quotes);
    }

    fn rows(&self, account: &str) -> Vec<crate::portfolio::valuation::DailyAccountValuation> {
        self.valuation_repo
            .get_historical_valuations(account, None, None)
            .unwrap()
    }
}

fn request() -> PortfolioJobRequest {
    PortfolioJobRequest {
        account_ids: None,
        market_sync: MarketSyncMode::None,
        ..PortfolioJobRequest::default()
    }
}

fn plan_of(report: &PortfolioJobReport, account: &str) -> Option<RebuildPlan> {
    report
        .plans
        .iter()
        .find(|p| p.account_id == account)
        .map(|p| p.plan)
}

#[tokio::test]
async fn a_run_persists_rows_and_consumes_its_markers() {
    let scenario = scenario("NOM-TRADE-01");
    let harness = harness(scenario.facts()).await;
    assert!(!harness.projections.pending_markers().unwrap().is_empty());
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);

    let account = &report.account_ids[0];
    assert_eq!(
        plan_of(&report, account),
        Some(RebuildPlan::Refold { from: GENESIS })
    );
    let valuations = harness.rows(account);
    assert!(!valuations.is_empty(), "valuation rows persisted");
    assert!(valuations.iter().all(|v| v.account_id == *account));
    assert!(!harness
        .snapshot_repo
        .get_snapshots_by_account(account, None, None)
        .unwrap()
        .is_empty());
    assert!(!harness
        .lot_repo
        .get_all_lots_for_account(account)
        .await
        .unwrap()
        .is_empty());
    assert!(harness.projections.pending_markers().unwrap().is_empty());
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());

    // Nothing stale: nothing planned.
    let again = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(again.plans.is_empty(), "{:?}", again.plans);
}

/// The kernel golden of a scenario (the insta header stripped).
fn kernel_golden(id: &str) -> Option<serde_yaml::Value> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../portfolio-engine/tests/fixtures/goldens/kernel")
        .join(format!("{id}.snap"));
    let text = std::fs::read_to_string(path).ok()?;
    let body = text.splitn(3, "---\n").nth(2)?;
    serde_yaml::from_str(body).ok()
}

fn golden_str(value: &serde_yaml::Value, key: &str) -> String {
    match value.get(key) {
        Some(serde_yaml::Value::String(s)) => s.clone(),
        Some(other) => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_string(),
        None => String::new(),
    }
}

fn golden_decimal(value: &serde_yaml::Value, key: &str) -> rust_decimal::Decimal {
    golden_str(value, key).parse().unwrap_or_default()
}

fn same_status(row: &str, golden: &str) -> bool {
    row.to_ascii_lowercase().replace('_', "") == golden.to_ascii_lowercase().replace('_', "")
}

/// Every parity scenario through the real fact loading, row mapping and
/// persistence, one two-day window at a time and in yearly windows: the
/// stored valuations, lots and keyframes must equal the kernel golden
/// (architecture §3.3). Nothing here asserts mere non-emptiness.
#[tokio::test]
async fn every_parity_scenario_persists_the_kernel_golden() {
    for cadence in [WindowCadence::Days(2), WindowCadence::Year] {
        assert_parity(cadence).await;
    }
}

async fn assert_parity(cadence: WindowCadence) {
    let mut compared = 0;
    for scenario in load_all_scenarios()
        .into_iter()
        .filter(|s| s.is_parity_eligible())
    {
        let Some(golden) = kernel_golden(&scenario.id) else {
            panic!("{}: no kernel golden", scenario.id);
        };
        let facts = scenario.facts();
        let harness = harness_with(facts.clone(), cadence).await;
        let report = harness
            .coordinator
            .run_job(request(), &SilentObserver)
            .await
            .unwrap();
        assert!(
            report.failures.is_empty(),
            "{}: {:?}",
            scenario.id,
            report.failures
        );
        assert!(
            harness.coordinator.stale_accounts().unwrap().is_empty(),
            "{}: stale after rebuild",
            scenario.id
        );
        let accounts = golden["baseline"]["accounts"]
            .as_mapping()
            .expect("golden accounts");
        for (account_id, expected) in accounts {
            let account_id = account_id.as_str().unwrap();
            let Some(account) = facts.accounts.iter().find(|a| a.id == account_id) else {
                continue;
            };
            if account.is_archived {
                continue;
            }
            let label = format!("{}: {account_id}", scenario.id);
            let rows = harness
                .valuation_repo
                .get_historical_valuations(account_id, None, None)
                .unwrap();
            let expected_rows = expected["valuations"]
                .as_sequence()
                .cloned()
                .unwrap_or_default();
            assert_eq!(
                rows.len(),
                expected_rows.len(),
                "{label}: valuation row count"
            );
            for (row, want) in rows.iter().zip(&expected_rows) {
                assert_eq!(
                    row.valuation_date.to_string(),
                    golden_str(want, "date"),
                    "{label}"
                );
                let day = format!("{label} {}", row.valuation_date);
                for (name, actual, key) in [
                    ("total_value_base", row.total_value_base, "total_value_base"),
                    (
                        "cash_balance_base",
                        row.cash_balance_base,
                        "cash_balance_base",
                    ),
                    ("cost_basis_base", row.cost_basis_base, "cost_basis_base"),
                    (
                        "net_contribution_base",
                        row.net_contribution_base,
                        "net_contribution_base",
                    ),
                    (
                        "external_inflow_base",
                        row.external_inflow_base,
                        "external_inflow_base",
                    ),
                    (
                        "external_outflow_base",
                        row.external_outflow_base,
                        "external_outflow_base",
                    ),
                ] {
                    assert_eq!(
                        actual.round_dp(8).normalize(),
                        golden_decimal(want, key).normalize(),
                        "{day}: {name}"
                    );
                }
                assert!(
                    same_status(row.value_status.as_str(), &golden_str(want, "value_status")),
                    "{day}: value_status {:?} != {}",
                    row.value_status,
                    golden_str(want, "value_status")
                );
            }
            if account.tracking_mode == crate::accounts::TrackingMode::Holdings {
                continue;
            }
            let lots = harness
                .lot_repo
                .get_all_lots_for_account(account_id)
                .await
                .unwrap();
            let expected_lots = expected["lots"].as_sequence().cloned().unwrap_or_default();
            assert_eq!(lots.len(), expected_lots.len(), "{label}: lot count");
            for want in &expected_lots {
                let id = golden_str(want, "id");
                let lot = lots
                    .iter()
                    .find(|l| l.id == id)
                    .unwrap_or_else(|| panic!("{label}: lot {id} not persisted"));
                // Goldens print decimals at 8 places; rows keep full precision.
                let stored = |raw: &str| {
                    raw.parse::<rust_decimal::Decimal>()
                        .unwrap()
                        .round_dp(8)
                        .normalize()
                };
                assert_eq!(
                    stored(&lot.remaining_quantity),
                    golden_decimal(want, "remaining_quantity").normalize(),
                    "{label}: lot {id} remaining quantity"
                );
                assert_eq!(
                    stored(&lot.remaining_cost_basis),
                    golden_decimal(want, "remaining_cost_basis").normalize(),
                    "{label}: lot {id} remaining cost basis"
                );
            }
            let keyframes = harness
                .snapshot_repo
                .get_snapshots_by_account(account_id, None, None)
                .unwrap();
            let expected_keyframes = expected["keyframes"]
                .as_sequence()
                .map(|k| k.len())
                .unwrap_or(0);
            assert_eq!(
                keyframes.len(),
                expected_keyframes,
                "{label}: keyframe count"
            );
            let disposals = harness
                .lot_repo
                .get_lot_disposals_for_account(account_id)
                .await
                .unwrap();
            let expected_disposals = expected["disposals"]
                .as_sequence()
                .cloned()
                .unwrap_or_default();
            assert_eq!(
                disposals.len(),
                expected_disposals.len(),
                "{label}: disposal count"
            );
            for want in &expected_disposals {
                let activity = golden_str(want, "activity_id");
                let realized = golden_decimal(want, "realized_pnl_base").normalize();
                assert!(
                    disposals.iter().any(|d| {
                        activity.starts_with(&d.disposal_activity_id)
                            && d.realized_pnl_base
                                .parse::<rust_decimal::Decimal>()
                                .is_ok_and(|v| v.round_dp(8).normalize() == realized)
                    }),
                    "{label}: disposal of {activity} (realized {realized}) not persisted"
                );
            }
        }
        compared += 1;
    }
    assert!(compared >= 81, "only {compared} scenarios compared");
}

fn normalized_valuations(
    mut rows: Vec<crate::portfolio::valuation::DailyAccountValuation>,
) -> Vec<crate::portfolio::valuation::DailyAccountValuation> {
    for row in &mut rows {
        row.calculated_at = chrono::DateTime::<chrono::Utc>::MIN_UTC;
    }
    rows.sort_by(|a, b| {
        (a.account_id.clone(), a.valuation_date).cmp(&(b.account_id.clone(), b.valuation_date))
    });
    rows
}

fn normalized_lots(mut rows: Vec<crate::lots::LotRecord>) -> Vec<crate::lots::LotRecord> {
    for row in &mut rows {
        row.created_at.clear();
        row.updated_at.clear();
    }
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    rows
}

/// LIFE fixtures: after every lifecycle step (appends, backdated edits,
/// deletions, quote backfills, a new day) the incrementally maintained
/// projection must equal a fresh run over the same facts.
#[tokio::test]
async fn lifecycle_steps_match_a_fresh_rebuild() {
    let mut steps_checked = 0;
    let mut plans_seen: HashSet<&str> = HashSet::new();
    for scenario in load_all_scenarios()
        .into_iter()
        .filter(|s| !s.lifecycle.is_empty())
    {
        let baseline = scenario.facts();
        let live = harness(baseline.clone()).await;
        live.coordinator
            .run_job(request(), &SilentObserver)
            .await
            .unwrap();
        for (index, step) in scenario.lifecycle.iter().enumerate() {
            let label = format!("{} step {} ({})", scenario.id, index + 1, step.label);
            let before = scenario.facts_after(index);
            let after = scenario.facts_after(index + 1);
            crate::utils::clock::set_frozen(as_of_instant(after.as_of, &after.timezone));
            let by_id = |ids: &[String]| -> Vec<Activity> {
                after
                    .activities
                    .iter()
                    .filter(|a| ids.contains(&a.id))
                    .cloned()
                    .collect()
            };
            let added: Vec<String> = step.add_activities.iter().map(|a| a.id.clone()).collect();
            let updated: Vec<String> = step
                .update_activities
                .iter()
                .map(|a| a.id.clone())
                .collect();
            live.change_activities(
                by_id(&added),
                by_id(&updated),
                &step.remove_activities,
                &before.activities,
            );
            live.add_quotes(
                after
                    .quotes
                    .iter()
                    .filter(|q| {
                        step.add_quotes.iter().any(|spec| {
                            spec.asset == q.asset_id && spec.day == q.timestamp.date_naive()
                        })
                    })
                    .cloned()
                    .collect(),
            );
            for spec in &step.add_fx_rates {
                let rate = crate::test_support::scenario::fx_rate_from_spec(spec);
                live.store.mark(MarkerScope::Fx(rate.id), spec.day);
            }
            live.fx_repo.add_rates(
                step.add_fx_rates
                    .iter()
                    .map(crate::test_support::scenario::fx_rate_from_spec)
                    .collect(),
            );

            let report = live
                .coordinator
                .ensure_consistent(MarketSyncMode::None, &SilentObserver)
                .await
                .unwrap();
            assert!(report.failures.is_empty(), "{label}: {:?}", report.failures);
            for plan in &report.plans {
                plans_seen.insert(match plan.plan {
                    RebuildPlan::Refold { from } if from == GENESIS => "full",
                    RebuildPlan::Refold { .. } => "refold",
                    RebuildPlan::Revalue { .. } => "revalue",
                });
            }
            assert!(
                live.coordinator.stale_accounts().unwrap().is_empty(),
                "{label}: stale after the run"
            );

            let fresh = harness(after.clone()).await;
            fresh
                .coordinator
                .run_job(request(), &SilentObserver)
                .await
                .unwrap();
            for account in after.accounts.iter().filter(|a| !a.is_archived) {
                assert_eq!(
                    normalized_valuations(live.rows(&account.id)),
                    normalized_valuations(fresh.rows(&account.id)),
                    "{label}: valuations of {}",
                    account.id
                );
                let incremental = live
                    .lot_repo
                    .get_all_lots_for_account(&account.id)
                    .await
                    .unwrap();
                let rebuilt = fresh
                    .lot_repo
                    .get_all_lots_for_account(&account.id)
                    .await
                    .unwrap();
                assert_eq!(
                    format!("{:#?}", normalized_lots(incremental)),
                    format!("{:#?}", normalized_lots(rebuilt)),
                    "{label}: lots of {}",
                    account.id
                );
            }
            steps_checked += 1;
        }
    }
    assert!(steps_checked > 0, "no lifecycle steps found");
    // The equivalence above only proves the incremental paths if they ran.
    for path in ["refold", "revalue"] {
        assert!(
            plans_seen.contains(path),
            "no lifecycle step took the {path} path"
        );
    }
}

/// Rows dated before `day` are the first run's rows (same stamp); rows from
/// `day` were rewritten by the later run.
fn untouched_before(
    before: &[crate::portfolio::valuation::DailyAccountValuation],
    after: &[crate::portfolio::valuation::DailyAccountValuation],
    day: chrono::NaiveDate,
) {
    for row in after {
        let first = before
            .iter()
            .find(|b| b.valuation_date == row.valuation_date);
        let stamp = first.map(|b| b.calculated_at);
        if row.valuation_date < day {
            assert_eq!(
                Some(row.calculated_at),
                stamp,
                "{} rewritten",
                row.valuation_date
            );
        } else {
            assert_ne!(
                Some(row.calculated_at),
                stamp,
                "{} not rewritten",
                row.valuation_date
            );
        }
    }
}

#[tokio::test]
async fn a_price_change_revalues_from_its_day_only() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let live = harness(facts.clone()).await;
    live.coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    let first_run = crate::utils::clock::now();
    let rows_before = live.rows(&account);
    let lots_before = format!(
        "{:#?}",
        live.lot_repo
            .get_all_lots_for_account(&account)
            .await
            .unwrap()
    );

    let template = facts.quotes[0].clone();
    let late = Quote {
        id: "late-quote".to_string(),
        timestamp: template.timestamp + chrono::Duration::days(1),
        close: template.close * rust_decimal_macros::dec!(1.1),
        ..template
    };
    let changed_day = late.timestamp.date_naive();
    crate::utils::clock::set_frozen(first_run + chrono::Duration::hours(1));
    live.add_quotes(vec![late.clone()]);
    let report = live
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert_eq!(
        plan_of(&report, &account),
        Some(RebuildPlan::Revalue { from: changed_day })
    );
    untouched_before(&rows_before, &live.rows(&account), changed_day);
    assert_eq!(
        lots_before,
        format!(
            "{:#?}",
            live.lot_repo
                .get_all_lots_for_account(&account)
                .await
                .unwrap()
        ),
        "a revalue never rewrites lots"
    );

    let mut fresh_facts = facts.clone();
    fresh_facts.quotes.push(late);
    let fresh = harness(fresh_facts).await;
    fresh
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert_eq!(
        normalized_valuations(live.rows(&account)),
        normalized_valuations(fresh.rows(&account))
    );
}

#[tokio::test]
async fn a_new_day_revalues_only_the_new_day() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let harness = harness(facts.clone()).await;
    harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    let rows_before = harness.rows(&account);
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());

    let tomorrow = facts.as_of + chrono::Duration::days(1);
    crate::utils::clock::set_frozen(as_of_instant(tomorrow, &facts.timezone));
    let stale = harness.coordinator.stale_accounts().unwrap();
    assert!(!stale.is_empty());
    assert!(stale.iter().all(|s| s.reason == StaleReason::DayAdvanced));

    let report = harness
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        plan_of(&report, &account),
        Some(RebuildPlan::Revalue { from: tomorrow })
    );
    let rows = harness.rows(&account);
    assert_eq!(rows.last().unwrap().valuation_date, tomorrow);
    untouched_before(&rows_before, &rows, tomorrow);
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());
}

/// Every account's rows and lots equal those of a fresh run over `facts`.
async fn assert_matches_a_fresh_rebuild(live: &Harness, facts: &ScenarioFacts, label: &str) {
    let fresh = harness(facts.clone()).await;
    fresh
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    for account in facts.accounts.iter().filter(|a| !a.is_archived) {
        assert_eq!(
            normalized_valuations(live.rows(&account.id)),
            normalized_valuations(fresh.rows(&account.id)),
            "{label}: valuations of {}",
            account.id
        );
        let incremental = live
            .lot_repo
            .get_all_lots_for_account(&account.id)
            .await
            .unwrap();
        let rebuilt = fresh
            .lot_repo
            .get_all_lots_for_account(&account.id)
            .await
            .unwrap();
        assert_eq!(
            format!("{:#?}", normalized_lots(incremental)),
            format!("{:#?}", normalized_lots(rebuilt)),
            "{label}: lots of {}",
            account.id
        );
    }
}

/// A split decides how every earlier close of its asset reads: editing one
/// revalues its holders from the beginning, not from the split's day.
#[tokio::test]
async fn a_split_edit_revalues_its_holders_from_the_beginning() {
    let scenario = scenario("EDGE-QT-04");
    let facts = scenario.facts();
    let live = harness(facts.clone()).await;
    live.coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();

    let mut after = facts.clone();
    let split = after
        .activities
        .iter_mut()
        .find(|a| a.activity_type == "SPLIT")
        .expect("split");
    split.amount = Some(rust_decimal_macros::dec!(4));
    let split = split.clone();
    live.change_activities(Vec::new(), vec![split], &[], &facts.activities);
    let report = live
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_matches_a_fresh_rebuild(&live, &after, "split edited").await;
}

/// An activity dated after today is left out of the fold; the day it comes
/// due, the account folds it rather than only valuing the new day.
#[tokio::test]
async fn a_future_activity_is_folded_when_its_day_comes() {
    let scenario = scenario("NOM-TRADE-01");
    let mut facts = scenario.facts();
    let mut deposit = facts
        .activities
        .iter()
        .find(|a| a.activity_type == "DEPOSIT")
        .expect("deposit")
        .clone();
    deposit.id = "scheduled-deposit".to_string();
    deposit.activity_date = as_of_instant(facts.as_of, &facts.timezone) + chrono::Duration::days(2);
    facts.activities.push(deposit);
    let live = harness(facts.clone()).await;
    live.coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();

    let mut later = facts.clone();
    later.as_of = facts.as_of + chrono::Duration::days(3);
    crate::utils::clock::set_frozen(as_of_instant(later.as_of, &later.timezone));
    let report = live
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    let account = &facts.accounts[0].id;
    assert!(
        matches!(plan_of(&report, account), Some(RebuildPlan::Refold { .. })),
        "{:?}",
        plan_of(&report, account)
    );
    assert_matches_a_fresh_rebuild(&live, &later, "scheduled deposit due").await;
}

#[tokio::test]
async fn a_backdated_edit_rewrites_from_its_day_only() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let live = harness(facts.clone()).await;
    live.coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    let first_run = crate::utils::clock::now();
    let rows_before = live.rows(&account);

    let mut edited = facts
        .activities
        .iter()
        .filter(|a| a.account_id == account)
        .max_by_key(|a| a.activity_date)
        .unwrap()
        .clone();
    edited.quantity = edited.quantity.map(|q| q * rust_decimal_macros::dec!(2));
    edited.amount = edited.amount.map(|a| a * rust_decimal_macros::dec!(2));
    let dirty_from = edited.activity_date.date_naive().pred_opt().unwrap();
    crate::utils::clock::set_frozen(first_run + chrono::Duration::hours(1));
    live.change_activities(Vec::new(), vec![edited.clone()], &[], &facts.activities);
    let report = live
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert_eq!(
        plan_of(&report, &account),
        Some(RebuildPlan::Refold { from: dirty_from })
    );
    untouched_before(&rows_before, &live.rows(&account), dirty_from);

    let mut after = facts.clone();
    if let Some(slot) = after.activities.iter_mut().find(|a| a.id == edited.id) {
        *slot = edited;
    }
    let fresh = harness(after).await;
    fresh
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert_eq!(
        normalized_valuations(live.rows(&account)),
        normalized_valuations(fresh.rows(&account))
    );
    assert_eq!(
        format!(
            "{:#?}",
            normalized_lots(
                live.lot_repo
                    .get_all_lots_for_account(&account)
                    .await
                    .unwrap()
            )
        ),
        format!(
            "{:#?}",
            normalized_lots(
                fresh
                    .lot_repo
                    .get_all_lots_for_account(&account)
                    .await
                    .unwrap()
            )
        )
    );
}

#[tokio::test]
async fn a_deletion_refolds_from_the_deleted_day() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let harness = harness(facts.clone()).await;
    harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    let removed = facts
        .activities
        .iter()
        .filter(|a| a.account_id == account)
        .max_by_key(|a| a.activity_date)
        .unwrap()
        .clone();
    harness.change_activities(
        Vec::new(),
        Vec::new(),
        std::slice::from_ref(&removed.id),
        &facts.activities,
    );
    let report = harness
        .coordinator
        .ensure_consistent(MarketSyncMode::None, &SilentObserver)
        .await
        .unwrap();
    assert_eq!(
        plan_of(&report, &account),
        Some(RebuildPlan::Refold {
            from: removed.activity_date.date_naive().pred_opt().unwrap()
        })
    );
}

#[tokio::test]
async fn failures_keep_the_markers_and_are_retried() {
    let scenario = scenario("NOM-TRADE-01");
    let harness = harness(scenario.facts()).await;
    harness.store.fail_next_persists(1);
    let report = harness
        .coordinator
        .run_job_with_retry(request(), &SilentObserver, RetryPolicy::immediate(3))
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());

    harness.store.fail_next_persists(5);
    let report = harness
        .coordinator
        .run_job_with_retry(
            PortfolioJobRequest {
                force_full: true,
                ..request()
            },
            &SilentObserver,
            RetryPolicy::immediate(2),
        )
        .await
        .unwrap();
    assert!(!report.failures.is_empty());
    assert!(report
        .failures
        .iter()
        .all(|f| f.code == "PROJECTION_FAILED"));
    assert!(
        !harness.projections.pending_markers().unwrap().is_empty(),
        "a failed run leaves its markers for the next one"
    );
}

#[tokio::test]
async fn concurrent_requests_run_one_after_another() {
    let scenario = scenario("NOM-TRADE-01");
    let harness = harness(scenario.facts()).await;
    let first = harness.coordinator.run_job(request(), &SilentObserver);
    let second = harness.coordinator.run_job(request(), &SilentObserver);
    let (first, second) = tokio::join!(first, second);
    let (first, second) = (first.unwrap(), second.unwrap());
    assert!(first.failures.is_empty() && second.failures.is_empty());
    // One of them did the work; the other found nothing stale.
    assert_eq!(
        [first.plans.is_empty(), second.plans.is_empty()]
            .iter()
            .filter(|empty| **empty)
            .count(),
        1
    );
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());
}

#[tokio::test]
async fn a_future_dated_only_account_projects_without_failing() {
    let scenario = scenario("NOM-TRADE-01");
    let mut facts = scenario.facts();
    let mut account = facts.accounts[0].clone();
    account.id = "acc-future".to_string();
    account.name = "Future".to_string();
    facts.accounts.push(account);
    let mut deposit = facts.activities[0].clone();
    deposit.id = "future-deposit".to_string();
    deposit.account_id = "acc-future".to_string();
    deposit.activity_type = "DEPOSIT".to_string();
    deposit.asset_id = None;
    deposit.quantity = None;
    deposit.unit_price = None;
    deposit.amount = Some(rust_decimal_macros::dec!(100));
    deposit.activity_date = as_of_instant(facts.as_of, &facts.timezone) + chrono::Duration::days(1);
    facts.activities.push(deposit);
    let harness = harness(facts).await;
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert!(harness.coordinator.stale_accounts().unwrap().is_empty());
}

#[tokio::test]
async fn an_out_of_policy_observed_snapshot_fails_only_its_account() {
    let scenario = scenario("NOM-OBS-01");
    let facts = scenario.facts();
    let holdings_account = facts
        .accounts
        .iter()
        .find(|a| a.tracking_mode == crate::accounts::TrackingMode::Holdings)
        .expect("holdings account")
        .id
        .clone();
    let harness = harness(facts).await;
    let bad_date = chrono::NaiveDate::from_ymd_opt(224, 7, 20).unwrap();
    harness
        .snapshot_repo
        .save_snapshots(&[manual_snapshot(&holdings_account, bad_date)])
        .await
        .unwrap();
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!(report.failures[0].account_id, holdings_account);
    assert_eq!(report.failures[0].code, "INVALID_SNAPSHOT_DATE");
    assert!(harness.rows(&holdings_account).is_empty());
    assert!(
        !harness.projections.pending_markers().unwrap().is_empty(),
        "the markers stay until the account can be projected"
    );
}

#[tokio::test]
async fn a_changed_transfer_leg_refolds_its_partner() {
    let scenario = scenario("NOM-TXF-01");
    let facts = scenario.facts();
    let (out, into) = transfer_legs(&facts);
    let harness = harness(facts.clone()).await;
    harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    // Only the sending account is marked; the pair brings the receiver in.
    harness.store.mark(
        MarkerScope::Account(out.account_id.clone()),
        out.activity_date.date_naive(),
    );
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(matches!(
        plan_of(&report, &into.account_id),
        Some(RebuildPlan::Refold { .. })
    ));
}

#[tokio::test]
async fn unsupported_cost_basis_settings_fail_the_account() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let harness = harness(facts).await;
    harness
        .account_repo
        .set_accounting_settings(AccountAccountingSettings {
            cost_basis_method: CostBasisMethod::Lifo,
            ..AccountAccountingSettings::default_for_account(account.clone())
        });
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert_eq!(report.failures.len(), 1, "{:?}", report.failures);
    assert_eq!(report.failures[0].code, "UNSUPPORTED_COST_BASIS");
    assert!(harness
        .lot_repo
        .get_all_lots_for_account(&account)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn explicitly_requested_archived_accounts_are_rebuilt() {
    let scenario = scenario("NOM-TXF-01");
    let mut facts = scenario.facts();
    let (_, into) = transfer_legs(&facts);
    let archived = into.account_id.clone();
    facts
        .accounts
        .iter_mut()
        .find(|a| a.id == archived)
        .unwrap()
        .is_archived = true;
    let harness = harness(facts).await;
    let report = harness
        .coordinator
        .run_job(
            PortfolioJobRequest {
                account_ids: Some(vec![archived.clone()]),
                force_full: true,
                ..request()
            },
            &SilentObserver,
        )
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert!(report.account_ids.contains(&archived));
}

#[tokio::test]
async fn rejected_activities_are_stored_for_the_read_path() {
    let scenario = scenario("EDGE-DRIP-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let harness = harness(facts).await;
    let report = harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    let rejected = harness
        .projections
        .rejections(std::slice::from_ref(&account))
        .unwrap();
    assert_eq!(rejected.len(), 1, "{rejected:?}");
    assert_eq!(rejected[0].activity_id, "drip-1");
}

fn manual_snapshot(account_id: &str, date: chrono::NaiveDate) -> AccountStateSnapshot {
    AccountStateSnapshot {
        id: format!("{account_id}_{date}"),
        account_id: account_id.to_string(),
        snapshot_date: date,
        source: SnapshotSource::ManualEntry,
        ..AccountStateSnapshot::default()
    }
}

fn transfer_legs(
    facts: &ScenarioFacts,
) -> (crate::activities::Activity, crate::activities::Activity) {
    let out = facts
        .activities
        .iter()
        .find(|a| a.activity_type == "TRANSFER_OUT")
        .expect("transfer out")
        .clone();
    let into = facts
        .activities
        .iter()
        .find(|a| a.activity_type == "TRANSFER_IN" && a.source_group_id == out.source_group_id)
        .expect("paired transfer in")
        .clone();
    (out, into)
}

#[tokio::test]
async fn manual_snapshots_survive_a_rebuild() {
    let scenario = scenario("NOM-TRADE-01");
    let facts = scenario.facts();
    let account = facts.accounts[0].id.clone();
    let manual_date = facts.as_of - chrono::Duration::days(1);
    let harness = harness(facts).await;
    harness
        .snapshot_repo
        .save_snapshots(&[manual_snapshot(&account, manual_date)])
        .await
        .unwrap();
    harness
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    let snapshots = harness
        .snapshot_repo
        .get_snapshots_by_account(&account, None, None)
        .unwrap();
    assert!(snapshots
        .iter()
        .any(|s| s.snapshot_date == manual_date && s.source == SnapshotSource::ManualEntry));
    assert!(snapshots
        .iter()
        .any(|s| s.source == SnapshotSource::Calculated));
}

#[tokio::test]
async fn one_run_refolds_and_revalues_different_accounts_like_a_fresh_rebuild() {
    // acc-b's deposit changes (acc-b and its transfer partner acc-a refold)
    // while a new close revalues the holdings account acc-h from 01-10: one
    // job writes both kinds, in windows cut at each account's first day.
    let scenario = scenario("NOM-MIX-01");
    let before = scenario.facts();
    let mut after = before.clone();
    let deposit = after
        .activities
        .iter_mut()
        .find(|a| a.id == "dep-2")
        .expect("dep-2");
    deposit.amount = deposit
        .amount
        .map(|amount| amount + rust_decimal_macros::dec!(100));
    let edited: Vec<Activity> = vec![deposit.clone()];
    let template = before
        .quotes
        .iter()
        .find(|q| q.asset_id == "aapl" && q.timestamp.date_naive().to_string() == "2025-01-09")
        .expect("aapl close on 01-09")
        .clone();
    let late = Quote {
        id: "aapl-2025-01-10".to_string(),
        timestamp: template.timestamp + chrono::Duration::days(1),
        close: rust_decimal_macros::dec!(109),
        ..template
    };
    let revalued_from = late.timestamp.date_naive();
    after.quotes.push(late.clone());

    for cadence in [
        WindowCadence::Days(2),
        WindowCadence::Days(3),
        WindowCadence::Year,
    ] {
        let live = harness_with(before.clone(), cadence).await;
        live.coordinator
            .run_job(request(), &SilentObserver)
            .await
            .unwrap();
        live.change_activities(Vec::new(), edited.clone(), &[], &before.activities);
        live.add_quotes(vec![late.clone()]);
        let report = live
            .coordinator
            .run_job(request(), &SilentObserver)
            .await
            .unwrap();
        assert!(
            report.failures.is_empty(),
            "{cadence:?}: {:?}",
            report.failures
        );
        for account in ["acc-a", "acc-b"] {
            assert!(
                matches!(plan_of(&report, account), Some(RebuildPlan::Refold { .. })),
                "{cadence:?}: {account} refolds"
            );
        }
        assert_eq!(
            plan_of(&report, "acc-h"),
            Some(RebuildPlan::Revalue {
                from: revalued_from
            }),
            "{cadence:?}"
        );

        let fresh = harness_with(after.clone(), cadence).await;
        fresh
            .coordinator
            .run_job(request(), &SilentObserver)
            .await
            .unwrap();
        for account in ["acc-a", "acc-b", "acc-h"] {
            assert_eq!(
                normalized_valuations(live.rows(account)),
                normalized_valuations(fresh.rows(account)),
                "{cadence:?}: valuations of {account}"
            );
        }
    }
}

#[tokio::test]
async fn an_fx_rate_revalues_from_its_previous_observation_and_refolds_only_later_activity() {
    // USD/CAD is observed daily to 01-08, then on 01-13. A rate for 01-11 is
    // the nearest one for 01-10 too (and 01-12), so every account revalues
    // from 01-09; acc-1 sells (converting USD) on 01-10 and refolds, acc-2's
    // only activity is its 01-02 deposit, so it just revalues.
    let day = |d: u32| NaiveDate::from_ymd_opt(2025, 1, d).unwrap();
    let mut before = scenario("NOM-FX-01").facts();
    before
        .fx_rates
        .retain(|r| !(day(9)..=day(12)).contains(&r.timestamp.date_naive()));
    let mut second = before.accounts[0].clone();
    second.id = "acc-2".to_string();
    second.name = "CAD savings".to_string();
    before.accounts.push(second);
    let mut deposit = before
        .activities
        .iter()
        .find(|a| a.id == "dep-1")
        .expect("dep-1")
        .clone();
    deposit.id = "dep-2".to_string();
    deposit.account_id = "acc-2".to_string();
    before.activities.push(deposit);

    let mut rate = before
        .fx_rates
        .iter()
        .find(|r| r.timestamp.date_naive() == day(8))
        .expect("01-08 rate")
        .clone();
    rate.timestamp += chrono::Duration::days(3);
    rate.rate = rust_decimal_macros::dec!(1.50);
    let mut after = before.clone();
    after.fx_rates.push(rate.clone());

    let live = harness(before.clone()).await;
    live.coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    live.fx_repo.add_rates(vec![rate.clone()]);
    live.store.mark(MarkerScope::Fx(rate.id.clone()), day(11));
    let report = live
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    assert!(report.failures.is_empty(), "{:?}", report.failures);
    assert_eq!(
        plan_of(&report, "acc-1"),
        Some(RebuildPlan::Refold { from: day(9) })
    );
    assert_eq!(
        plan_of(&report, "acc-2"),
        Some(RebuildPlan::Revalue { from: day(9) })
    );

    let fresh = harness(after).await;
    fresh
        .coordinator
        .run_job(request(), &SilentObserver)
        .await
        .unwrap();
    for account in ["acc-1", "acc-2"] {
        assert_eq!(
            normalized_valuations(live.rows(account)),
            normalized_valuations(fresh.rows(account)),
            "valuations of {account}"
        );
    }
    let realized = |disposals: Vec<crate::lots::LotDisposal>| -> Vec<(String, String)> {
        disposals
            .into_iter()
            .map(|d| (d.disposal_activity_id, d.realized_pnl_base))
            .collect()
    };
    assert_eq!(
        realized(
            live.lot_repo
                .get_lot_disposals_for_account("acc-1")
                .await
                .unwrap()
        ),
        realized(
            fresh
                .lot_repo
                .get_lot_disposals_for_account("acc-1")
                .await
                .unwrap()
        )
    );
}
