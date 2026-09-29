//! One run over the pending markers: a plan per account, then the refold and
//! revalue loops. Each window is read, folded or revalued, and written before
//! the next is read, so memory holds one window plus the running state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use chrono::NaiveDate;
use wealthfolio_portfolio_engine as engine;
use wealthfolio_portfolio_engine::model::{
    AccountId, AccountState, DateRange, Keyframe, LotClosure, LotDisposal as KernelDisposal,
    ProjectionBundle, ProjectionState, TrackingMode,
};
use wealthfolio_portfolio_engine::{Diagnostic, DiagnosticCode};

use super::persist::{self, Resolved, WindowCadence};
use super::{blocking, facts, AccountPlan, FactSources, LoadedFacts, RebuildPlan};
use crate::errors::Result;
use crate::lots::{LotDisposal, LotRepositoryTrait};
use crate::portfolio::projection::{
    LotBook, MarkerScope, ProjectionMarker, ProjectionStoreTrait, RejectedActivity, RunCompletion,
    WindowRows, GENESIS,
};
use crate::portfolio::snapshot::SnapshotSource;

/// First day each account's stored rows must be rewritten from.
#[derive(Debug, Default)]
pub(super) struct Plan {
    /// Facts changed: fold from the first activity, write from the day.
    pub refold: BTreeMap<String, NaiveDate>,
    /// Prices changed or the day moved: revalue stored keyframes (or
    /// observed snapshots) from the day.
    pub revalue: BTreeMap<String, NaiveDate>,
}

impl Plan {
    pub fn account_plans(&self) -> Vec<AccountPlan> {
        let refold = self.refold.iter().map(|(id, from)| AccountPlan {
            account_id: id.clone(),
            plan: RebuildPlan::Refold { from: *from },
        });
        let revalue = self.revalue.iter().map(|(id, from)| AccountPlan {
            account_id: id.clone(),
            plan: RebuildPlan::Revalue { from: *from },
        });
        refold.chain(revalue).collect()
    }
}

fn lower(map: &mut BTreeMap<String, NaiveDate>, account: &str, day: NaiveDate) -> bool {
    match map.get_mut(account) {
        Some(existing) if *existing <= day => false,
        Some(existing) => {
            *existing = day;
            true
        }
        None => {
            map.insert(account.to_string(), day);
            true
        }
    }
}

/// Maps the markers onto `targets`: asset markers reach the accounts holding
/// the asset, a refold reaches the account's transfer partners (their lots
/// and flows come from its legs), and an account whose valuations end before
/// today is revalued from the next day.
pub(super) fn plan(
    resolved: &Resolved,
    markers: &[ProjectionMarker],
    last_valued: &HashMap<String, NaiveDate>,
    today: NaiveDate,
    targets: &[String],
) -> Plan {
    let facts = &resolved.facts;
    let targets: BTreeSet<&str> = targets.iter().map(String::as_str).collect();
    let mut holdings: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for activity in facts.activities() {
        if let Some(asset) = &activity.asset {
            holdings
                .entry(activity.account.as_str())
                .or_default()
                .insert(asset.as_str());
        }
    }
    for snapshot in facts.observed_snapshots() {
        for asset in snapshot.positions.keys() {
            holdings
                .entry(snapshot.account.as_str())
                .or_default()
                .insert(asset.as_str());
        }
    }
    let holders = |asset: &str| -> Vec<&str> {
        targets
            .iter()
            .copied()
            .filter(|t| holdings.get(t).is_some_and(|assets| assets.contains(asset)))
            .collect()
    };

    let mut plan = Plan::default();
    for marker in markers {
        let day = marker.dirty_from;
        match &marker.scope {
            MarkerScope::All => {
                for target in &targets {
                    lower(&mut plan.refold, target, day);
                }
            }
            MarkerScope::Account(id) if targets.contains(id.as_str()) => {
                lower(&mut plan.refold, id, day);
            }
            MarkerScope::Account(_) => {}
            MarkerScope::Asset(asset) => {
                for target in holders(asset) {
                    lower(&mut plan.refold, target, day);
                }
            }
            MarkerScope::Prices(asset) => {
                for target in holders(asset) {
                    lower(&mut plan.revalue, target, day);
                }
            }
        }
    }
    loop {
        let mut changed = false;
        for pair in facts.transfer_pairs().iter() {
            for (from, to) in [
                (&pair.out_account, &pair.in_account),
                (&pair.in_account, &pair.out_account),
            ] {
                let Some(day) = plan.refold.get(from.as_str()).copied() else {
                    continue;
                };
                if targets.contains(to.as_str()) {
                    changed |= lower(&mut plan.refold, to.as_str(), day);
                }
            }
        }
        if !changed {
            break;
        }
    }
    let has_facts = |account: &str| holdings.contains_key(account);
    for target in &targets {
        match last_valued.get(*target) {
            Some(last) if *last < today => {
                if let Some(next) = last.succ_opt() {
                    lower(&mut plan.revalue, target, next);
                }
            }
            Some(_) => {}
            // Never projected: nothing stored to revalue.
            None if has_facts(target) => {
                lower(&mut plan.refold, target, GENESIS);
            }
            None => {}
        }
    }
    let accounts = facts.accounts();
    let holdings_mode: Vec<String> = plan
        .refold
        .keys()
        .filter(|id| {
            accounts
                .get(&AccountId::new(id.as_str()))
                .is_some_and(|a| a.tracking == TrackingMode::Holdings)
        })
        .cloned()
        .collect();
    // Holdings-mode accounts never fold: their facts are observed snapshots.
    for id in holdings_mode {
        if let Some(day) = plan.refold.remove(&id) {
            lower(&mut plan.revalue, &id, day);
        }
    }
    // A refold rewrites valuations too.
    let covered: Vec<(String, NaiveDate)> = plan
        .revalue
        .iter()
        .filter(|(id, _)| plan.refold.contains_key(*id))
        .map(|(id, day)| (id.clone(), *day))
        .collect();
    for (id, day) in covered {
        plan.revalue.remove(&id);
        lower(&mut plan.refold, &id, day);
    }
    plan
}

pub(super) struct RunContext {
    pub sources: FactSources,
    pub projections: Arc<dyn ProjectionStoreTrait>,
    pub lots: Arc<dyn LotRepositoryTrait>,
    pub loaded: Arc<LoadedFacts>,
    pub resolved: Arc<Resolved>,
    pub cadence: WindowCadence,
}

/// Refolds, then revalues; returns what the run commits last (the caller adds
/// the consumed markers).
pub(super) async fn execute(context: &RunContext, plan: &Plan) -> Result<RunCompletion> {
    let completion = refold(context, &plan.refold).await?;
    revalue(context, &plan.revalue).await?;
    Ok(completion)
}

/// Where an account's rows start in a window: it writes rows from
/// `max(from, window start)`, and clears stored rows from the window start,
/// or from `from` in its first written window (which also clears rows before
/// a history that now starts later).
#[derive(Clone)]
struct Writer {
    account: String,
    from: NaiveDate,
    clear_from: NaiveDate,
}

fn writers(
    accounts: &BTreeMap<String, NaiveDate>,
    window: DateRange,
    written: &mut BTreeSet<String>,
) -> Vec<Writer> {
    accounts
        .iter()
        .filter(|(_, from)| **from <= window.end)
        .map(|(account, from)| Writer {
            account: account.clone(),
            from: *from,
            clear_from: if written.insert(account.clone()) {
                *from
            } else {
                window.start
            },
        })
        .collect()
}

/// The accounts a window values: only those it writes.
fn valued(active: &[Writer]) -> BTreeSet<AccountId> {
    active
        .iter()
        .map(|writer| AccountId::new(writer.account.as_str()))
        .collect()
}

/// Rows `[start, end]` or, for the window that ends today, `[start, ∞)`.
fn row_end(resolved: &Resolved, window: DateRange) -> Option<NaiveDate> {
    (window.end < resolved.facts.policy().as_of).then_some(window.end)
}

struct FoldedWindow {
    rows: Vec<WindowRows>,
    final_state: ProjectionState,
    keyframed: Vec<AccountId>,
    disposals: Vec<KernelDisposal>,
    closures: Vec<LotClosure>,
    rejections: Vec<Diagnostic>,
}

async fn refold(
    context: &RunContext,
    accounts: &BTreeMap<String, NaiveDate>,
) -> Result<RunCompletion> {
    let mut completion = RunCompletion::default();
    let Some(earliest) = accounts.values().min().copied() else {
        return Ok(completion);
    };
    let resolved = Arc::clone(&context.resolved);
    let owner: HashMap<String, String> = resolved
        .facts
        .activities()
        .iter()
        .map(|a| (a.id.as_str().to_string(), a.account.as_str().to_string()))
        .collect();
    let mut rejections: BTreeMap<String, Vec<RejectedActivity>> = accounts
        .keys()
        .map(|account| (account.clone(), Vec::new()))
        .collect();
    let mut disposals: BTreeMap<String, Vec<KernelDisposal>> = BTreeMap::new();
    let mut closures: Vec<LotClosure> = Vec::new();
    let mut state: Option<ProjectionState> = None;
    let mut started: BTreeSet<AccountId> = BTreeSet::new();
    let mut written: BTreeSet<String> = BTreeSet::new();

    for window in context.cadence.windows(resolved.range()) {
        let active = writers(accounts, window, &mut written);
        let seed: BTreeMap<AccountId, AccountState> = if active.is_empty() {
            BTreeMap::new()
        } else {
            state
                .as_ref()
                .map(|s| {
                    s.accounts
                        .iter()
                        .filter(|(id, _)| started.contains(*id))
                        .map(|(id, account)| (id.clone(), account.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let job_resolved = Arc::clone(&resolved);
        let sources = context.sources.clone();
        let loaded = Arc::clone(&context.loaded);
        let prior = state.take();
        let folded = blocking(move || {
            fold_window(
                &job_resolved,
                &sources,
                &loaded,
                window,
                prior,
                &seed,
                &active,
            )
        })
        .await?;
        if !folded.rows.is_empty() {
            context.projections.write_window(folded.rows).await?;
        }
        for diagnostic in folded.rejections {
            let Some(account) = owner.get(&diagnostic.source) else {
                continue;
            };
            if let Some(list) = rejections.get_mut(account) {
                list.push(RejectedActivity {
                    activity_id: diagnostic.source,
                    message: diagnostic.message,
                });
            }
        }
        for disposal in folded.disposals {
            if accounts
                .get(disposal.account.as_str())
                .is_some_and(|from| disposal.date >= *from)
            {
                disposals
                    .entry(disposal.account.as_str().to_string())
                    .or_default()
                    .push(disposal);
            }
        }
        closures.extend(
            folded
                .closures
                .into_iter()
                .filter(|closure| closure.close_date >= earliest),
        );
        started.extend(folded.keyframed);
        state = Some(folded.final_state);
    }

    let Some(final_state) = state else {
        return Ok(completion);
    };
    let fx = engine::FxResolver {
        surface: &resolved.surfaces.fx,
        policy: resolved.facts.policy(),
    };
    let records = engine::lot_records(
        &ProjectionBundle {
            keyframes: BTreeMap::new(),
            final_state,
            disposals: Vec::new(),
            closures,
            diagnostics: Vec::new(),
        },
        &resolved.facts,
        &fx,
    );
    for (account, from) in accounts {
        let lots = persist::lot_rows(&resolved, records.clone(), account)
            .into_iter()
            .filter(|lot| {
                lot.close_date
                    .as_deref()
                    .and_then(|d| d.parse::<NaiveDate>().ok())
                    .is_none_or(|closed| closed >= *from)
            })
            .collect();
        let own: Vec<&KernelDisposal> = disposals
            .get(account)
            .map(|rows| rows.iter().collect())
            .unwrap_or_default();
        completion.lot_books.push(LotBook {
            account_id: account.clone(),
            since: *from,
            lots,
            disposals: persist::disposal_rows(&resolved, &own, account),
        });
    }
    completion.rejections = rejections.into_iter().collect();
    Ok(completion)
}

fn fold_window(
    resolved: &Resolved,
    sources: &FactSources,
    loaded: &LoadedFacts,
    window: DateRange,
    state: Option<ProjectionState>,
    seed: &BTreeMap<AccountId, AccountState>,
    active: &[Writer],
) -> Result<FoldedWindow> {
    let fx = engine::FxResolver {
        surface: &resolved.surfaces.fx,
        policy: resolved.facts.policy(),
    };
    let bundle = engine::project(&resolved.ledger, &resolved.facts, &fx, state, window)?;
    let mut rows = Vec::with_capacity(active.len());
    if !active.is_empty() {
        let quotes = facts::window_quotes(sources, &loaded.asset_ids, window.start, window.end)?;
        let surfaces = resolved.window_surfaces(quotes);
        let series = engine::value_window(
            &engine::ValueInputs {
                resolved: engine::Resolved {
                    facts: &resolved.facts,
                    ledger: &resolved.ledger,
                    surfaces: &surfaces,
                    range: window,
                },
                bundle: &bundle,
            },
            seed,
            Some(&valued(active)),
        );
        let base = resolved.facts.policy().base_currency.as_str();
        for writer in active {
            let id = AccountId::new(writer.account.as_str());
            let Some(account) = resolved.facts.accounts().get(&id) else {
                // No facts left (the account was emptied): clear its rows.
                rows.push(WindowRows {
                    account_id: writer.account.clone(),
                    start: writer.clear_from,
                    end: row_end(resolved, window),
                    snapshots: Some(Vec::new()),
                    valuations: Vec::new(),
                });
                continue;
            };
            let first_day = writer.from.max(window.start);
            let frames: Vec<&Keyframe> = bundle
                .keyframes
                .get(&id)
                .map(|frames| frames.iter().filter(|f| f.date >= first_day).collect())
                .unwrap_or_default();
            rows.push(WindowRows {
                account_id: writer.account.clone(),
                start: writer.clear_from,
                end: row_end(resolved, window),
                snapshots: Some(persist::snapshot_rows(
                    &frames,
                    &writer.account,
                    &account.currency,
                )),
                valuations: series
                    .get(&id)
                    .map(|s| persist::valuation_rows(s, &writer.account, base))
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|row| row.valuation_date >= first_day)
                    .collect(),
            });
        }
    }
    let keyframed = bundle
        .keyframes
        .iter()
        .filter(|(_, frames)| !frames.is_empty())
        .map(|(id, _)| id.clone())
        .collect();
    let rejections = bundle
        .diagnostics
        .iter()
        .filter(|d| d.code == DiagnosticCode::ActivityRejected)
        .cloned()
        .collect();
    Ok(FoldedWindow {
        rows,
        final_state: bundle.final_state,
        keyframed,
        disposals: bundle.disposals,
        closures: bundle.closures,
        rejections,
    })
}

async fn revalue(context: &RunContext, accounts: &BTreeMap<String, NaiveDate>) -> Result<()> {
    let Some(earliest) = accounts.values().min().copied() else {
        return Ok(());
    };
    let resolved = Arc::clone(&context.resolved);
    let as_of = resolved.facts.policy().as_of;
    // Stored disposals price unquoted outbound transfers; stored rejections
    // keep the activities the last fold rejected out of the flows.
    let mut stored_disposals: Vec<LotDisposal> = Vec::new();
    for account in accounts.keys() {
        stored_disposals.extend(context.lots.get_lot_disposals_for_account(account).await?);
    }
    let disposals = Arc::new(super::rows::stored_disposals(&stored_disposals));
    let ids: Vec<String> = accounts.keys().cloned().collect();
    let rejected: Arc<Vec<Diagnostic>> = Arc::new(
        context
            .projections
            .rejections(&ids)?
            .into_iter()
            .map(|r| Diagnostic::error(DiagnosticCode::ActivityRejected, r.activity_id, r.message))
            .collect(),
    );
    let range = DateRange {
        start: earliest.max(resolved.genesis).min(as_of),
        end: as_of,
    };
    let mut written: BTreeSet<String> = BTreeSet::new();
    for window in context.cadence.windows(range) {
        let active = writers(accounts, window, &mut written);
        if active.is_empty() {
            continue;
        }
        let job_resolved = Arc::clone(&resolved);
        let sources = context.sources.clone();
        let loaded = Arc::clone(&context.loaded);
        let disposals = Arc::clone(&disposals);
        let rejected = Arc::clone(&rejected);
        let rows = blocking(move || {
            revalue_window(
                &job_resolved,
                &sources,
                &loaded,
                window,
                &active,
                &disposals,
                &rejected,
            )
        })
        .await?;
        context.projections.write_window(rows).await?;
    }
    Ok(())
}

fn revalue_window(
    resolved: &Resolved,
    sources: &FactSources,
    loaded: &LoadedFacts,
    window: DateRange,
    active: &[Writer],
    disposals: &[KernelDisposal],
    rejected: &[Diagnostic],
) -> Result<Vec<WindowRows>> {
    let mut keyframes: BTreeMap<AccountId, Vec<Keyframe>> = BTreeMap::new();
    let mut seed: BTreeMap<AccountId, AccountState> = BTreeMap::new();
    for writer in active {
        let id = AccountId::new(writer.account.as_str());
        let Some(account) = resolved.facts.accounts().get(&id) else {
            continue;
        };
        if account.tracking == TrackingMode::Holdings {
            continue;
        }
        if let Some(before) = window.start.pred_opt() {
            if let Some(snapshot) = sources
                .snapshots
                .get_latest_calculated_snapshot_on_or_before(&writer.account, before)?
            {
                seed.insert(
                    id.clone(),
                    persist::account_state_from_snapshot(&id, &account.currency, &snapshot),
                );
            }
        }
        let mut frames: Vec<Keyframe> = sources
            .snapshots
            .get_snapshots_by_account(&writer.account, Some(window.start), Some(window.end))?
            .iter()
            .filter(|s| s.source == SnapshotSource::Calculated)
            .map(|s| Keyframe {
                date: s.snapshot_date,
                state: persist::account_state_from_snapshot(&id, &account.currency, s),
            })
            .collect();
        frames.sort_by_key(|f| f.date);
        keyframes.insert(id, frames);
    }
    let bundle = ProjectionBundle {
        keyframes,
        final_state: ProjectionState {
            date: window.end,
            accounts: BTreeMap::new(),
            transfer_cache: BTreeMap::new(),
        },
        disposals: disposals.to_vec(),
        closures: Vec::new(),
        diagnostics: rejected.to_vec(),
    };
    let quotes = facts::window_quotes(sources, &loaded.asset_ids, window.start, window.end)?;
    let surfaces = resolved.window_surfaces(quotes);
    let series = engine::value_window(
        &engine::ValueInputs {
            resolved: engine::Resolved {
                facts: &resolved.facts,
                ledger: &resolved.ledger,
                surfaces: &surfaces,
                range: window,
            },
            bundle: &bundle,
        },
        &seed,
        Some(&valued(active)),
    );
    let base = resolved.facts.policy().base_currency.as_str();
    Ok(active
        .iter()
        .map(|writer| {
            let first_day = writer.from.max(window.start);
            WindowRows {
                account_id: writer.account.clone(),
                start: writer.clear_from,
                end: row_end(resolved, window),
                snapshots: None,
                valuations: series
                    .get(&AccountId::new(writer.account.as_str()))
                    .map(|s| persist::valuation_rows(s, &writer.account, base))
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|row| row.valuation_date >= first_day)
                    .collect(),
            }
        })
        .collect())
}
