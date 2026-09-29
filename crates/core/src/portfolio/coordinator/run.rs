//! One run over the pending markers: a plan per account, then one pass over
//! the windows. Each window is read, folded or revalued, and written before the
//! next is read, so memory holds one window plus the running state.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use chrono::NaiveDate;
use wealthfolio_portfolio_engine as engine;
use wealthfolio_portfolio_engine::model::{
    AccountId, AccountState, DateRange, Keyframe, LotClosure, LotDisposal as KernelDisposal,
    ProjectionBundle, ProjectionState, TrackingMode,
};
use wealthfolio_portfolio_engine::{Diagnostic, DiagnosticCode};

use super::persist::{self, Resolved, WindowCadence};
use super::{blocking, facts, AccountPlan, FactSources, RebuildPlan};
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
    let holdings = &resolved.assets_by_account;
    let holders = |asset: &str| -> Vec<&str> {
        targets
            .iter()
            .copied()
            .filter(|t| {
                holdings
                    .get(*t)
                    .is_some_and(|assets| assets.contains(asset))
            })
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
    pub resolved: Arc<Resolved>,
    pub cadence: WindowCadence,
}

/// One pass over the windows. The fold runs in memory from genesis when an
/// account refolds; from the first day an account rewrites, each window loads
/// once the prices of the assets its accounts reference, values the refolded
/// accounts from the fold and the revalued ones from their stored keyframes,
/// and writes their rows together. Returns what the run commits last (the
/// caller adds the consumed markers).
pub(super) async fn execute(context: &RunContext, plan: &Plan) -> Result<RunCompletion> {
    let resolved = Arc::clone(&context.resolved);
    let starts: BTreeSet<NaiveDate> = plan
        .refold
        .values()
        .chain(plan.revalue.values())
        .copied()
        .collect();
    let Some(earliest) = starts.first().copied() else {
        return Ok(RunCompletion::default());
    };
    let folds = !plan.refold.is_empty();
    let as_of = resolved.facts.policy().as_of;
    let range = if folds {
        resolved.range()
    } else {
        DateRange {
            start: earliest.max(resolved.genesis).min(as_of),
            end: as_of,
        }
    };
    let stored = Arc::new(stored_inputs(context, &plan.revalue).await?);

    let owner: HashMap<String, String> = resolved
        .facts
        .activities()
        .iter()
        .map(|a| (a.id.as_str().to_string(), a.account.as_str().to_string()))
        .collect();
    let mut rejections: BTreeMap<String, Vec<RejectedActivity>> = plan
        .refold
        .keys()
        .map(|account| (account.clone(), Vec::new()))
        .collect();
    let mut disposals: BTreeMap<String, Vec<KernelDisposal>> = BTreeMap::new();
    let mut closures: Vec<LotClosure> = Vec::new();
    let mut state: Option<ProjectionState> = None;
    let mut started: BTreeSet<AccountId> = BTreeSet::new();
    let mut written: BTreeSet<String> = BTreeSet::new();

    for window in windows(context.cadence, range, &starts) {
        let refold = writers(&plan.refold, window, &mut written);
        let revalue = writers(&plan.revalue, window, &mut written);
        let step = folds.then(|| {
            let seed = if refold.is_empty() {
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
            FoldStep {
                state: state.take(),
                seed,
                active: refold,
            }
        });
        let job_resolved = Arc::clone(&resolved);
        let sources = context.sources.clone();
        let stored = Arc::clone(&stored);
        let output =
            blocking(move || run_window(&job_resolved, &sources, window, step, &revalue, &stored))
                .await?;
        if !output.rows.is_empty() {
            context.projections.write_window(output.rows).await?;
        }
        let Some(folded) = output.folded else {
            continue;
        };
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
            if plan
                .refold
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

    let mut completion = RunCompletion::default();
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
    for (account, from) in &plan.refold {
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

/// The cadence windows over `range`, each also cut at every day an account
/// starts rewriting, so a window only values accounts that write all of it.
fn windows(
    cadence: WindowCadence,
    range: DateRange,
    starts: &BTreeSet<NaiveDate>,
) -> Vec<DateRange> {
    let mut windows = Vec::new();
    for window in cadence.windows(range) {
        let mut start = window.start;
        for cut in starts.range((Bound::Excluded(window.start), Bound::Included(window.end))) {
            let Some(end) = cut.pred_opt() else {
                continue;
            };
            windows.push(DateRange { start, end });
            start = *cut;
        }
        windows.push(DateRange {
            start,
            end: window.end,
        });
    }
    windows
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

/// A window of the fold: the state carried in, the seeds of the accounts the
/// window writes, and those writers.
struct FoldStep {
    state: Option<ProjectionState>,
    seed: BTreeMap<AccountId, AccountState>,
    active: Vec<Writer>,
}

/// What revaluing reads from the last run: stored disposals price unquoted
/// outbound transfers, stored rejections keep the activities the last fold
/// rejected out of the flows.
struct StoredInputs {
    disposals: Vec<KernelDisposal>,
    rejected: Vec<Diagnostic>,
}

async fn stored_inputs(
    context: &RunContext,
    accounts: &BTreeMap<String, NaiveDate>,
) -> Result<StoredInputs> {
    if accounts.is_empty() {
        return Ok(StoredInputs {
            disposals: Vec::new(),
            rejected: Vec::new(),
        });
    }
    let mut stored_disposals: Vec<LotDisposal> = Vec::new();
    for account in accounts.keys() {
        stored_disposals.extend(context.lots.get_lot_disposals_for_account(account).await?);
    }
    let ids: Vec<String> = accounts.keys().cloned().collect();
    Ok(StoredInputs {
        disposals: super::rows::stored_disposals(&stored_disposals),
        rejected: context
            .projections
            .rejections(&ids)?
            .into_iter()
            .map(|r| Diagnostic::error(DiagnosticCode::ActivityRejected, r.activity_id, r.message))
            .collect(),
    })
}

struct WindowOutput {
    rows: Vec<WindowRows>,
    folded: Option<Folded>,
}

struct Folded {
    final_state: ProjectionState,
    keyframed: Vec<AccountId>,
    disposals: Vec<KernelDisposal>,
    closures: Vec<LotClosure>,
    rejections: Vec<Diagnostic>,
}

impl Folded {
    fn from_bundle(bundle: ProjectionBundle) -> Self {
        Folded {
            keyframed: bundle
                .keyframes
                .iter()
                .filter(|(_, frames)| !frames.is_empty())
                .map(|(id, _)| id.clone())
                .collect(),
            rejections: bundle
                .diagnostics
                .iter()
                .filter(|d| d.code == DiagnosticCode::ActivityRejected)
                .cloned()
                .collect(),
            final_state: bundle.final_state,
            disposals: bundle.disposals,
            closures: bundle.closures,
        }
    }
}

/// Folds the window (when the run refolds), then loads its prices once for
/// every account it writes and values both kinds against them.
fn run_window(
    resolved: &Resolved,
    sources: &FactSources,
    window: DateRange,
    fold: Option<FoldStep>,
    revalue: &[Writer],
    stored: &StoredInputs,
) -> Result<WindowOutput> {
    let fx = engine::FxResolver {
        surface: &resolved.surfaces.fx,
        policy: resolved.facts.policy(),
    };
    let fold = match fold {
        Some(step) => {
            let bundle =
                engine::project(&resolved.ledger, &resolved.facts, &fx, step.state, window)?;
            Some((bundle, step.seed, step.active))
        }
        None => None,
    };
    let refold: &[Writer] = fold.as_ref().map_or(&[], |(_, _, active)| active);
    let mut rows = Vec::new();
    if !refold.is_empty() || !revalue.is_empty() {
        let assets: BTreeSet<&String> = refold
            .iter()
            .chain(revalue)
            .filter_map(|writer| resolved.assets_by_account.get(&writer.account))
            .flatten()
            .collect();
        let assets: Vec<String> = assets.into_iter().cloned().collect();
        let quotes = facts::window_quotes(sources, &assets, window.start, window.end)?;
        let surfaces = resolved.window_surfaces(quotes);
        if let Some((bundle, seed, active)) = &fold {
            rows.extend(refold_rows(
                resolved, &surfaces, window, bundle, seed, active,
            ));
        }
        if !revalue.is_empty() {
            rows.extend(revalue_rows(
                resolved, sources, &surfaces, window, revalue, stored,
            )?);
        }
    }
    Ok(WindowOutput {
        rows,
        folded: fold.map(|(bundle, _, _)| Folded::from_bundle(bundle)),
    })
}

fn refold_rows(
    resolved: &Resolved,
    surfaces: &engine::ResolvedSurfaces,
    window: DateRange,
    bundle: &ProjectionBundle,
    seed: &BTreeMap<AccountId, AccountState>,
    active: &[Writer],
) -> Vec<WindowRows> {
    if active.is_empty() {
        return Vec::new();
    }
    let series = engine::value_window(
        &engine::ValueInputs {
            resolved: engine::Resolved {
                facts: &resolved.facts,
                ledger: &resolved.ledger,
                surfaces,
                range: window,
            },
            bundle,
        },
        seed,
        Some(&valued(active)),
    );
    let base = resolved.facts.policy().base_currency.as_str();
    let mut rows = Vec::with_capacity(active.len());
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
    rows
}

fn revalue_rows(
    resolved: &Resolved,
    sources: &FactSources,
    surfaces: &engine::ResolvedSurfaces,
    window: DateRange,
    active: &[Writer],
    stored: &StoredInputs,
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
        disposals: stored.disposals.clone(),
        closures: Vec::new(),
        diagnostics: stored.rejected.clone(),
    };
    let series = engine::value_window(
        &engine::ValueInputs {
            resolved: engine::Resolved {
                facts: &resolved.facts,
                ledger: &resolved.ledger,
                surfaces,
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
