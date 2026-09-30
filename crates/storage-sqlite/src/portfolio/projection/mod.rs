//! Kernel projection persistence: the markers triggers record (see the
//! `projection_state` migration), windowed row writes, and the end-of-run
//! commit of lot books, rejections and consumed markers.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::NaiveDate;
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel::sqlite::SqliteConnection;
use wealthfolio_core::errors::Result;
use wealthfolio_core::portfolio::projection::{
    MarkerScope, ProjectionMarker, ProjectionStoreTrait, RejectedActivity, RunCompletion,
    WindowRows, GENESIS,
};
use wealthfolio_core::portfolio::snapshot::Position;

use crate::db::{get_connection, DbPool, WriteHandle};
use crate::errors::StorageError;
use crate::lots::{filter_and_normalize_lots, LotDisposalDB, LotRecordDB};
use crate::portfolio::snapshot::{AccountStateSnapshotDB, SnapshotRepository};
use crate::portfolio::valuation::DailyAccountValuationDB;

pub struct ProjectionStore {
    pool: Arc<DbPool>,
    writer: WriteHandle,
}

impl ProjectionStore {
    pub fn new(pool: Arc<DbPool>, writer: WriteHandle) -> Self {
        Self { pool, writer }
    }
}

const SOURCE_CALCULATED: &str = "CALCULATED";

/// Lowers a marker's dirty day and bumps its version, as the triggers do.
const INVALIDATE: &str = "INSERT INTO projection_state (scope, dirty_from, version) VALUES (?, ?, 1) \
     ON CONFLICT (scope) DO UPDATE SET \
     dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from), \
     version = projection_state.version + 1";

#[derive(QueryableByName)]
struct MarkerRow {
    #[diesel(sql_type = Text)]
    scope: String,
    #[diesel(sql_type = Nullable<Text>)]
    dirty_from: Option<String>,
    #[diesel(sql_type = BigInt)]
    version: i64,
}

#[derive(QueryableByName)]
struct LastValuedRow {
    #[diesel(sql_type = Text)]
    account_id: String,
    #[diesel(sql_type = Text)]
    day: String,
}

fn parse_day(raw: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok()
}

/// Disposal rows must reference a stored lot and a stored activity
/// (`lot_disposals` foreign keys). A row that cannot is dropped with a
/// warning rather than rolling back the whole account.
fn referentially_valid_disposals(
    conn: &mut SqliteConnection,
    disposals: Vec<LotDisposalDB>,
    account: &str,
) -> Result<Vec<LotDisposalDB>> {
    use crate::schema::activities::dsl as a;
    use crate::schema::lots::dsl as l;
    let lot_ids: HashSet<String> = l::lots
        .filter(l::account_id.eq(account))
        .select(l::id)
        .load::<String>(conn)
        .map_err(StorageError::from)?
        .into_iter()
        .collect();
    let wanted: Vec<String> = disposals
        .iter()
        .map(|d| d.disposal_activity_id.clone())
        .collect();
    let activity_ids: HashSet<String> = a::activities
        .filter(a::id.eq_any(&wanted))
        .select(a::id)
        .load::<String>(conn)
        .map_err(StorageError::from)?
        .into_iter()
        .collect();
    Ok(disposals
        .into_iter()
        .filter(|d| {
            let valid =
                lot_ids.contains(&d.lot_id) && activity_ids.contains(&d.disposal_activity_id);
            if !valid {
                log::warn!(
                    "Dropping lot disposal {} for account {}: lot {} or activity {} is not stored",
                    d.id,
                    account,
                    d.lot_id,
                    d.disposal_activity_id
                );
            }
            valid
        })
        .collect())
}

struct PreparedWindow {
    account: String,
    start: String,
    end: Option<String>,
    snapshots: Option<Vec<AccountStateSnapshotDB>>,
    positions: Vec<(String, HashMap<String, Position>)>,
    valuations: Vec<DailyAccountValuationDB>,
}

fn write_window_rows(conn: &mut SqliteConnection, window: PreparedWindow) -> Result<()> {
    let PreparedWindow {
        account,
        start,
        end,
        snapshots,
        positions,
        valuations,
    } = window;
    if let Some(snapshots) = snapshots {
        use crate::schema::holdings_snapshots::dsl as hs;
        // Only calculated rows are the projection's; manual and imported
        // snapshots are the user's (REG-0913).
        let target = hs::holdings_snapshots
            .filter(hs::account_id.eq(&account))
            .filter(hs::source.eq(SOURCE_CALCULATED))
            .filter(hs::snapshot_date.ge(&start));
        match &end {
            Some(end) => diesel::delete(target.filter(hs::snapshot_date.le(end)))
                .execute(conn)
                .map_err(StorageError::from)?,
            None => diesel::delete(target)
                .execute(conn)
                .map_err(StorageError::from)?,
        };
        for chunk in snapshots.chunks(1000) {
            diesel::replace_into(hs::holdings_snapshots)
                .values(chunk)
                .execute(conn)
                .map_err(StorageError::from)?;
        }
        let positions: Vec<(&str, &HashMap<String, Position>)> = positions
            .iter()
            .map(|(snapshot_id, positions)| (snapshot_id.as_str(), positions))
            .collect();
        SnapshotRepository::write_snapshots_positions(conn, &positions)?;
    }
    use crate::schema::daily_account_valuation::dsl as v;
    let target = v::daily_account_valuation
        .filter(v::account_id.eq(&account))
        .filter(v::valuation_date.ge(&start));
    match &end {
        Some(end) => diesel::delete(target.filter(v::valuation_date.le(end)))
            .execute(conn)
            .map_err(StorageError::from)?,
        None => diesel::delete(target)
            .execute(conn)
            .map_err(StorageError::from)?,
    };
    for chunk in valuations.chunks(1000) {
        diesel::replace_into(v::daily_account_valuation)
            .values(chunk)
            .execute(conn)
            .map_err(StorageError::from)?;
    }
    Ok(())
}

#[async_trait]
impl ProjectionStoreTrait for ProjectionStore {
    fn pending_markers(&self) -> Result<Vec<ProjectionMarker>> {
        let mut conn = get_connection(&self.pool)?;
        let rows: Vec<MarkerRow> = diesel::sql_query(
            "SELECT scope, dirty_from, version FROM projection_state WHERE dirty_from IS NOT NULL",
        )
        .load(&mut conn)
        .map_err(StorageError::from)?;
        Ok(rows
            .into_iter()
            .map(|row| ProjectionMarker {
                scope: MarkerScope::parse(&row.scope),
                // A day the triggers could not date means "everything".
                dirty_from: row
                    .dirty_from
                    .as_deref()
                    .and_then(parse_day)
                    .unwrap_or(GENESIS),
                version: row.version,
            })
            .collect())
    }

    fn last_valued_days(&self) -> Result<HashMap<String, NaiveDate>> {
        let mut conn = get_connection(&self.pool)?;
        let rows: Vec<LastValuedRow> = diesel::sql_query(
            "SELECT account_id, MAX(valuation_date) AS day FROM daily_account_valuation \
             GROUP BY account_id",
        )
        .load(&mut conn)
        .map_err(StorageError::from)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| Some((row.account_id, parse_day(&row.day)?)))
            .collect())
    }

    fn rejections(&self, account_ids: &[String]) -> Result<Vec<RejectedActivity>> {
        use crate::schema::projection_state::dsl as ps;
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = get_connection(&self.pool)?;
        let rows: Vec<String> = ps::projection_state
            .filter(ps::scope.eq_any(account_ids))
            .select(ps::rejections)
            .load(&mut conn)
            .map_err(StorageError::from)?;
        Ok(rows
            .iter()
            .flat_map(|row| serde_json::from_str::<Vec<RejectedActivity>>(row).unwrap_or_default())
            .collect())
    }

    async fn write_window(&self, rows: Vec<WindowRows>) -> Result<()> {
        let prepared: Vec<PreparedWindow> = rows
            .into_iter()
            .map(|rows| PreparedWindow {
                account: rows.account_id,
                start: rows.start.to_string(),
                end: rows.end.map(|d| d.to_string()),
                positions: rows
                    .snapshots
                    .as_ref()
                    .map(|snapshots| {
                        snapshots
                            .iter()
                            .map(|s| (s.id.clone(), s.positions.clone()))
                            .collect()
                    })
                    .unwrap_or_default(),
                snapshots: rows.snapshots.map(|snapshots| {
                    snapshots
                        .into_iter()
                        .map(AccountStateSnapshotDB::from)
                        .collect()
                }),
                valuations: rows
                    .valuations
                    .into_iter()
                    .map(DailyAccountValuationDB::from)
                    .collect(),
            })
            .collect();
        self.writer
            .exec(move |conn: &mut SqliteConnection| {
                for window in prepared {
                    write_window_rows(conn, window)?;
                }
                Ok(())
            })
            .await
    }

    async fn complete_run(&self, completion: RunCompletion) -> Result<()> {
        let books: Vec<(String, String, Vec<LotRecordDB>, Vec<LotDisposalDB>)> = completion
            .lot_books
            .iter()
            .map(|book| {
                (
                    book.account_id.clone(),
                    book.since.to_string(),
                    book.lots.iter().map(LotRecordDB::from).collect(),
                    book.disposals.iter().map(LotDisposalDB::from).collect(),
                )
            })
            .collect();
        let rejections: Vec<(String, String)> = completion
            .rejections
            .iter()
            .map(|(account, rejected)| {
                (
                    account.clone(),
                    serde_json::to_string(rejected).unwrap_or_else(|_| "[]".to_string()),
                )
            })
            .collect();
        let consumed = completion.consumed;
        self.writer
            .exec(move |conn: &mut SqliteConnection| {
                for (account, since, lots, disposals) in books {
                    {
                        use crate::schema::lots::dsl as l;
                        // Lots closed before `since` are history the run did
                        // not touch; open lots are re-emitted whole. The book's
                        // lots are updated in place: deleting one would
                        // cascade away its disposals, including the ones dated
                        // before `since` that the run does not re-emit.
                        let normalized = filter_and_normalize_lots(conn, lots, &account)?;
                        let kept: Vec<&str> = normalized.iter().map(|lot| lot.id()).collect();
                        diesel::delete(
                            l::lots
                                .filter(l::account_id.eq(&account))
                                .filter(l::is_closed.eq(0).or(l::close_date.ge(since.clone())))
                                .filter(l::id.ne_all(&kept)),
                        )
                        .execute(conn)
                        .map_err(StorageError::from)?;
                        // A lot the book re-emits unchanged is left as stored.
                        let stored: HashMap<String, LotRecordDB> = l::lots
                            .filter(l::account_id.eq(&account))
                            .filter(l::is_closed.eq(0).or(l::close_date.ge(since.clone())))
                            .select(LotRecordDB::as_select())
                            .load::<LotRecordDB>(conn)
                            .map_err(StorageError::from)?
                            .into_iter()
                            .map(|lot| (lot.id().to_string(), lot))
                            .collect();
                        for lot in &normalized {
                            if stored.get(lot.id()).is_some_and(|row| row.same_lot(lot)) {
                                continue;
                            }
                            diesel::insert_into(l::lots)
                                .values(lot)
                                .on_conflict(l::id)
                                .do_update()
                                .set(lot)
                                .execute(conn)
                                .map_err(StorageError::from)?;
                        }
                    }
                    use crate::schema::lot_disposals::dsl as d;
                    diesel::delete(
                        d::lot_disposals
                            .filter(d::account_id.eq(&account))
                            .filter(d::disposal_date.ge(&since)),
                    )
                    .execute(conn)
                    .map_err(StorageError::from)?;
                    let disposals = referentially_valid_disposals(conn, disposals, &account)?;
                    if !disposals.is_empty() {
                        diesel::insert_into(d::lot_disposals)
                            .values(&disposals)
                            .execute(conn)
                            .map_err(StorageError::from)?;
                    }
                }
                for (account, rejected) in rejections {
                    diesel::sql_query(
                        "INSERT INTO projection_state (scope, dirty_from, version, rejections) \
                         VALUES (?, NULL, 0, ?) \
                         ON CONFLICT (scope) DO UPDATE SET rejections = excluded.rejections",
                    )
                    .bind::<Text, _>(&account)
                    .bind::<Text, _>(&rejected)
                    .execute(conn)
                    .map_err(StorageError::from)?;
                }
                for marker in consumed {
                    // A write since the run read the marker bumped its version:
                    // that change is not in the run, so the marker stays.
                    let statement = match marker.scope {
                        MarkerScope::Account(_) => {
                            "UPDATE projection_state SET dirty_from = NULL \
                             WHERE scope = ? AND version = ?"
                        }
                        _ => "DELETE FROM projection_state WHERE scope = ? AND version = ?",
                    };
                    diesel::sql_query(statement)
                        .bind::<Text, _>(marker.scope.key())
                        .bind::<BigInt, _>(marker.version)
                        .execute(conn)
                        .map_err(StorageError::from)?;
                }
                Ok(())
            })
            .await
    }

    async fn invalidate(&self, scope: MarkerScope, from: NaiveDate) -> Result<()> {
        let key = scope.key();
        let day = from.to_string();
        self.writer
            .exec(move |conn: &mut SqliteConnection| {
                diesel::sql_query(INVALIDATE)
                    .bind::<Text, _>(&key)
                    .bind::<Text, _>(&day)
                    .execute(conn)
                    .map_err(StorageError::from)?;
                Ok(())
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::{NaiveDate, Utc};
    use rust_decimal::Decimal;
    use tempfile::tempdir;
    use wealthfolio_core::lots::{LotDisposal, LotRecord, LotRepositoryTrait};
    use wealthfolio_core::portfolio::economic_events::BasisStatus;
    use wealthfolio_core::portfolio::projection::LotBook;
    use wealthfolio_core::portfolio::snapshot::{AccountStateSnapshot, Position, SnapshotSource};
    use wealthfolio_core::portfolio::valuation::{
        DailyAccountValuation, ExternalFlowSource, ValuationRepositoryTrait, ValuationStatus,
    };

    use super::*;
    use crate::db::{create_pool, get_connection, run_migrations, write_actor::spawn_writer};
    use crate::lots::LotsRepository;
    use crate::portfolio::valuation::ValuationRepository;
    struct Db {
        pool: Arc<DbPool>,
        writer: WriteHandle,
        _dir: tempfile::TempDir,
    }

    fn setup() -> Db {
        std::env::set_var("CONNECT_API_URL", "http://test.local");
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db").to_string_lossy().to_string();
        run_migrations(&db_path).unwrap();
        let pool = create_pool(&db_path).unwrap();
        let writer = spawn_writer((*pool).clone()).unwrap();
        let mut conn = get_connection(&pool).unwrap();
        diesel::sql_query(
            "INSERT INTO accounts (id, name, account_type, currency, is_default, is_active, \
             created_at, updated_at, tracking_mode, is_archived) \
             VALUES ('acc1', 'Test', 'SECURITIES', 'USD', 0, 1, datetime('now'), datetime('now'), 'TRANSACTIONS', 0)",
        )
        .execute(&mut conn)
        .unwrap();
        diesel::sql_query(
            "INSERT INTO assets (id, kind, is_active, quote_mode, quote_ccy, created_at, updated_at) \
             VALUES ('AAPL', 'INVESTMENT', 1, 'MARKET', 'USD', datetime('now'), datetime('now'))",
        )
        .execute(&mut conn)
        .unwrap();
        // Disposals reference the disposing activity row.
        diesel::sql_query(
            "INSERT INTO activities (id, account_id, activity_type, status, activity_date, currency, \
             is_user_modified, needs_review, created_at, updated_at) \
             VALUES ('sell-1', 'acc1', 'SELL', 'POSTED', '2025-01-05T00:00:00Z', 'USD', 0, 0, \
             datetime('now'), datetime('now'))",
        )
        .execute(&mut conn)
        .unwrap();
        Db {
            pool,
            writer,
            _dir: dir,
        }
    }

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2025, 1, day).unwrap()
    }

    fn snapshot(day: u32, quantity: &str) -> AccountStateSnapshot {
        let mut positions = HashMap::new();
        positions.insert(
            "AAPL".to_string(),
            Position {
                id: "acc1-AAPL".to_string(),
                account_id: "acc1".to_string(),
                asset_id: "AAPL".to_string(),
                quantity: quantity.parse().unwrap(),
                average_cost: Decimal::from(100),
                total_cost_basis: Decimal::from(100) * quantity.parse::<Decimal>().unwrap(),
                currency: "USD".to_string(),
                ..Position::default()
            },
        );
        AccountStateSnapshot {
            id: AccountStateSnapshot::stable_id("acc1", date(day)),
            account_id: "acc1".to_string(),
            snapshot_date: date(day),
            currency: "USD".to_string(),
            positions,
            cash_balances: HashMap::from([("USD".to_string(), Decimal::from(500))]),
            cost_basis: Decimal::from(1000),
            net_contribution: Decimal::from(1500),
            net_contribution_base: Decimal::from(1500),
            cash_total_account_currency: Decimal::from(500),
            cash_total_base_currency: Decimal::from(500),
            calculated_at: Utc::now().naive_utc(),
            source: SnapshotSource::Calculated,
        }
    }

    fn lot(id: &str) -> LotRecord {
        LotRecord {
            id: id.to_string(),
            account_id: "acc1".to_string(),
            asset_id: "AAPL".to_string(),
            open_date: "2025-01-02".to_string(),
            open_activity_id: None,
            original_quantity: "10".to_string(),
            remaining_quantity: "10".to_string(),
            cost_per_unit: "100".to_string(),
            original_cost_basis: "1000".to_string(),
            remaining_cost_basis: "1000".to_string(),
            original_cost_basis_base: "1000".to_string(),
            remaining_cost_basis_base: "1000".to_string(),
            fee_allocated: "0".to_string(),
            fee_allocated_base: "0".to_string(),
            tax_allocated: "0".to_string(),
            tax_allocated_base: "0".to_string(),
            currency: "USD".to_string(),
            base_currency: "USD".to_string(),
            fx_rate_to_base: "1".to_string(),
            fx_rate_to_account: None,
            account_currency: None,
            cost_basis_method: "FIFO".to_string(),
            split_ratio: "1".to_string(),
            is_closed: false,
            close_date: None,
            close_activity_id: None,
            created_at: "2025-01-02T00:00:00.000Z".to_string(),
            updated_at: "2025-01-02T00:00:00.000Z".to_string(),
        }
    }

    fn disposal(id: &str) -> LotDisposal {
        LotDisposal {
            id: id.to_string(),
            lot_id: "lot-1".to_string(),
            account_id: "acc1".to_string(),
            asset_id: "AAPL".to_string(),
            disposal_activity_id: "sell-1".to_string(),
            disposal_date: "2025-01-05".to_string(),
            quantity: "4".to_string(),
            proceeds: "480".to_string(),
            cost_basis: "400".to_string(),
            realized_pnl: "80".to_string(),
            proceeds_base: "480".to_string(),
            cost_basis_base: "400".to_string(),
            realized_pnl_base: "80".to_string(),
            currency: "USD".to_string(),
            base_currency: "USD".to_string(),
            fx_rate_to_base: "1".to_string(),
            cost_basis_method: "FIFO".to_string(),
            created_at: "2025-01-05T00:00:00.000Z".to_string(),
        }
    }

    fn valuation(day: u32, total: i64) -> DailyAccountValuation {
        DailyAccountValuation {
            id: format!("acc1_{}", date(day)),
            account_id: "acc1".to_string(),
            valuation_date: date(day),
            account_currency: "USD".to_string(),
            base_currency: "USD".to_string(),
            fx_rate_to_base: Decimal::ONE,
            cash_balance: Decimal::from(500),
            investment_market_value: Decimal::from(total - 500),
            total_value: Decimal::from(total),
            cost_basis: Decimal::from(1000),
            book_basis: Decimal::from(1500),
            net_contribution: Decimal::from(1500),
            cash_balance_base: Decimal::from(500),
            investment_market_value_base: Decimal::from(total - 500),
            total_value_base: Decimal::from(total),
            cost_basis_base: Decimal::from(1000),
            book_basis_base: Decimal::from(1500),
            net_contribution_base: Decimal::from(1500),
            external_inflow_base: Decimal::ZERO,
            external_outflow_base: Decimal::ZERO,
            external_flow_source: ExternalFlowSource::NoFlow,
            performance_eligible_value_base: Decimal::from(total),
            value_status: ValuationStatus::Complete,
            basis_status: BasisStatus::Complete,
            calculated_at: Utc::now(),
        }
    }

    fn sql(db: &Db, statement: &str) {
        let mut conn = get_connection(&db.pool).unwrap();
        diesel::sql_query(statement).execute(&mut conn).unwrap();
    }

    /// (dirty day, version) of a scope, `None` when no row.
    fn marker(db: &Db, scope: &str) -> Option<(Option<String>, i64)> {
        let mut conn = get_connection(&db.pool).unwrap();
        let rows: Vec<MarkerRow> = diesel::sql_query(
            "SELECT scope, dirty_from, version FROM projection_state WHERE scope = ?",
        )
        .bind::<Text, _>(scope)
        .load(&mut conn)
        .unwrap();
        rows.into_iter()
            .next()
            .map(|row| (row.dirty_from, row.version))
    }

    fn dirty(db: &Db, scope: &str) -> Option<String> {
        marker(db, scope).and_then(|(day, _)| day)
    }

    fn window(start: u32, end: Option<u32>, days: &[u32]) -> WindowRows {
        WindowRows {
            account_id: "acc1".to_string(),
            start: date(start),
            end: end.map(date),
            snapshots: Some(days.iter().map(|d| snapshot(*d, "10")).collect()),
            valuations: days
                .iter()
                .map(|d| valuation(*d, 1500 + *d as i64))
                .collect(),
        }
    }

    #[tokio::test]
    async fn the_first_run_rebuilds_everything() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let markers = store.pending_markers().unwrap();
        assert!(markers
            .iter()
            .any(|m| m.scope == MarkerScope::All && m.dirty_from == GENESIS));
        // The fixture's sell-1 (2025-01-05) marked its account a day early.
        assert!(markers.iter().any(
            |m| m.scope == MarkerScope::Account("acc1".to_string()) && m.dirty_from == date(4)
        ));
    }

    #[tokio::test]
    async fn activity_writes_mark_their_account_and_transfer_partners() {
        let db = setup();
        sql(
            &db,
            "INSERT INTO accounts (id, name, account_type, currency, is_default, is_active, \
             created_at, updated_at, tracking_mode, is_archived) \
             VALUES ('acc2', 'Other', 'SECURITIES', 'USD', 0, 1, datetime('now'), datetime('now'), 'TRANSACTIONS', 0)",
        );
        let insert = |id: &str, account: &str, day: &str, group: &str| {
            format!(
                "INSERT INTO activities (id, account_id, activity_type, status, activity_date, currency, \
                 source_group_id, is_user_modified, needs_review, created_at, updated_at) \
                 VALUES ('{id}', '{account}', 'TRANSFER_OUT', 'POSTED', '{day}', 'USD', '{group}', 0, 0, \
                 datetime('now'), datetime('now'))"
            )
        };
        sql(&db, &insert("out-1", "acc1", "2025-01-20T15:00:00Z", "g1"));
        assert_eq!(dirty(&db, "acc2"), None);
        // The partner leg marks both accounts, each from its own leg's day.
        sql(&db, &insert("in-1", "acc2", "2025-01-21T15:00:00Z", "g1"));
        assert_eq!(dirty(&db, "acc2").as_deref(), Some("2025-01-20"));
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("2025-01-04"));

        let (_, before) = marker(&db, "acc1").unwrap();
        sql(&db, "UPDATE activities SET notes = 'x' WHERE id = 'in-1'");
        let (_, after) = marker(&db, "acc1").unwrap();
        assert!(after > before, "every write bumps the partner's version");

        // Backdating lowers the day; deleting marks the partner again.
        sql(
            &db,
            "UPDATE activities SET activity_date = '2024-12-01T10:00:00Z' WHERE id = 'in-1'",
        );
        assert_eq!(dirty(&db, "acc2").as_deref(), Some("2024-11-30"));
        sql(&db, "DELETE FROM activities WHERE id = 'out-1'");
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("2025-01-04"));
    }

    #[tokio::test]
    async fn prices_and_fx_rates_mark_their_asset() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        store
            .complete_run(RunCompletion {
                consumed: store.pending_markers().unwrap(),
                ..RunCompletion::default()
            })
            .await
            .unwrap();
        assert!(store.pending_markers().unwrap().is_empty());

        sql(
            &db,
            "INSERT INTO quotes (id, asset_id, day, source, close, currency, created_at, timestamp) \
             VALUES ('q1', 'AAPL', '2025-01-03', 'YAHOO', '100', 'USD', datetime('now'), '2025-01-03T00:00:00Z')",
        );
        assert_eq!(dirty(&db, "q:AAPL").as_deref(), Some("2025-01-03"));
        assert_eq!(dirty(&db, "@all"), None);

        sql(
            &db,
            "INSERT INTO assets (id, kind, is_active, quote_mode, quote_ccy, created_at, updated_at) \
             VALUES ('FX:EURUSD', 'FX', 1, 'MARKET', 'USD', datetime('now'), datetime('now'))",
        );
        sql(
            &db,
            "INSERT INTO quotes (id, asset_id, day, source, close, currency, created_at, timestamp) \
             VALUES ('q2', 'FX:EURUSD', '2025-01-02', 'YAHOO', '1.1', 'USD', datetime('now'), '2025-01-02T00:00:00Z')",
        );
        assert_eq!(dirty(&db, "fx:FX:EURUSD").as_deref(), Some("2025-01-02"));
        assert_eq!(dirty(&db, "q:FX:EURUSD"), None);
        assert_eq!(dirty(&db, "@all"), None);
        assert!(store
            .pending_markers()
            .unwrap()
            .iter()
            .any(|m| m.scope == MarkerScope::Fx("FX:EURUSD".to_string())));
    }

    #[tokio::test]
    async fn only_engine_facts_of_assets_accounts_and_settings_mark() {
        let db = setup();
        sql(&db, "UPDATE assets SET name = 'Apple' WHERE id = 'AAPL'");
        assert_eq!(
            marker(&db, "a:AAPL"),
            None,
            "a profile edit changes nothing"
        );
        sql(
            &db,
            "UPDATE assets SET metadata = '{\"contractMultiplier\": 10}' WHERE id = 'AAPL'",
        );
        assert_eq!(dirty(&db, "a:AAPL").as_deref(), Some("0001-01-01"));

        sql(
            &db,
            "UPDATE accounts SET name = 'Renamed' WHERE id = 'acc1'",
        );
        sql(
            &db,
            "UPDATE accounts SET meta = '{\"broker\": {\"lastSync\": \"x\"}}' WHERE id = 'acc1'",
        );
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("2025-01-04"));
        sql(
            &db,
            "UPDATE accounts SET meta = '{\"accounting\": {\"costBasisMethod\": \"LIFO\"}}' \
             WHERE id = 'acc1'",
        );
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("0001-01-01"));
        sql(
            &db,
            "UPDATE projection_state SET dirty_from = '2025-01-04' WHERE scope = 'acc1'",
        );
        sql(
            &db,
            "UPDATE accounts SET currency = 'CAD' WHERE id = 'acc1'",
        );
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("0001-01-01"));

        let (_, before) = marker(&db, "@all").unwrap();
        sql(
            &db,
            "INSERT INTO app_settings (setting_key, setting_value) VALUES ('theme', 'dark')",
        );
        assert_eq!(marker(&db, "@all").unwrap().1, before);
        sql(
            &db,
            "INSERT INTO app_settings (setting_key, setting_value) VALUES ('base_currency', 'EUR') \
             ON CONFLICT (setting_key) DO UPDATE SET setting_value = excluded.setting_value",
        );
        assert!(marker(&db, "@all").unwrap().1 > before);
    }

    #[tokio::test]
    async fn projection_writes_do_not_mark_but_manual_snapshots_do() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let snapshots = SnapshotRepository::new(db.pool.clone(), db.writer.clone());
        let before = marker(&db, "acc1").unwrap();
        store
            .write_window(vec![window(2, None, &[2, 3])])
            .await
            .unwrap();
        assert_eq!(marker(&db, "acc1").unwrap(), before);

        let mut manual = snapshot(1, "5");
        manual.id = "acc1_manual".to_string();
        manual.source = SnapshotSource::ManualEntry;
        snapshots.save_snapshots(&[manual]).await.unwrap();
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("2025-01-01"));
    }

    #[tokio::test]
    async fn a_window_replaces_only_its_own_rows() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let snapshots = SnapshotRepository::new(db.pool.clone(), db.writer.clone());
        let valuations = ValuationRepository::new(db.pool.clone(), db.writer.clone());
        let mut manual = snapshot(3, "10");
        manual.id = "acc1_manual".to_string();
        manual.source = SnapshotSource::ManualEntry;
        snapshots.save_snapshots(&[manual]).await.unwrap();

        store
            .write_window(vec![window(2, None, &[2, 3, 4, 5, 6])])
            .await
            .unwrap();
        // A later bounded window, then the open-ended tail from day 7.
        store
            .write_window(vec![window(4, Some(5), &[4])])
            .await
            .unwrap();
        store
            .write_window(vec![window(7, None, &[])])
            .await
            .unwrap();

        let calculated: Vec<NaiveDate> = snapshots
            .get_snapshots_by_account("acc1", None, None)
            .unwrap()
            .iter()
            .filter(|s| s.source == SnapshotSource::Calculated)
            .map(|s| s.snapshot_date)
            .collect();
        assert_eq!(calculated, vec![date(2), date(3), date(4), date(6)]);
        let manual_rows = snapshots
            .get_snapshots_by_account("acc1", None, None)
            .unwrap()
            .into_iter()
            .filter(|s| s.source == SnapshotSource::ManualEntry)
            .count();
        assert_eq!(manual_rows, 1, "manual snapshots survive every rewrite");
        let valued: Vec<NaiveDate> = valuations
            .get_historical_valuations("acc1", None, None)
            .unwrap()
            .iter()
            .map(|v| v.valuation_date)
            .collect();
        assert_eq!(valued, vec![date(2), date(3), date(4), date(6)]);
        assert_eq!(
            store.last_valued_days().unwrap().get("acc1"),
            Some(&date(6))
        );
    }

    #[tokio::test]
    async fn a_window_writes_the_positions_of_every_snapshot_it_keeps() {
        #[derive(QueryableByName)]
        struct PositionRow {
            #[diesel(sql_type = diesel::sql_types::Text)]
            snapshot_id: String,
            #[diesel(sql_type = diesel::sql_types::Text)]
            quantity: String,
        }
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let positions = || -> Vec<(String, String)> {
            let mut conn = get_connection(&db.pool).unwrap();
            diesel::sql_query(
                "SELECT snapshot_id, quantity FROM snapshot_positions ORDER BY snapshot_id",
            )
            .load::<PositionRow>(&mut conn)
            .unwrap()
            .into_iter()
            .map(|row| (row.snapshot_id, row.quantity))
            .collect()
        };
        let id = |day: u32| AccountStateSnapshot::stable_id("acc1", date(day));

        store
            .write_window(vec![window(2, None, &[2, 3, 4])])
            .await
            .unwrap();
        let mut expected = vec![
            (id(2), "10".to_string()),
            (id(3), "10".to_string()),
            (id(4), "10".to_string()),
        ];
        expected.sort();
        assert_eq!(positions(), expected);

        // Rewriting from day 3: day 3 gets new positions, day 4's go with it.
        let mut rewrite = window(3, None, &[]);
        rewrite.snapshots = Some(vec![snapshot(3, "7")]);
        store.write_window(vec![rewrite]).await.unwrap();
        let mut expected = vec![(id(2), "10".to_string()), (id(3), "7".to_string())];
        expected.sort();
        assert_eq!(positions(), expected);
    }

    #[tokio::test]
    async fn a_lot_book_rewrites_only_the_lots_that_changed() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let lots = LotsRepository::new(db.pool.clone(), db.writer.clone());
        let commit = |book: Vec<LotRecord>| RunCompletion {
            lot_books: vec![LotBook {
                account_id: "acc1".to_string(),
                since: GENESIS,
                lots: book,
                disposals: Vec::new(),
            }],
            ..RunCompletion::default()
        };
        store
            .complete_run(commit(vec![lot("lot-a"), lot("lot-b")]))
            .await
            .unwrap();

        // The next run re-emits lot-a unchanged and lot-b partly sold, both
        // stamped with the run's time.
        let later = "2026-01-01T00:00:00.000Z".to_string();
        let mut same = lot("lot-a");
        same.updated_at.clone_from(&later);
        let mut sold = lot("lot-b");
        sold.remaining_quantity = "4".to_string();
        sold.updated_at.clone_from(&later);
        store.complete_run(commit(vec![same, sold])).await.unwrap();

        let stored = lots.get_all_lots_for_account("acc1").await.unwrap();
        let find = |id: &str| stored.iter().find(|l| l.id == id).expect("lot").clone();
        assert_eq!(find("lot-a").updated_at, "2025-01-02T00:00:00.000Z");
        assert_eq!(find("lot-b").remaining_quantity, "4");
        assert_eq!(find("lot-b").updated_at, later);
    }

    #[tokio::test]
    async fn completing_a_run_commits_the_lot_book_and_clears_what_it_saw() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let lots = LotsRepository::new(db.pool.clone(), db.writer.clone());

        let mut closed = lot("lot-closed");
        closed.is_closed = true;
        closed.close_date = Some("2025-01-03".to_string());
        closed.remaining_quantity = "0".to_string();
        let mut early = disposal("d-early");
        early.lot_id = "lot-closed".to_string();
        early.disposal_date = "2025-01-03".to_string();
        let seen = store.pending_markers().unwrap();
        store
            .complete_run(RunCompletion {
                lot_books: vec![LotBook {
                    account_id: "acc1".to_string(),
                    since: GENESIS,
                    lots: vec![closed, lot("lot-open")],
                    disposals: vec![early],
                }],
                rejections: vec![(
                    "acc1".to_string(),
                    vec![RejectedActivity {
                        activity_id: "sell-1".to_string(),
                        message: "rejected".to_string(),
                    }],
                )],
                consumed: seen.clone(),
            })
            .await
            .unwrap();
        assert!(store.pending_markers().unwrap().is_empty());
        assert_eq!(
            store.rejections(&["acc1".to_string()]).unwrap(),
            vec![RejectedActivity {
                activity_id: "sell-1".to_string(),
                message: "rejected".to_string(),
            }]
        );

        // From day 4: the lot closed before stays, open lots and later
        // disposals are replaced, an orphan disposal is dropped.
        let mut late = disposal("d-late");
        late.lot_id = "lot-open".to_string();
        late.disposal_date = "2025-01-05".to_string();
        let mut orphan = disposal("d-orphan");
        orphan.disposal_activity_id = "drip-1:buy".to_string();
        sql(&db, "UPDATE activities SET notes = 'x' WHERE id = 'sell-1'");
        let stale = store.pending_markers().unwrap();
        sql(&db, "UPDATE activities SET notes = 'y' WHERE id = 'sell-1'");
        store
            .complete_run(RunCompletion {
                lot_books: vec![LotBook {
                    account_id: "acc1".to_string(),
                    since: date(4),
                    lots: vec![lot("lot-open")],
                    disposals: vec![late, orphan],
                }],
                rejections: vec![("acc1".to_string(), Vec::new())],
                consumed: stale,
            })
            .await
            .unwrap();
        let mut lot_ids: Vec<String> = lots
            .get_all_lots_for_account("acc1")
            .await
            .unwrap()
            .into_iter()
            .map(|l| l.id)
            .collect();
        lot_ids.sort();
        assert_eq!(lot_ids, vec!["lot-closed", "lot-open"]);
        let mut disposal_ids: Vec<String> = lots
            .get_lot_disposals_for_account("acc1")
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.id)
            .collect();
        disposal_ids.sort();
        assert_eq!(disposal_ids, vec!["d-early", "d-late"]);
        assert!(store.rejections(&["acc1".to_string()]).unwrap().is_empty());
        assert_eq!(
            dirty(&db, "acc1").as_deref(),
            Some("2025-01-04"),
            "a write after the run read the marker keeps it"
        );
    }

    #[tokio::test]
    async fn a_later_lot_book_keeps_earlier_disposals_of_open_lots() {
        // lot-open was partly sold on day 3; a run from day 4 re-emits the lot
        // and must not take the day-3 disposal with it (lot_disposals cascade
        // on lot deletion).
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        let lots = LotsRepository::new(db.pool.clone(), db.writer.clone());
        let mut partly_sold = disposal("d-partial");
        partly_sold.lot_id = "lot-open".to_string();
        partly_sold.disposal_date = "2025-01-03".to_string();
        let book = |since, disposals| RunCompletion {
            lot_books: vec![LotBook {
                account_id: "acc1".to_string(),
                since,
                lots: vec![lot("lot-open")],
                disposals,
            }],
            ..RunCompletion::default()
        };
        store
            .complete_run(book(GENESIS, vec![partly_sold]))
            .await
            .unwrap();
        store.complete_run(book(date(4), Vec::new())).await.unwrap();
        let kept: Vec<String> = lots
            .get_lot_disposals_for_account("acc1")
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.id)
            .collect();
        assert_eq!(kept, vec!["d-partial"]);

        // A lot the new book no longer has goes, with its disposals.
        store
            .complete_run(RunCompletion {
                lot_books: vec![LotBook {
                    account_id: "acc1".to_string(),
                    since: date(2),
                    lots: Vec::new(),
                    disposals: Vec::new(),
                }],
                ..RunCompletion::default()
            })
            .await
            .unwrap();
        assert!(lots
            .get_all_lots_for_account("acc1")
            .await
            .unwrap()
            .is_empty());
        assert!(lots
            .get_lot_disposals_for_account("acc1")
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn invalidate_lowers_and_bumps() {
        let db = setup();
        let store = ProjectionStore::new(db.pool.clone(), db.writer.clone());
        store
            .invalidate(MarkerScope::Account("acc1".to_string()), date(2))
            .await
            .unwrap();
        assert_eq!(dirty(&db, "acc1").as_deref(), Some("2025-01-02"));
        let (_, version) = marker(&db, "acc1").unwrap();
        store
            .invalidate(MarkerScope::Account("acc1".to_string()), date(9))
            .await
            .unwrap();
        assert_eq!(
            marker(&db, "acc1").unwrap(),
            (Some("2025-01-02".to_string()), version + 1)
        );
    }
}
