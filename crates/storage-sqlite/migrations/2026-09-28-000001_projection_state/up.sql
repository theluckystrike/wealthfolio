-- What the stored projection (calculated snapshots, lots, disposals and daily
-- valuations) no longer reflects, recorded by triggers in the same transaction
-- as the fact that changed, so a crash or a killed app never loses it.
--
-- scope       an account id: refold the account from dirty_from
--             'a:<asset>': the asset's facts changed, refold its holders
--             'q:<asset>': its prices changed, revalue its holders
--             'fx:<asset>': an FX rate changed, revalue every account (and refold
--             those with activity) from the pair's previous observation
--             '@all': policy changed, refold every account
-- dirty_from  first local day to recompute; NULL once the projection is clean
-- version     bumped by every write, so a job only clears what it has seen
-- rejections  account rows: activities the last run rejected (JSON)
CREATE TABLE projection_state (
    scope TEXT PRIMARY KEY NOT NULL,
    dirty_from TEXT,
    version INTEGER NOT NULL DEFAULT 0,
    rejections TEXT NOT NULL DEFAULT '[]'
);

-- Nothing is projected yet: the first run rebuilds every account.
INSERT INTO projection_state (scope, dirty_from, version) VALUES ('@all', '0001-01-01', 1);

-- An activity's local business day is within a day of its UTC date, so one day
-- earlier is always early enough. Its transfer partners' legs (same
-- source_group_id) are dirty from their own dates.
CREATE TRIGGER projection_activity_insert AFTER INSERT ON activities
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (NEW.account_id, coalesce(date(NEW.activity_date, '-1 day'), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT DISTINCT p.account_id, coalesce(date(p.activity_date, '-1 day'), '0001-01-01'), 1
    FROM activities p
    WHERE NEW.source_group_id IS NOT NULL AND p.source_group_id = NEW.source_group_id
      AND p.account_id <> NEW.account_id
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_activity_update AFTER UPDATE ON activities
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (OLD.account_id, coalesce(date(OLD.activity_date, '-1 day'), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (NEW.account_id, coalesce(date(NEW.activity_date, '-1 day'), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT DISTINCT p.account_id, coalesce(date(p.activity_date, '-1 day'), '0001-01-01'), 1
    FROM activities p
    WHERE p.source_group_id IS NOT NULL
      AND p.source_group_id IN (OLD.source_group_id, NEW.source_group_id)
      AND p.id <> NEW.id
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_activity_delete AFTER DELETE ON activities
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (OLD.account_id, coalesce(date(OLD.activity_date, '-1 day'), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT DISTINCT p.account_id, coalesce(date(p.activity_date, '-1 day'), '0001-01-01'), 1
    FROM activities p
    WHERE OLD.source_group_id IS NOT NULL AND p.source_group_id = OLD.source_group_id
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

-- Account facts the kernel reads (currency, type, tracking mode, archived, and
-- the accounting settings under meta.accounting): the whole history may
-- change. Other meta keys (broker details, timestamps) do not.
CREATE TRIGGER projection_account_update AFTER UPDATE OF currency, account_type, tracking_mode, is_archived, meta ON accounts
WHEN OLD.currency IS NOT NEW.currency
  OR OLD.account_type IS NOT NEW.account_type
  OR OLD.tracking_mode IS NOT NEW.tracking_mode
  OR OLD.is_archived IS NOT NEW.is_archived
  OR (CASE WHEN json_valid(OLD.meta) THEN coalesce(
        json_extract(OLD.meta, '$.accounting.costBasisMethod'),
        json_extract(OLD.meta, '$.accounting.cost_basis_method')) END)
     IS NOT (CASE WHEN json_valid(NEW.meta) THEN coalesce(
        json_extract(NEW.meta, '$.accounting.costBasisMethod'),
        json_extract(NEW.meta, '$.accounting.cost_basis_method')) END)
  OR (CASE WHEN json_valid(OLD.meta) THEN coalesce(
        json_extract(OLD.meta, '$.accounting.costBasisProfile'),
        json_extract(OLD.meta, '$.accounting.cost_basis_profile')) END)
     IS NOT (CASE WHEN json_valid(NEW.meta) THEN coalesce(
        json_extract(NEW.meta, '$.accounting.costBasisProfile'),
        json_extract(NEW.meta, '$.accounting.cost_basis_profile')) END)
  OR (CASE WHEN json_valid(OLD.meta) THEN coalesce(
        json_extract(OLD.meta, '$.accounting.poolingScope'),
        json_extract(OLD.meta, '$.accounting.pooling_scope')) END)
     IS NOT (CASE WHEN json_valid(NEW.meta) THEN coalesce(
        json_extract(NEW.meta, '$.accounting.poolingScope'),
        json_extract(NEW.meta, '$.accounting.pooling_scope')) END)
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (NEW.id, '0001-01-01', 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = '0001-01-01',
        version = projection_state.version + 1;
END;

-- Asset facts the kernel reads: kind, quote currency, instrument type and the
-- contract multiplier (metadata.option, metadata.contractMultiplier). Profile,
-- logo or name edits do not touch the projection.
CREATE TRIGGER projection_asset_update AFTER UPDATE OF kind, quote_ccy, instrument_type, metadata ON assets
WHEN OLD.kind IS NOT NEW.kind
  OR OLD.quote_ccy IS NOT NEW.quote_ccy
  OR OLD.instrument_type IS NOT NEW.instrument_type
  OR (CASE WHEN json_valid(OLD.metadata) THEN json_extract(OLD.metadata, '$.option') END)
     IS NOT (CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.option') END)
  OR (CASE WHEN json_valid(OLD.metadata) THEN json_extract(OLD.metadata, '$.contractMultiplier') END)
     IS NOT (CASE WHEN json_valid(NEW.metadata) THEN json_extract(NEW.metadata, '$.contractMultiplier') END)
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES ('a:' || NEW.id, '0001-01-01', 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = '0001-01-01',
        version = projection_state.version + 1;
END;

-- Prices revalue the asset's holders from the quote's day. FX rates are quotes
-- of FX assets: the job reaches back from the rate's day to its pair's
-- previous observation, since conversions take the nearest one either way.
CREATE TRIGGER projection_quote_insert AFTER INSERT ON quotes
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (
        CASE WHEN (SELECT kind FROM assets WHERE id = NEW.asset_id) = 'FX' THEN 'fx:' || NEW.asset_id ELSE 'q:' || NEW.asset_id END,
        coalesce(date(NEW.day), '0001-01-01'),
        1
    )
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_quote_update AFTER UPDATE ON quotes
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (
        CASE WHEN (SELECT kind FROM assets WHERE id = NEW.asset_id) = 'FX' THEN 'fx:' || NEW.asset_id ELSE 'q:' || NEW.asset_id END,
        coalesce(min(date(OLD.day), date(NEW.day)), '0001-01-01'),
        1
    )
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_quote_delete AFTER DELETE ON quotes
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (
        CASE WHEN (SELECT kind FROM assets WHERE id = OLD.asset_id) = 'FX' THEN 'fx:' || OLD.asset_id ELSE 'q:' || OLD.asset_id END,
        coalesce(date(OLD.day), '0001-01-01'),
        1
    )
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

-- Observed snapshots (manual, imported, broker) are facts of holdings-mode
-- accounts; the projection's own CALCULATED rows are not.
CREATE TRIGGER projection_snapshot_insert AFTER INSERT ON holdings_snapshots
WHEN NEW.source <> 'CALCULATED'
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (NEW.account_id, coalesce(date(NEW.snapshot_date), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_snapshot_update AFTER UPDATE ON holdings_snapshots
WHEN OLD.source <> 'CALCULATED' OR NEW.source <> 'CALCULATED'
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (NEW.account_id, coalesce(min(date(OLD.snapshot_date), date(NEW.snapshot_date)), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_snapshot_delete AFTER DELETE ON holdings_snapshots
WHEN OLD.source <> 'CALCULATED'
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES (OLD.account_id, coalesce(date(OLD.snapshot_date), '0001-01-01'), 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_snapshot_position_insert AFTER INSERT ON snapshot_positions
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT s.account_id, coalesce(date(s.snapshot_date), '0001-01-01'), 1
    FROM holdings_snapshots s
    WHERE s.id = NEW.snapshot_id AND s.source <> 'CALCULATED'
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_snapshot_position_update AFTER UPDATE ON snapshot_positions
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT s.account_id, coalesce(date(s.snapshot_date), '0001-01-01'), 1
    FROM holdings_snapshots s
    WHERE s.id = NEW.snapshot_id AND s.source <> 'CALCULATED'
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_snapshot_position_delete AFTER DELETE ON snapshot_positions
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    SELECT s.account_id, coalesce(date(s.snapshot_date), '0001-01-01'), 1
    FROM holdings_snapshots s
    WHERE s.id = OLD.snapshot_id AND s.source <> 'CALCULATED'
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = min(coalesce(projection_state.dirty_from, excluded.dirty_from), excluded.dirty_from),
        version = projection_state.version + 1;
END;

-- Base currency and timezone shape every figure and every business day.
CREATE TRIGGER projection_settings_insert AFTER INSERT ON app_settings
WHEN NEW.setting_key IN ('base_currency', 'timezone')
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES ('@all', '0001-01-01', 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = '0001-01-01',
        version = projection_state.version + 1;
END;

CREATE TRIGGER projection_settings_update AFTER UPDATE ON app_settings
WHEN NEW.setting_key IN ('base_currency', 'timezone')
  AND OLD.setting_value IS NOT NEW.setting_value
BEGIN
    INSERT INTO projection_state (scope, dirty_from, version)
    VALUES ('@all', '0001-01-01', 1)
    ON CONFLICT (scope) DO UPDATE SET
        dirty_from = '0001-01-01',
        version = projection_state.version + 1;
END;
