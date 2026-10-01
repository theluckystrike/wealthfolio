# Portfolio engine rules for boundary cases

The architecture (`portfolio-engine.md`) describes how the engine works. This
page states what it must produce where the answer is a decision, not a
mechanism: money in and out, transfers, dates and currencies, dated reads, and
which writes invalidate which results. Code and tests follow this page; a change
of rule changes this page first, and is approved before it is implemented.

Each rule names the fixtures whose expected values are worked out by hand from
it, never taken from the engine.

## 1. Money in and out (flows)

**R1.1 Transactions accounts.** A deposit, withdrawal, or transfer to or from
outside the scope is money in or out on its business day, at its amount (or its
units at that day's price, for securities). Income, fees and taxes are returns,
not flows.

**R1.2 Holdings accounts: snapshots for numbers, activities for reports.** A
holdings account is known only through its snapshots, so its value, positions,
flows and returns come only from them. At each snapshot after its first, money
in or out = the snapshot's value − the previous snapshot's holdings valued at
that day's prices; if either side is not fully priced, the flow is undetermined.
Users may record any activity on a holdings account, transfers included, but the
engine never uses a holdings account's activities for those numbers: deposits
and withdrawals are inside the next snapshot, and dividends, interest, fees and
taxes feed only the income, fees and taxes reports. Fixtures: EDGE-MIX-04,
NOM-MIX-01.

**R1.3 A scope's first day** carries no flow: it is where returns start.

**R1.4 An account opening inside a scope** (its first day is not the scope's
first day) brings money in. A transactions account brings what its activities
brought in that day, or its net contribution when it recorded none; a holdings
account brings its first snapshot's value (undetermined when not fully priced).
Fixtures: EDGE-MIX-02, LIFE-EMPTY-01.

## 2. Transfers

**R2.1 Between two transactions accounts in the scope.**

- Each leg is priced on its own day. The sender gives what it held: its leg is
  priced on the units it actually removed, and is no flow when it held none. The
  receiver books the quantity its own activity records; units the sender lacked
  arrive at the transfer's price.
- At the scope, the outgoing leg nets whole and the incoming leg nets in the
  share of units the sender gave (out units ÷ in units), computed from both legs
  whatever a dated read cuts. What remains is money from outside the history. A
  cash pair nets whole: a rate difference between its legs is a gain (#1655).
- Fixtures: EDGE-TXF-02, EDGE-TXF-09, EDGE-TXF-12, EDGE-TXF-14.

**R2.2 A currency conversion inside one account.** When the import linker
recorded it, it moves no money in or out and a better rate than the market's is
a gain; otherwise each leg moves net contribution at the market rate (legacy)
and the difference reads as an estimated flow. Fixtures: EDGE-TXF-05,
EDGE-TXF-13.

**R2.3 A transfer between a transactions account and a holdings account** is not
netted as a pair. The transactions side is money (or shares) leaving or entering
the scope on its day, as a transfer to or from outside: the fold does not wait
for the holdings side. The holdings side counts when its next snapshot shows it
(R1.2), at that snapshot's prices, so a price move in between reads as money in
or out (§7). Fixtures: EDGE-MIX-03, EDGE-MIX-05.

**R2.4 A transfer without a quote** is valued at cost.

- The outgoing leg flows the cost it removed.
- The incoming leg flows the cost of every unit it delivered, less its own
  capitalised fee: the lots it opened, plus the units that covered a short in
  the receiving account.
- Covered units carry the sender's cost at its historical rates to the base
  currency, as opened lots do; a transfer cover's realized P&L in base uses that
  cost.
- Fixtures: EDGE-TXF-15, EDGE-TXF-16.

**R2.5 Moving a short** is a liability changing hands: sending it is money in,
receiving it money out. Fixtures: EDGE-TXF-07, EDGE-TXF-08.

## 3. Dates and currencies

**R3.1** An activity's day is its business date in the portfolio's time zone.

**R3.2** A quote's day is the UTC date of its timestamp. Normal writes store
both consistently; synced rows are normalized where applied (R6.1).

**R3.3** A sale's or cover's proceeds convert to the base currency at the
disposal day's rate; costs at their acquisition rate, so realized P&L in base
includes the currency move. Exception: R2.4's covered units.

## 4. Dated reads

**R4.1** A dated read equals the full read on every day after its first; its
first day carries no flow.

**R4.2** Units in transit between the legs of a transfer spread over several
days belong to neither account. A window starting between the legs reads their
return as gain; a window spanning both legs is unaffected (§7).

## 5. What invalidates what

Every fact the engine reads leaves a marker in `projection_state` when it
changes, in the same transaction, from the earliest day it can affect. `GENESIS`
is `0001-01-01`; `@all` refolds every account from `GENESIS`.

| Fact              | Change                                                                                                    | Marker and earliest day                                                                                                                            |
| ----------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Account           | insert                                                                                                    | the account, from `GENESIS` (sync can deliver its snapshots first)                                                                                 |
| Account           | currency, type, tracking mode, archived, accounting method, profile, pooling scope, lot selection         | the account, from `GENESIS`                                                                                                                        |
| Activity          | insert, update, delete (any field)                                                                        | old and new accounts, from the day before their old and new dates; transfer partners likewise; a split's old and new assets from `GENESIS`         |
| Asset             | insert                                                                                                    | its holders, from `GENESIS` (sync can deliver snapshots naming it first; an FX asset's rates cannot precede it, quotes reference their asset)      |
| Asset             | kind, quote currency, instrument type, option, contract multiplier, and an FX asset's `instrument_symbol` | its holders from `GENESIS`; an FX asset, or one becoming or ceasing to be FX: `@all`                                                               |
| Asset             | delete                                                                                                    | as its kind was: its holders from `GENESIS`, or `@all` for an FX asset                                                                             |
| Quote             | insert, update, delete                                                                                    | its asset's prices (an FX asset's: conversions) from the earliest of its old and new `day` and timestamp dates; old and new assets when reassigned |
| Observed snapshot | insert, update, delete                                                                                    | its account from its date; old and new accounts, each from its own date, when reassigned                                                           |
| Snapshot position | insert, update, delete                                                                                    | its snapshot's account from the snapshot's date                                                                                                    |
| Settings          | base currency, time zone                                                                                  | `@all`                                                                                                                                             |

A run consumes a marker only after writing what it covers. An account or asset
that arrives after facts naming it marks itself on insert (rows above), so a run
that could not project it yet projects it once it exists.

## 6. Data normalized where written

**R6.1** When sync applies a quote, its day is set from its timestamp (R3.2).

**R6.2** Sync may move a quote to another asset or a snapshot to another
account; both owners are invalidated (§5).

## 7. Known limits

- Holdings mode assumes trades and transfers happen at snapshot prices: a price
  move between a trade (or a transfer, R2.3) and the next snapshot reads as
  money in or out.
- A dividend recorded in a holdings account between snapshots reads as money in
  at the next snapshot, as an unrecorded one does (R1.2: activities never change
  a holdings account's numbers).
- Units in transit between transfer legs are not valued (R4.2).

## 8. How tests use these rules

- Each rule's fixtures carry expected values worked out by hand in their
  `expected_notes`, and the goldens pin them.
- Property laws state rules over every scenario. Where a law compares the engine
  with itself (determinism, windows, renaming), it proves consistency, not these
  rules; the fixtures above prove the rules.
- §5 is checked mechanically: a storage test changes every column the engine
  reads, one at a time, and fails when the change leaves no marker.
