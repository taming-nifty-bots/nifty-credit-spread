# nifty-credit-spread

## Gross and net P&L

Both jobs require `tamingnifty==2.4.3`. Publish that library version before
building/deploying the updated executor; source changes alone do not update a running bot.

`pnl` remains gross. Closed trades add only `net_pnl` and an itemized `charges`
breakdown. Ordinary spread round trips have four executed option orders.
Management signals and existing running gross-P&L behavior are unchanged.

Rates come from the user-supplied chart in the vault's
`Trading Charges - Arrow and Dhan Reference` page. Charges use actual recorded
premiums and quantity, with STT/stamp rounded per order to whole rupees and
other components to paise. Contract-note aggregation, slippage, financing and
exercise/assignment charges can differ; the estimate is not a broker invoice.

Historical closed records with complete fills and reconciled gross P&L can be
backfilled with `python backfill_net_pnl.py --output preview.json`. Review the
preview, then add `--apply` to write. The script targets only
`Bots.nifty_weekly_sugam`, never open positions, preserves gross P&L and fills,
uses compare-and-set updates, and skips records already carrying `net_pnl`.

## Holiday gate and execution fail-stop

Both jobs read `Bots.exchange_calendars` at startup before broker login or
polling. Weekends/NSE holidays exit; missing, invalid or unavailable annual
calendars alert and stop. Maintain the reviewed calendar using the shared
library's `seed_exchange_calendar.py` procedure. 2027 must be loaded separately.

The executor uses `Bots.execution_guards`, keyed by its strategy collection.
A pending attempt is acknowledged before either entry/exit leg; it becomes
complete only after order and strategy persistence. Any execution exception,
partial/non-TRADED response or persistence failure alerts and exits nonzero.
The failed/pending latch survives restarts: restarting does not retry the legs.
An active-strategy recheck inside the reservation also refuses a second entry.
India VIX and fresh current-session index candles are checked before entry
orders. Paper fills require a current-session completed option candle no more
than two minutes old; no latest-seven-day price fallback. Created-spread success
notifications follow successful strategy persistence, not just leg submission.

This is fail-stop, not automatic rollback or broker recovery. A position/order
may remain open. Reconcile broker working orders/partial fills/positions with
Mongo first, manage exposure manually, then explicitly set only that guard's
status to `complete` while retaining the audit fields. Never automatically
delete/expire it. Existing incident paper-order records are not deleted.

Local source/pins do not protect deployed jobs until 2.4.3 is published and
both jobs are rebuilt/deployed. Offline checks from this repository:

```powershell
python -m pytest test_safety.py test_net_pnl.py -q
```
