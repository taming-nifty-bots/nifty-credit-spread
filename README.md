# nifty-credit-spread

## Gross and net P&L

The executor requires `tamingnifty==2.3.2`. Publish that library version before
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
