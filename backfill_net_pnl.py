"""Backfill complete, closed credit spreads only. Dry-run unless --apply."""
import argparse
import json
import math
import sys
from pathlib import Path

from dotenv import dotenv_values
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tamingnifty"))
from tamingnifty.charges import CHARGE_SCHEDULE, option_spread_charges


def charge_fields(trade):
    quantity = trade["quantity"]
    fields = ("long_option_cost", "long_exit_price", "short_option_cost", "short_exit_price")
    prices = []
    for field in fields:
        value = float(trade[field])
        if not math.isfinite(value) or value < 0:
            raise ValueError("Invalid fill: " + field)
        prices.append(value)
    long_entry, long_exit, short_entry, short_exit = prices
    gross = round(float(quantity) * (short_entry - short_exit + long_exit - long_entry), 2)
    if not math.isfinite(float(trade["pnl"])) or abs(gross - float(trade["pnl"])) > 0.01:
        raise ValueError("Stored gross P&L does not reconcile to the four fills")
    charges = option_spread_charges(quantity, *prices)
    return dict(net_pnl=round(gross - charges["total"], 2), charges=charges)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    env = dotenv_values(Path(__file__).parent / "spread" / ".env")
    report = dict(applied=args.apply, schedule=CHARGE_SCHEDULE, database="Bots",
                  collection="nifty_weekly_sugam", updated=[], skipped=[])
    with MongoClient(env["CONNECTION_STRING"], serverSelectionTimeoutMS=20000) as client:
        coll = client["Bots"]["nifty_weekly_sugam"]
        trades = list(coll.find({"strategy_state": "closed", "net_pnl": None}))
        for trade in trades:
            try:
                fields = charge_fields(trade)
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                report["skipped"].append(dict(id=str(trade["_id"]), reason=str(error)))
                print("SKIP", trade["_id"], error)
                continue
            if args.apply:
                query = {"_id": trade["_id"], "strategy_state": "closed", "net_pnl": None}
                for field in ("quantity", "long_option_cost", "long_exit_price",
                              "short_option_cost", "short_exit_price", "pnl"):
                    query[field] = trade[field]
                result = coll.update_one(query, {"$set": fields})
                if result.modified_count != 1:
                    raise RuntimeError("Trade changed concurrently; backfill stopped: " + str(trade["_id"]))
                saved = coll.find_one({"_id": trade["_id"]})
                if any(saved[key] != value for key, value in fields.items()):
                    raise RuntimeError("Backfill read-back failed: " + str(trade["_id"]))
            report["updated"].append(dict(
                id=str(trade["_id"]), entry_date=trade["entry_date"],
                exit_date=trade["exit_date"], quantity=trade["quantity"],
                long_entry=trade["long_option_cost"], long_exit=trade["long_exit_price"],
                short_entry=trade["short_option_cost"], short_exit=trade["short_exit_price"],
                pnl=trade["pnl"], **fields))
        report["remaining_without_net_pnl"] = coll.count_documents(
            {"strategy_state": "closed", "net_pnl": None})
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
