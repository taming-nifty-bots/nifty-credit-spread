"""Exercise the real close/MTM functions without importing the bot's startup code."""
import ast
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tamingnifty"))
from tamingnifty.charges import CHARGE_SCHEDULE, option_spread_charges
from backfill_net_pnl import charge_fields


def load_functions(names, namespace):
    path = Path(__file__).parent / "spread" / "credit_spread.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(nodes) == len(names)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)


def test_close_saves_gross_net_and_fills_in_one_update():
    trade = dict(_id="test", quantity=65, long_option_cost=100, short_option_cost=200,
                 long_option_symbol="LONG", short_option_symbol="SHORT",
                 long_option_security_id="1", short_option_security_id="2",
                 trend="Bullish", entry_date="2026-10-01", entry_time="10:01",
                 total_credit_received=6500)
    writes = []
    collection = SimpleNamespace(
        find=lambda query: [trade],
        update_one=lambda query, update: writes.append(update["$set"]))
    namespace = dict(
        strategies=collection, instrument_name="NIFTY",
        safe_notify=lambda message: None,
        place_buy_order=lambda *args: dict(orderStatus="TRADED", averageTradedPrice=150),
        place_sell_order=lambda *args: dict(orderStatus="TRADED", averageTradedPrice=80),
        option_spread_charges=option_spread_charges, CHARGE_SCHEDULE=CHARGE_SCHEDULE,
        update_last_exit_time=lambda: None, days_to_expiry=lambda date: 1,
        datetime=SimpleNamespace(datetime=datetime), sys=sys)
    load_functions({"calculate_pnl", "close_active_positions"}, namespace)
    namespace["close_active_positions"]("test")
    assert len(writes) == 1
    assert writes[0]["strategy_state"] == "closed"
    assert writes[0]["pnl"] == 1950
    assert writes[0]["net_pnl"] == 1813.12
    assert "charges_schedule" not in writes[0]
    assert "charges_estimated" not in writes[0]
    assert "running_net_pnl" not in writes[0]

    def fail_costs(*args):
        raise ValueError("Invalid fee input")

    writes.clear()
    namespace["option_spread_charges"] = fail_costs
    try:
        namespace["close_active_positions"]("test")
    except SystemExit as error:
        assert error.code == 1
    else:
        raise AssertionError("Bot must stop after a filled exit with an accounting error")
    assert writes[0]["strategy_state"] == "closed"
    assert writes[0]["net_pnl"] is None
    assert writes[0]["accounting_error"] == "Invalid fee input"


def test_backfill_reconciles_and_rejects_incomplete_trades():
    trade = dict(quantity=65, long_option_cost=100, long_exit_price=80,
                 short_option_cost=200, short_exit_price=150, pnl=1950)
    assert charge_fields(trade)["net_pnl"] == 1813.12
    for changes in (dict(pnl=999), dict(long_exit_price=None), dict(quantity=0)):
        try:
            charge_fields(dict(trade, **changes))
        except (TypeError, ValueError):
            continue
        raise AssertionError("Bad historical trade accepted")


def test_running_pnl_behavior_is_unchanged():
    trade = dict(_id="test", quantity=65, long_option_cost=100, short_option_cost=200,
                 long_option_security_id="1", short_option_security_id="2")
    writes = []
    namespace = dict(
        strategies=SimpleNamespace(update_one=lambda query, update: writes.append(update["$set"])),
        edge=SimpleNamespace(login_to_dhan=lambda: None,
                             get_option_price=lambda conn, secid, *args: 80 if secid == "1" else 150),
        datetime=SimpleNamespace(datetime=datetime), option_spread_charges=option_spread_charges)
    load_functions({"calculate_pnl", "get_pnl"}, namespace)
    gross = namespace["get_pnl"](trade, start=datetime(2026, 10, 1))
    assert gross == 1950
    assert writes == [dict(running_pnl=1950)]


if __name__ == "__main__":
    test_close_saves_gross_net_and_fills_in_one_update()
    test_backfill_reconciles_and_rejects_incomplete_trades()
    test_running_pnl_behavior_is_unchanged()
    print("3 credit-spread accounting tests passed")
