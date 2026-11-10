import ast
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest
from pymongo.errors import DuplicateKeyError

sys.path.insert(0, str(Path(__file__).parent.parent / "tamingnifty"))
from tamingnifty.safety import guarded_execution

from test_net_pnl import load_functions
from tamingnifty.charges import option_spread_charges


def execution_client():
    state = {}
    collection = Mock()

    def update(query, changes):
        matched = bool(state) and all(state.get(key) == value for key, value in query.items())
        if matched:
            state.update(changes["$set"])
        return SimpleNamespace(acknowledged=True, matched_count=int(matched))

    def insert(document):
        if state:
            raise DuplicateKeyError("existing unresolved guard")
        state.update(document)
        return SimpleNamespace(acknowledged=True)

    collection.update_one.side_effect = update
    collection.insert_one.side_effect = insert
    return {"Bots": {"execution_guards": collection}}, state


@pytest.mark.parametrize("side", ["bull", "bear"])
@pytest.mark.parametrize("failure", ["hedge_timeout", "short_timeout", "strategy_insert", "success"])
def test_entry_execution_failures_never_repeat_legs(side, failure):
    client, state = execution_client()
    buy = Mock(return_value=dict(orderStatus="TRADED", averageTradedPrice=100))
    sell = Mock(return_value=dict(orderStatus="TRADED", averageTradedPrice=200))
    record = Mock()
    if failure == "hedge_timeout":
        buy.side_effect = TimeoutError("broker accepted but response lost")
    elif failure == "short_timeout":
        sell.side_effect = TimeoutError("broker accepted but response lost")
    elif failure == "strategy_insert":
        record.side_effect = RuntimeError("Mongo insertion failed after fills")
    namespace = dict(
        strategies=SimpleNamespace(count_documents=lambda query: 0),
        entry_vix=lambda: 15, get_st_strike=lambda: 25000,
        get_instrument_close=lambda: 25000, instrument_name="NIFTY", quantity=65,
        edge=SimpleNamespace(get_index_option_symbol=lambda *args, **kwargs:
                             ("OPTION", "1", "2026-11-24", 65)),
        place_buy_order=buy, place_sell_order=sell, record_details_in_mongo=record,
        safe_notify=Mock(), days_to_expiry=lambda expiry: 14, sys=sys,
        guarded_execution=guarded_execution, mongo_client=client,
        strategies_collection_name="test")
    name = "create_bull_put_spread" if side == "bull" else "create_bear_call_spread"
    load_functions({name, "_" + name}, namespace)
    if failure == "success":
        namespace[name]()
        assert state["status"] == "complete"
        assert buy.call_count == sell.call_count == record.call_count == 1
    else:
        for attempt in range(2):
            with pytest.raises(SystemExit):
                namespace[name]()
        assert state["status"] == "failed"
        assert buy.call_count == 1
        assert sell.call_count == (0 if failure == "hedge_timeout" else 1)


@pytest.mark.parametrize("failure", ["buyback", "hedge_sale", "save", "exit_time"])
def test_exit_execution_failure_never_rebuys_short(failure):
    client, state = execution_client()
    trade = dict(_id="trade", quantity=65, long_option_cost=100, short_option_cost=200,
                 long_option_symbol="LONG", short_option_symbol="SHORT",
                 long_option_security_id="1", short_option_security_id="2",
                 trend="Bullish", entry_date="2026-11-09", entry_time="10:01",
                 total_credit_received=6500)
    buy = Mock(return_value=dict(orderStatus="TRADED", averageTradedPrice=150))
    sell = Mock(return_value=dict(orderStatus="TRADED", averageTradedPrice=80))
    save = Mock(return_value=SimpleNamespace(acknowledged=True, matched_count=1))
    exit_time = Mock()
    if failure == "buyback":
        buy.side_effect = TimeoutError("unknown broker outcome")
    elif failure == "hedge_sale":
        sell.side_effect = TimeoutError("unknown broker outcome")
    elif failure == "save":
        save.side_effect = RuntimeError("closed-strategy update failed")
    elif failure == "exit_time":
        exit_time.side_effect = RuntimeError("last exit write failed")
    namespace = dict(
        strategies=SimpleNamespace(find=lambda query: [trade], update_one=save),
        instrument_name="NIFTY", safe_notify=Mock(),
        place_buy_order=buy, place_sell_order=sell, option_spread_charges=option_spread_charges,
        update_last_exit_time=exit_time, days_to_expiry=lambda date: 1,
        datetime=SimpleNamespace(datetime=datetime), sys=sys,
        mongo_client=client, strategies_collection_name="test", guarded_execution=guarded_execution)
    load_functions({"calculate_pnl", "close_active_positions", "_close_active_positions"}, namespace)
    for attempt in range(2):
        with pytest.raises(SystemExit):
            namespace["close_active_positions"]("test")
    assert state["status"] == "failed"
    assert buy.call_count == 1
    assert sell.call_count == (0 if failure == "buyback" else 1)


def test_stale_candles_cannot_create_paper_fill():
    namespace = {"pd": pd}
    load_functions({"fresh_completed_close"}, namespace)
    now = datetime(2026, 11, 11, 10, 0)
    for stamps in (["2026-11-09 15:29"], ["2026-11-11 09:55"],
                   ["2026-11-11 10:00"], ["2026-11-11 09:59"] * 2):
        frame = pd.DataFrame(dict(datetime=pd.to_datetime(stamps), close=[100] * len(stamps)))
        with pytest.raises(ValueError):
            namespace["fresh_completed_close"](frame, now)
    good = pd.DataFrame(dict(datetime=pd.to_datetime(["2026-11-11 09:59"]), close=[100]))
    assert namespace["fresh_completed_close"](good, now) == 100


@pytest.mark.parametrize("side", ["bull", "bear"])
def test_missing_vix_halts_before_any_leg(side):
    notify = Mock()
    namespace = dict(
        strategies=SimpleNamespace(count_documents=lambda query: 0),
        entry_vix=Mock(side_effect=KeyError("21")),
        place_buy_order=Mock(), place_sell_order=Mock())
    name = "_create_bull_put_spread" if side == "bull" else "_create_bear_call_spread"
    load_functions({name}, namespace)
    guard = Mock()
    guard.update_one.side_effect = [
        SimpleNamespace(acknowledged=True, matched_count=0),
        SimpleNamespace(acknowledged=True, matched_count=1)]
    guard.insert_one.return_value = SimpleNamespace(acknowledged=True)
    with pytest.raises(SystemExit):
        guarded_execution({"Bots": {"execution_guards": guard}}, "test", notify,
                          "entry", namespace[name])
    namespace["place_buy_order"].assert_not_called()
    namespace["place_sell_order"].assert_not_called()
    notify.assert_called_once()


def test_creation_persists_before_success_notification():
    tree = ast.parse((Path(__file__).parent / "spread" / "credit_spread.py").read_text())
    for name in ("_create_bull_put_spread", "_create_bear_call_spread"):
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
        calls = sorted(((node.lineno, node) for node in ast.walk(function)
                        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)),
                       key=lambda item: item[0])
        record_line = next(line for line, call in calls if call.func.id == "record_details_in_mongo")
        success_line = next(line for line, call in calls if call.func.id == "safe_notify"
                            and call.args and isinstance(call.args[0], ast.JoinedStr)
                            and call.args[0].values[0].value.startswith("Created "))
        assert record_line < success_line


@pytest.mark.parametrize("job", ["spread/credit_spread.py", "signal/credit_spread_signal.py"])
def test_startup_calendar_gate_precedes_loop(job):
    tree = ast.parse((Path(__file__).parent / Path(job)).read_text())
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    first = function.body[0]
    assert isinstance(first, ast.If)
    assert first.test.operand.func.id == "check_trading_day"
    assert isinstance(first.body[0], ast.Return)
