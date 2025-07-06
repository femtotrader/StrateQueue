"""
Stub replacements for the *alpaca-trade-api* package used by AlpacaBroker.
This file executes at import-time and installs lightweight in-memory fakes
into ``sys.modules``.  Test modules should simply ``import
tests.unit_tests.brokers.alpaca_stubs`` *before* importing
``StrateQueue.brokers.Alpaca.alpaca_broker`` and the real broker will then
successfully import while talking to these stubs.

Why not rely solely on a pytest fixture?
---------------------------------------
During pytest collection each ``test_*.py`` module is imported *before* any
fixture code runs, so the broker import would fail if the real
``alpaca-trade-api`` is absent.  Performing the monkey-patch at *module
import* time guarantees the stubs are in place early, while still allowing a
fixture (optional) to clean state between tests if needed.
"""

from __future__ import annotations

import datetime as _dt
import sys
import types
import uuid
import gc
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Minimal fake order object matching the attributes AlpacaBroker accesses
# ---------------------------------------------------------------------------
class _FakeOrder:  # noqa: D401 – simple data holder
    def __init__(self, symbol: str, side: str, qty: float, otype: str, price: float | None = None):
        self.id: str = str(uuid.uuid4())
        self.client_order_id: str = f"cli-{self.id}"
        self.symbol: str = symbol
        self.side = types.SimpleNamespace(value=side)
        self.order_type = types.SimpleNamespace(value=otype)
        self.qty = qty
        self.notional = None  # type: ignore[assignment]
        self.filled_qty = 0
        self.status = types.SimpleNamespace(value="accepted")
        # ADD start
        # Pricing helpers – AlpacaBroker may read these directly
        self.limit_price = price  # present for LIMIT / STOP_LIMIT orders
        self.stop_price = None
        self.price = price  # fallback used by broker when limit_price absent
        
        # Additional attributes expected by get_order_status
        self.client_order_id = self.id  # Use same as id for simplicity
        self.order_type = types.SimpleNamespace(value=otype)
        self.filled_qty = 0.0
        # ADD end
        now = _dt.datetime.utcnow()
        self.created_at = self.updated_at = now

    # Allow ``.__dict__`` serialisation similar to real alpaca objects
    def __repr__(self) -> str:  # pragma: no cover
        return f"<_FakeOrder {self.id} {self.side.value} qty={self.qty}>"


# ---------------------------------------------------------------------------
# Fake TradingClient exposing only the subset used by AlpacaBroker
# ---------------------------------------------------------------------------
class _FakeAlpacaClient:  # noqa: D401 – stub class
    def __init__(self, *_: Any, **__: Any) -> None:  # accept arbitrary kwargs
        self._orders: List[_FakeOrder] = []

    # ---------------------------------------------------------------------
    # Credential validation helper (called by broker.validate_credentials)
    # ---------------------------------------------------------------------
    def get_account(self):
        return types.SimpleNamespace(
            id="ACCT-TEST",
            portfolio_value="100000",
            cash="50000",
            daytrade_count=0,
            # ADD start – extra fields accessed by AlpacaBroker
            buying_power="50000",
            pattern_day_trader=False,
            currency="USD",
            # ADD end
        )

    # ---------------------------------------------------------------------
    # Order lifecycle helpers
    # ---------------------------------------------------------------------
    def submit_order(self, order_request):  # noqa: D401 – stub
        symbol = getattr(order_request, "symbol", "AAPL")
        order = _FakeOrder(
            symbol=symbol,
            side=order_request.side.value,
            qty=getattr(order_request, "qty", 0.0),
            otype=order_request.__class__.__name__.replace("OrderRequest", "").upper(),
            price=getattr(order_request, "limit_price", None),
        )
        # Set additional order attributes from request
        order.stop_price = getattr(order_request, "stop_price", None)
        order.limit_price = getattr(order_request, "limit_price", None)
        self._orders.append(order)
        return order

    def get_orders(self, symbol: str | None = None):  # noqa: D401 – stub
        return [o for o in self._orders if symbol is None or o.symbol == symbol]

    def get_order_by_id(self, order_id: str):  # noqa: D401 – stub
        return next((o for o in self._orders if o.id == order_id), None)

    def cancel_order_by_id(self, order_id: str):  # noqa: D401 – stub
        # Mark status as canceled rather than deleting to mimic real API
        for o in self._orders:
            if o.id == order_id:
                o.status = types.SimpleNamespace(value="canceled")
                break

    def cancel_all_orders(self, *_: Any, **__: Any):  # noqa: D401 – stub
        self._orders.clear()

    # ADD start – alias used by AlpacaBroker.cancel_all_orders
    def cancel_orders(self, *_: Any, **__: Any):  # noqa: D401 – stub
        # Real SDK name; delegate to the existing helper
        self.cancel_all_orders()

    # ADD end

    def replace_order_by_id(self, order_id: str, replace_request=None, **kwargs):  # noqa: D401 – stub
        # Simplified: update known mutable fields on the target order
        order = self.get_order_by_id(order_id)
        if order is None:
            # Raise an exception like the real API would
            raise _FakeAPIError(f"Order {order_id} not found")
        
        # Update from replace_request if provided
        if replace_request is not None:
            # Copy over attributes the broker may set (limit_price, stop_price, qty)
            for attr in ("limit_price", "stop_price", "qty", "price"):
                if hasattr(replace_request, attr):
                    setattr(order, attr, getattr(replace_request, attr))
                    order.updated_at = _dt.datetime.utcnow()
        
        # Update from kwargs (direct parameter updates)
        for attr, value in kwargs.items():
            if hasattr(order, attr):
                setattr(order, attr, value)
                order.updated_at = _dt.datetime.utcnow()
        
        return True

    # Simplified positions helper
    def get_open_position(self, _symbol: str):  # noqa: D401 – stub
        raise Exception("no position")

    def close_all_positions(self, *_: Any, **__: Any):  # noqa: D401 – stub
        self._orders.clear()

    # ADD start – positions list helper required by broker.get_positions
    def get_all_positions(self, *_: Any, **__: Any):  # noqa: D401 – stub
        # Return empty list (no open positions) for simplicity
        return []
    # ADD end


# ---------------------------------------------------------------------------
# Fake exceptions and tiny helper to generate request dataclasses
# ---------------------------------------------------------------------------
class _FakeAPIError(Exception):
    """Replacement for alpaca.common.exceptions.APIError"""


def _make_req(name: str):
    """Return a dynamic stub class mimicking an Alpaca *OrderRequest."""

    def _init(self, **kwargs):
        self.__dict__.update(kwargs)

    attrs = {"__init__": _init, "__repr__": lambda self: f"<{name} {self.__dict__}>"}
    return type(name, (object,), attrs)


# ---------------------------------------------------------------------------
# Install stub sub-modules into sys.modules so that *import* statements succeed
# ---------------------------------------------------------------------------

# trading.client
client_mod = types.ModuleType("alpaca.trading.client")
client_mod.TradingClient = _FakeAlpacaClient

# trading.enums (only the fields the broker accesses)
enums_mod = types.ModuleType("alpaca.trading.enums")
enums_mod.OrderSide = types.SimpleNamespace(
    BUY=types.SimpleNamespace(value="BUY"),
    SELL=types.SimpleNamespace(value="SELL"),
)
enums_mod.TimeInForce = types.SimpleNamespace(
    DAY="day",
    GTC="gtc",
    IOC="ioc",
    FOK="fok",
    OPG="opg",
    CLS="cls",
)

# trading.requests – fabricate simple dataclasses
req_mod = types.ModuleType("alpaca.trading.requests")
for _name in [
    "Market",
    "Limit",
    "Stop",
    "StopLimit",
    "TrailingStop",
    "Replace",
]:
    setattr(req_mod, f"{_name}OrderRequest", _make_req(f"{_name}OrderRequest"))

# common.exceptions
exc_mod = types.ModuleType("alpaca.common.exceptions")
exc_mod.APIError = _FakeAPIError

# Register all stubs
sys.modules.update(
    {
        "alpaca": types.ModuleType("alpaca"),  # root placeholder
        "alpaca.trading": types.ModuleType("alpaca.trading"),  # parent package placeholder
        "alpaca.trading.client": client_mod,
        "alpaca.trading.enums": enums_mod,
        "alpaca.trading.requests": req_mod,
        "alpaca.common": types.ModuleType("alpaca.common"),
        "alpaca.common.exceptions": exc_mod,
    }
)

# Ensure parent packages expose the children (helps with 'from ... import ...')
sys.modules["alpaca"].trading = sys.modules["alpaca.trading"]
sys.modules["alpaca.trading"].client = client_mod
sys.modules["alpaca.trading"].enums = enums_mod
sys.modules["alpaca.trading"].requests = req_mod
sys.modules["alpaca"].common = sys.modules["alpaca.common"]
sys.modules["alpaca.common"].exceptions = exc_mod

# ADD start – flip flags in already-imported StrateQueue Alpaca modules
for _mn, _m in list(sys.modules.items()):
    if _mn.startswith("StrateQueue.brokers.Alpaca") and hasattr(_m, "ALPACA_AVAILABLE"):
        _m.ALPACA_AVAILABLE = True  # ensure broker uses stub SDK
        setattr(_m, "TradingClient", _FakeAlpacaClient)
        setattr(_m, "APIError", _FakeAPIError)
# ADD end

# ---------------------------------------------------------------------------
# Optional: a pytest *autouse* fixture that just refreshes internal order list
#           after each test so state never leaks between tests.
# ---------------------------------------------------------------------------

import pytest  # noqa: E402 – after sys.modules patch


@pytest.fixture(autouse=True)
def _reset_fake_alpaca_client_state():
    yield
    # After each test, wipe orders from every instantiated fake client to ensure
    # full isolation between cases (important when the same broker instance is
    # reused by different tests via fixtures or parameterisation).

    for obj in gc.get_objects():
        # Using isinstance is safe because every stub instance is of the exact
        # class defined above (we do not expect subclasses).
        if isinstance(obj, _FakeAlpacaClient):
            obj._orders.clear() 