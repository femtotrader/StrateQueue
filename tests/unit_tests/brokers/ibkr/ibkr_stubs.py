"""
Stub replacements for the *ib_insync* package used by IBKRBroker.

This module injects lightweight fake implementations of the classes and
functions that StrateQueue's IBKR integration touches so that unit-tests can
run fully offline (no TWS/Gateway, no ib_insync wheel).

Import this file *before* importing ``StrateQueue.brokers.IBKR.ibkr_broker`` in
any test.  A convenient way is to simply place
``import tests.unit_tests.brokers.ibkr.ibkr_stubs  # noqa: F401`` at the top of
the test module – the side-effects below will register the stubs in
``sys.modules``.
"""

from __future__ import annotations

import gc
import sys
import types
import uuid
from datetime import datetime
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Fake Trade / Order classes – provide only the attributes referenced by
# StrateQueue code (orderId, orderStatus, etc.).
# ---------------------------------------------------------------------------

class _FakeOrder:  # noqa: D401 – simple attribute bag
    def __init__(self, **kwargs):
        # Copy kwargs into the instance dict for convenient dot-access.
        self.__dict__.update(kwargs)
        # ``ibkr_broker`` will assign ``orderId`` later; initialise to None.
        self.orderId: int | None = None


class _FakeTrade:  # noqa: D401 – mirrors minimal ib_insync.Trade API
    def __init__(self, order: _FakeOrder):
        self.order = order
        # ``orderStatus`` attribute with the fields accessed in BaseOrderHandler.
        self.orderStatus = SimpleNamespace(
            status="Submitted",
            filled=0,
            remaining=order.__dict__.get("totalQuantity", 0),
            avgFillPrice=0.0,
            lastFillPrice=0.0,
        )

        # ADD start – minimal helper used by AsyncHelper.wait_for_order_completion
        def _is_done() -> bool:  # noqa: D401 – match ib_insync.Trade API
            return self.orderStatus.status in {"Filled", "Cancelled", "ApiCancelled"}

        # Bind as method so callers can call trade.isDone()
        self.isDone = _is_done  # type: ignore[method-assign]
        # ADD end


# ---------------------------------------------------------------------------
# Fake IB client – enough surface for Broker & Manager classes.
# ---------------------------------------------------------------------------
class _Wrapper:  # Mimic ib_insync.internal.wrapper accounts list
    accounts = ["DU123456"]


class _FakeIB:  # noqa: D401 – stub client
    def __init__(self):
        self._connected = False
        self._trades: list[_FakeTrade] = []
        self.wrapper = _Wrapper()
        
        # Event handlers - need proper += support
        class MockEvent:
            def __iadd__(self, handler):
                return self
        
        self.barUpdateEvent = MockEvent()
        self.pendingTickersEvent = MockEvent()

    # ----- connectivity ----------------------------------------------------
    def connect(self, host="127.0.0.1", port=7497, clientId=1, **kwargs):
        self._connected = True
        # Store port to mimic real ib_insync behavior
        self.client_port = port

    def isConnected(self):  # noqa: D401 – name matches real ib_insync
        return self._connected

    def disconnect(self):  # noqa: D401
        self._connected = False

    # ----- account / positions --------------------------------------------
    def accountSummary(self):  # noqa: D401
        # Return list of objects with .tag/value/currency/account attributes.
        return [
            SimpleNamespace(
                tag="NetLiquidation",
                value="100000",
                currency="USD",
                account="DU123456",
            ),
            SimpleNamespace(
                tag="BuyingPower",
                value="200000",
                currency="USD",
                account="DU123456",
            ),
            SimpleNamespace(
                tag="TotalCashValue",
                value="50000",
                currency="USD",
                account="DU123456",
            ),
        ]

    def positions(self):  # noqa: D401 – PositionManager expects an iterable
        return []  # No positions by default

    # ----- order handling ---------------------------------------------------
    def placeOrder(self, _contract, order):  # noqa: D401 – signature comp.
        # Allocate incremental orderId and wrap into _FakeTrade
        order.orderId = len(self._trades) + 1
        trade = _FakeTrade(order)

        # Decide immediate status based on order type – market orders fill instantly
        otype = getattr(order, "orderType", "MKT").upper()
        if otype == "MKT":
            # Determine quantity (shares or cashQty fallback)
            qty = (
                getattr(order, "totalQuantity", 0)
                or getattr(order, "cashQty", 0)
                or getattr(order, "quantity", 0)
                or 0
            )
            trade.orderStatus.status = "Filled"
            trade.orderStatus.filled = qty
            trade.orderStatus.remaining = 0
            # Provide dummy fill price so avg/last readouts are non-zero
            trade.orderStatus.avgFillPrice = 100.0
            trade.orderStatus.lastFillPrice = 100.0
        else:
            # Limit / other order types stay working
            trade.orderStatus.status = "Submitted"

        self._trades.append(trade)
        return trade

    def trades(self):  # noqa: D401 – OrderManager scans this
        return self._trades

    # ADD start – helpers required by broker & async utils
    def cancelOrder(self, order):  # noqa: D401 – mimic real API
        # Mark the matching trade as cancelled (no effect if not found)
        for tr in self._trades:
            if tr.order is order:
                tr.orderStatus.status = "Cancelled"
                tr.orderStatus.remaining = 0
                break

    # Alias used by some call-sites
    cancel_orders = cancelOrder  # type: ignore[method-assign]

    def openTrades(self):  # noqa: D401 – subset of trades still open
        return [
            tr
            for tr in self._trades
            if tr.orderStatus.status in {"Submitted", "PreSubmitted"}
        ]

    def waitOnUpdate(self, *_a, **_kw):  # noqa: D401 – stubbed event loop helper
        """Synchronisation no-op – real ib_insync blocks until update; tests proceed instantly."""
        pass

    # Market scanner – used by contracts.detect_security_type_from_ibkr
    def reqMatchingSymbols(self, symbol: str):  # noqa: D401
        """Return a minimal list mimicking IB's ContractDescription objects."""
        contract = SimpleNamespace(
            symbol=symbol.upper(),
            secType="STK",  # default to stock for tests
            exchange="SMART",
            currency="USD",
            primaryExchange="NASDAQ",
        )
        return [SimpleNamespace(contract=contract, derivativeSecTypes=[])]
    
    # Gateway broker specific methods
    def reqMktData(self, contract, genericTickList='', snapshot=False, regulatorySnapshot=False):
        """Request market data for a contract."""
        ticker = SimpleNamespace(
            contract=contract,
            last=100.0,
            bid=99.5,
            ask=100.5,
            bidSize=100,
            askSize=100,
            volume=1000,
            updateEvent=lambda: None  # Mock event
        )
        return ticker
    
    def reqRealTimeBars(self, contract, barSize, whatToShow, useRTH):
        """Request real-time bars."""
        pass  # Just a stub
    
    def reqHistoricalData(self, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate):
        """Request historical data."""
        # Return some mock bars
        return [
            SimpleNamespace(
                date=datetime.now(),
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                volume=1000
            )
        ]
    
    def reqMarketDataType(self, marketDataType):
        """Set market data type."""
        pass  # Just a stub
    
    def cancelMktData(self, contract):
        """Cancel market data subscription."""
        pass  # Just a stub
    
    def cancelRealTimeBars(self, contract):
        """Cancel real-time bars subscription."""
        pass  # Just a stub
    
    def tickers(self):
        """Return list of tickers."""
        return []
    
    def ticker(self, contract):
        """Get ticker for contract."""
        return SimpleNamespace(
            contract=contract,
            marketPrice=lambda: 100.0,
            last=100.0,
            close=100.0
        )
    
    # ADD end


# ---------------------------------------------------------------------------
# Fake util helper with startLoop used by IBKRBroker.__init__
# ---------------------------------------------------------------------------
class _FakeUtil:  # noqa: D401 – container for startLoop
    @staticmethod
    def startLoop():  # noqa: D401 – no-op for tests
        pass


# ---------------------------------------------------------------------------
# Assemble a fake ``ib_insync`` package tree and register in sys.modules.
# ---------------------------------------------------------------------------
ib_pkg = types.ModuleType("ib_insync")
ib_pkg.IB = _FakeIB
ib_pkg.util = types.ModuleType("ib_insync.util")
ib_pkg.util.startLoop = _FakeUtil.startLoop  # type: ignore[attr-defined]
ib_pkg.Order = _FakeOrder
ib_pkg.Trade = _FakeTrade

# Extra marker classes so that IBKRBroker detects a “real” ib_insync package.
class _FakeContract:  # noqa: D401 – placeholder
    pass


class _FakeStock:  # noqa: D401 – placeholder
    def __init__(self, *args, **kwargs):
        # Handle both positional and keyword arguments
        if args:
            self.symbol = args[0]
            if len(args) > 1:
                self.exchange = args[1]
            else:
                self.exchange = "SMART"
            if len(args) > 2:
                self.currency = args[2]
            else:
                self.currency = "USD"
        else:
            self.symbol = kwargs.get("symbol", "AAPL")
            self.exchange = kwargs.get("exchange", "SMART")
            self.currency = kwargs.get("currency", "USD")


class _FakeForex:  # noqa: D401 – placeholder
    def __init__(self, *args, **kwargs):
        if args:
            self.symbol = args[0]
        else:
            self.symbol = kwargs.get("pair", "EURUSD")


class _FakeFuture:  # noqa: D401 – placeholder
    def __init__(self, *args, **kwargs):
        if args:
            self.symbol = args[0]
            if len(args) > 1:
                self.exchange = args[1]
            else:
                self.exchange = "GLOBEX"
        else:
            self.symbol = kwargs.get("symbol", "ES")
            self.exchange = kwargs.get("exchange", "GLOBEX")


class _FakeOption:  # noqa: D401 – placeholder
    def __init__(self, *args, **kwargs):
        if args:
            self.symbol = args[0]
            if len(args) > 1:
                self.exchange = args[1]
            else:
                self.exchange = "SMART"
        else:
            self.symbol = kwargs.get("symbol", "AAPL")
            self.exchange = kwargs.get("exchange", "SMART")


class _FakeCrypto:  # noqa: D401 – placeholder
    def __init__(self, *args, **kwargs):
        if args:
            if len(args) >= 2:
                self.symbol = f"{args[0]}{args[1]}"
                if len(args) > 2:
                    self.exchange = args[2]
                else:
                    self.exchange = "PAXOS"
            else:
                self.symbol = args[0]
                self.exchange = "PAXOS"
        else:
            base = kwargs.get("base", "BTC")
            quote = kwargs.get("quote", "USD")
            self.symbol = f"{base}{quote}"
            self.exchange = kwargs.get("exchange", "PAXOS")


ib_pkg.Contract = _FakeContract
ib_pkg.Stock = _FakeStock
ib_pkg.Forex = _FakeForex
ib_pkg.Future = _FakeFuture
ib_pkg.Option = _FakeOption
ib_pkg.Crypto = _FakeCrypto

# Expose sub-module entries so that ``from ib_insync import util`` works.
sys.modules.update(
    {
        "ib_insync": ib_pkg,
        "ib_insync.util": ib_pkg.util,
    }
)

# ADD start – flip flags in already-imported StrateQueue IBKR modules
for _mn, _m in list(sys.modules.items()):
    if _mn.startswith("StrateQueue.brokers.IBKR") and hasattr(_m, "IB_INSYNC_AVAILABLE"):
        _m.IB_INSYNC_AVAILABLE = True  # noqa: D401 – runtime patch
        # Replace any previously imported placeholder classes with the full fakes
        setattr(_m, "IB", _FakeIB)
        setattr(_m, "util", ib_pkg.util)
        setattr(_m, "Order", _FakeOrder)
        # Also set other classes that might be imported
        setattr(_m, "Stock", _FakeStock)
        setattr(_m, "Contract", _FakeContract)
        setattr(_m, "Forex", _FakeForex)
        setattr(_m, "Future", _FakeFuture)
        setattr(_m, "Option", _FakeOption)
        setattr(_m, "Crypto", _FakeCrypto)
# ADD end

# ---------------------------------------------------------------------------
# Autouse pytest fixture: flush recorded trades between tests so state never
# leaks.
# ---------------------------------------------------------------------------
import pytest  # noqa: E402 – after sys.modules patch


@pytest.fixture(autouse=True)
def _reset_fake_ib_state():
    yield
    # After each test, clear trades recorded in any _FakeIB instance.
    for obj in gc.get_objects():
        if isinstance(obj, _FakeIB):
            obj._trades.clear() 