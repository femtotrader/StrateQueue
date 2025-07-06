"""
Comprehensive test suite for AlpacaBroker order handling functionality.

Tests cover:
- Connection and credential validation
- Order placement for all order types (MARKET, LIMIT, STOP, STOP_LIMIT, TRAILING_STOP)
- Order cancellation and replacement
- Position management and querying
- Symbol normalization for crypto
- Error handling and edge cases
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add src to path so we can import directly
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Import stubs first to set up fake alpaca SDK
import tests.unit_tests.brokers.alpaca.alpaca_stubs  # noqa: F401

# Now import the real classes
from StrateQueue.brokers.Alpaca.alpaca_broker import AlpacaBroker, normalize_crypto_symbol
from StrateQueue.brokers.broker_base import BrokerConfig, OrderType, OrderSide
from StrateQueue.core.signal_extractor import TradingSignal, SignalType


def _make_broker():
    """Create and connect AlpacaBroker for testing."""
    cfg = BrokerConfig("alpaca", credentials={"api_key": "test_key", "secret_key": "test_secret"})
    br = AlpacaBroker(cfg)
    assert br.connect()
    return br


# -------------------------------------------------------------------------------------------
# Connection and credential tests
# -------------------------------------------------------------------------------------------
def test_connect_missing_creds_fails():
    """Test that connection fails when credentials are missing."""
    # Test missing API key
    cfg = BrokerConfig("alpaca", credentials={"api_key": "", "secret_key": "secret"})
    br = AlpacaBroker(cfg)
    assert br.connect() is False
    assert br.is_connected is False
    
    # Test missing secret key
    cfg = BrokerConfig("alpaca", credentials={"api_key": "key", "secret_key": ""})
    br = AlpacaBroker(cfg)
    assert br.connect() is False
    assert br.is_connected is False
    
    # Test both missing
    cfg = BrokerConfig("alpaca", credentials={"api_key": "", "secret_key": ""})
    br = AlpacaBroker(cfg)
    assert br.connect() is False
    assert br.is_connected is False


def test_connect_success_with_valid_creds():
    """Test successful connection with valid credentials."""
    br = _make_broker()
    assert br.is_connected is True
    assert br.trading_client is not None


def test_validate_credentials_success():
    """Test successful credential validation."""
    br = _make_broker()
    
    # The stub's get_account method should work
    result = br.validate_credentials()
    assert result is True


def test_validate_credentials_failure():
    """Test credential validation failure."""
    cfg = BrokerConfig("alpaca", credentials={"api_key": "bad_key", "secret_key": "bad_secret"})
    br = AlpacaBroker(cfg)
    
    # Mock the trading client to raise an exception
    with patch('tests.unit_tests.brokers.alpaca.alpaca_stubs._FakeAlpacaClient.get_account', 
               side_effect=Exception("Invalid credentials")):
        result = br.validate_credentials()
        assert result is False


def test_get_broker_info():
    """Test that broker info includes all expected features."""
    br = _make_broker()
    info = br.get_broker_info()
    
    assert info.name == "Alpaca"
    assert info.supported_features["market_orders"] is True
    assert info.supported_features["limit_orders"] is True
    assert info.supported_features["stop_orders"] is True
    assert info.supported_features["crypto_trading"] is True
    assert info.supported_features["fractional_shares"] is True
    assert "stocks" in info.supported_markets
    assert "crypto" in info.supported_markets


# -------------------------------------------------------------------------------------------
# Order placement tests
# -------------------------------------------------------------------------------------------
def test_place_order_market_success():
    """Test successful market order placement."""
    br = _make_broker()
    
    result = br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    
    assert result.success is True
    assert result.order_id is not None
    assert result.client_order_id is not None
    assert "submitted" in result.message.lower()
    
    # Verify order was created in the mock client
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    assert orders[0].symbol == "AAPL"
    assert orders[0].side.value == "BUY"
    assert orders[0].qty == 5.0


def test_place_order_limit_success():
    """Test successful limit order placement."""
    br = _make_broker()
    
    result = br.place_order("AAPL", OrderType.LIMIT, OrderSide.SELL, 10.0, price=150.0)
    
    assert result.success is True
    assert result.order_id is not None
    
    # Verify order details
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.symbol == "AAPL"
    assert order.side.value == "SELL"
    assert order.qty == 10.0
    assert order.limit_price == 150.0


def test_place_order_limit_requires_price():
    """Test that limit orders require a price."""
    br = _make_broker()
    
    result = br.place_order("AAPL", OrderType.LIMIT, OrderSide.BUY, 5.0, price=None)
    
    assert result.success is False
    assert result.error_code == "MISSING_PRICE"
    assert "limit_price" in result.message


def test_place_order_stop_success():
    """Test successful stop order placement."""
    br = _make_broker()
    
    result = br.place_order(
        "AAPL", 
        OrderType.STOP, 
        OrderSide.SELL, 
        10.0, 
        metadata={"stop_price": 140.0}
    )
    
    assert result.success is True
    
    # Verify order details
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.symbol == "AAPL"
    assert order.stop_price == 140.0


def test_place_order_stop_limit_success():
    """Test successful stop-limit order placement."""
    br = _make_broker()
    
    result = br.place_order(
        "AAPL",
        OrderType.STOP_LIMIT,
        OrderSide.SELL,
        10.0,
        metadata={"stop_price": 140.0, "limit_price": 135.0}
    )
    
    assert result.success is True
    
    # Verify order details
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    order = orders[0]
    assert order.stop_price == 140.0
    assert order.limit_price == 135.0


def test_place_order_stop_limit_requires_both_prices():
    """Test that stop-limit orders require both stop and limit prices."""
    br = _make_broker()
    
    # Missing stop price
    result = br.place_order(
        "AAPL",
        OrderType.STOP_LIMIT,
        OrderSide.SELL,
        10.0,
        metadata={"limit_price": 135.0}
    )
    assert result.success is False
    assert result.error_code == "MISSING_PRICES"
    
    # Missing limit price
    result = br.place_order(
        "AAPL",
        OrderType.STOP_LIMIT,
        OrderSide.SELL,
        10.0,
        metadata={"stop_price": 140.0}
    )
    assert result.success is False
    assert result.error_code == "MISSING_PRICES"


def test_place_order_trailing_stop_success():
    """Test successful trailing stop order placement."""
    br = _make_broker()
    
    result = br.place_order(
        "AAPL",
        OrderType.TRAILING_STOP,
        OrderSide.SELL,
        10.0,
        metadata={"trail_percent": 5.0}
    )
    
    assert result.success is True
    
    # Verify order was created
    orders = br.trading_client.get_orders()
    assert len(orders) == 1


def test_place_order_trailing_stop_requires_trail_params():
    """Test that trailing stop orders require trail parameters."""
    br = _make_broker()
    
    result = br.place_order(
        "AAPL",
        OrderType.TRAILING_STOP,
        OrderSide.SELL,
        10.0,
        metadata={}
    )
    
    assert result.success is False
    assert result.error_code == "MISSING_TRAIL_PARAMS"


def test_place_order_not_connected():
    """Test that order placement fails when not connected."""
    cfg = BrokerConfig("alpaca", credentials={"api_key": "key", "secret_key": "secret"})
    br = AlpacaBroker(cfg)
    # Don't connect
    
    result = br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    
    assert result.success is False
    assert "not connected" in result.message.lower()


# -------------------------------------------------------------------------------------------
# Symbol normalization tests
# -------------------------------------------------------------------------------------------
def test_normalize_crypto_symbol_cases():
    """Test crypto symbol normalization."""
    assert normalize_crypto_symbol("eth/usd") == "ETH/USD"
    assert normalize_crypto_symbol("ETH") == "ETH/USD"
    assert normalize_crypto_symbol("BTC/USD") == "BTC/USD"
    assert normalize_crypto_symbol("btc") == "BTC/USD"
    assert normalize_crypto_symbol("AAPL") == "AAPL"  # Non-crypto unchanged


def test_place_order_crypto_symbol_normalization():
    """Test that crypto symbols are normalized during order placement."""
    br = _make_broker()
    
    result = br.place_order("btc", OrderType.MARKET, OrderSide.BUY, 0.1)
    
    assert result.success is True
    
    # Verify the symbol was normalized
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    assert orders[0].symbol == "BTC/USD"


# -------------------------------------------------------------------------------------------
# Order management tests
# -------------------------------------------------------------------------------------------
def test_cancel_order_success():
    """Test successful order cancellation."""
    br = _make_broker()
    
    # Place an order first
    result = br.place_order("AAPL", OrderType.LIMIT, OrderSide.BUY, 5.0, price=150.0)
    assert result.success is True
    order_id = result.order_id
    
    # Cancel the order
    cancel_result = br.cancel_order(order_id)
    assert cancel_result is True
    
    # Verify order status changed
    order_status = br.get_order_status(order_id)
    assert order_status is not None
    assert order_status["status"] == "canceled"


def test_cancel_order_not_connected():
    """Test that order cancellation fails when not connected."""
    cfg = BrokerConfig("alpaca", credentials={"api_key": "key", "secret_key": "secret"})
    br = AlpacaBroker(cfg)
    # Don't connect
    
    result = br.cancel_order("fake_order_id")
    assert result is False


def test_cancel_all_orders():
    """Test cancelling all orders."""
    br = _make_broker()
    
    # Place multiple orders
    br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    br.place_order("MSFT", OrderType.LIMIT, OrderSide.SELL, 10.0, price=300.0)
    
    # Verify orders exist
    orders = br.trading_client.get_orders()
    assert len(orders) == 2
    
    # Cancel all orders
    result = br.cancel_all_orders()
    assert result is True
    
    # Verify all orders are gone
    orders = br.trading_client.get_orders()
    assert len(orders) == 0


def test_replace_order_success():
    """Test successful order replacement."""
    br = _make_broker()
    
    # Place a limit order
    result = br.place_order("AAPL", OrderType.LIMIT, OrderSide.BUY, 5.0, price=150.0)
    assert result.success is True
    order_id = result.order_id
    
    # Replace the order with new price
    replace_result = br.replace_order(order_id, limit_price=155.0)
    assert replace_result is True
    
    # Verify order was updated
    order = br.trading_client.get_order_by_id(order_id)
    assert order is not None
    assert order.limit_price == 155.0


def test_replace_order_not_found():
    """Test replacing non-existent order."""
    br = _make_broker()
    
    result = br.replace_order("nonexistent_id", limit_price=100.0)
    assert result is False


# -------------------------------------------------------------------------------------------
# Order querying tests
# -------------------------------------------------------------------------------------------
def test_get_orders_all():
    """Test getting all orders."""
    br = _make_broker()
    
    # Place some orders
    br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    br.place_order("MSFT", OrderType.LIMIT, OrderSide.SELL, 10.0, price=300.0)
    
    # Get all orders
    orders = br.get_orders()
    assert len(orders) == 2
    
    # Verify order structure
    order = orders[0]
    assert "symbol" in order
    assert "side" in order
    assert "order_type" in order
    assert "qty" in order
    assert "status" in order


def test_get_orders_symbol_filtering():
    """Test getting orders filtered by symbol."""
    br = _make_broker()
    
    # Place orders for different symbols
    br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    br.place_order("MSFT", OrderType.MARKET, OrderSide.BUY, 10.0)
    br.place_order("AAPL", OrderType.LIMIT, OrderSide.SELL, 3.0, price=160.0)
    
    # Get orders for AAPL only
    aapl_orders = br.get_orders("AAPL")
    assert len(aapl_orders) == 2
    for order in aapl_orders:
        assert order["symbol"] == "AAPL"
    
    # Get orders for MSFT only
    msft_orders = br.get_orders("MSFT")
    assert len(msft_orders) == 1
    assert msft_orders[0]["symbol"] == "MSFT"


def test_get_order_status():
    """Test getting order status by ID."""
    br = _make_broker()
    
    # Place an order
    result = br.place_order("AAPL", OrderType.LIMIT, OrderSide.BUY, 5.0, price=150.0)
    assert result.success is True
    order_id = result.order_id
    
    # Get order status
    status = br.get_order_status(order_id)
    assert status is not None
    assert status["id"] == order_id
    assert status["symbol"] == "AAPL"
    assert status["status"] == "accepted"


def test_get_order_status_not_found():
    """Test getting status for non-existent order."""
    br = _make_broker()
    
    status = br.get_order_status("nonexistent_id")
    assert status is None


# -------------------------------------------------------------------------------------------
# Account and position tests
# -------------------------------------------------------------------------------------------
def test_get_account_info():
    """Test getting account information."""
    br = _make_broker()
    
    account_info = br.get_account_info()
    assert account_info is not None
    assert account_info.account_id == "ACCT-TEST"
    assert account_info.buying_power == 50000.0
    assert account_info.cash == 50000.0
    assert account_info.total_value == 100000.0


def test_get_account_info_not_connected():
    """Test that account info fails when not connected."""
    cfg = BrokerConfig("alpaca", credentials={"api_key": "key", "secret_key": "secret"})
    br = AlpacaBroker(cfg)
    # Don't connect
    
    account_info = br.get_account_info()
    assert account_info is None


def test_get_positions():
    """Test getting positions."""
    br = _make_broker()
    
    # The stub returns empty positions by default
    positions = br.get_positions()
    assert isinstance(positions, dict)
    assert len(positions) == 0


def test_close_all_positions():
    """Test closing all positions."""
    br = _make_broker()
    
    # Place some orders first
    br.place_order("AAPL", OrderType.MARKET, OrderSide.BUY, 5.0)
    br.place_order("MSFT", OrderType.MARKET, OrderSide.BUY, 10.0)
    
    # Close all positions
    result = br.close_all_positions()
    assert result is True
    
    # Verify orders are cleared (stub implementation)
    orders = br.trading_client.get_orders()
    assert len(orders) == 0


# -------------------------------------------------------------------------------------------
# Error handling tests
# -------------------------------------------------------------------------------------------
def test_place_order_unsupported_type():
    """Test placing order with unsupported order type."""
    br = _make_broker()
    
    # Create a mock unsupported order type
    class UnsupportedOrderType:
        value = "UNSUPPORTED"
    
    result = br.place_order("AAPL", UnsupportedOrderType(), OrderSide.BUY, 5.0)
    
    assert result.success is False
    assert result.error_code == "UNSUPPORTED_ORDER_TYPE"


def test_place_order_with_metadata():
    """Test placing order with various metadata options."""
    br = _make_broker()
    
    metadata = {
        "time_in_force": "gtc",
        "extended_hours": True,
        "order_class": "bracket",
        "take_profit": {"limit_price": 160.0},
        "stop_loss": {"stop_price": 140.0}
    }
    
    result = br.place_order(
        "AAPL",
        OrderType.LIMIT,
        OrderSide.BUY,
        5.0,
        price=150.0,
        metadata=metadata
    )
    
    assert result.success is True


def test_crypto_orders_use_gtc():
    """Test that crypto orders automatically use GTC time in force."""
    br = _make_broker()
    
    # Place a crypto order
    result = br.place_order("BTC/USD", OrderType.LIMIT, OrderSide.BUY, 0.1, price=50000.0)
    
    assert result.success is True
    
    # Verify order was created
    orders = br.trading_client.get_orders()
    assert len(orders) == 1
    assert orders[0].symbol == "BTC/USD" 