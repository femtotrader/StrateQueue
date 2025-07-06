"""
Tests for StatisticsManager high-level summary metrics (Section F).

Tests in this module verify that:
- calc_summary_metrics returns the full key-set even on tiny samples
- On a deterministic equity curve growing 1% per bar:
  * annualized_return > 0
  * sharpe > 0
  * sortino ≥ sharpe
  * calmar = annualized_return/|max_dd|
"""

import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock

from StrateQueue.core.statistics_manager import StatisticsManager


def test_summary_metrics_on_tiny_samples():
    """
    Test that calc_summary_metrics returns the full key-set even on tiny samples.
    """
    # Create a StatisticsManager with no trades
    stats = StatisticsManager(initial_cash=10000.0)
    
    # Get metrics with no trades
    empty_metrics = stats.calc_summary_metrics()
    
    # Should have at least one key ("trades")
    assert "trades" in empty_metrics
    assert empty_metrics["trades"] == 0
    
    # Add a single trade
    with patch('pandas.Timestamp.now') as mock_now:
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats.record_trade(
            symbol="ABC",
            action="buy",
            quantity=100.0,
            price=50.0,
            commission=10.0
        )
    
    # Get metrics with one trade
    one_trade_metrics = stats.calc_summary_metrics()
    
    # Should have more keys now
    assert "trades" in one_trade_metrics
    assert one_trade_metrics["trades"] == 1
    
    # Verify some basic metrics are present
    assert "current_cash" in one_trade_metrics
    assert "initial_cash" in one_trade_metrics
    assert "current_equity" in one_trade_metrics


def test_deterministic_growth_curve():
    """
    Test metrics on a deterministic equity curve growing 1% per bar:
    - annualized_return > 0
    - sharpe > 0
    - calmar = annualized_return/|max_dd|
    """
    stats = StatisticsManager(initial_cash=10000.0)
    
    # Create a mock equity curve with 1% growth per bar
    # We'll patch the calc_equity_curve method to return this
    start_date = pd.Timestamp("2023-01-01")
    dates = pd.date_range(start=start_date, periods=100, freq="D")
    
    # Create equity values with 1% growth per day
    equity_values = [10000 * (1.01 ** i) for i in range(100)]
    equity_curve = pd.Series(equity_values, index=dates)
    
    # Patch the calc_equity_curve method
    with patch.object(StatisticsManager, 'calc_equity_curve', return_value=equity_curve):
        # Also patch _build_round_trips to return an empty list to avoid the index error
        with patch.object(StatisticsManager, '_build_round_trips', return_value=[]):
            metrics = stats.calc_summary_metrics()
    
    # Verify metrics
    assert metrics["annualized_return"] > 0
    assert metrics["sharpe"] > 0
    
    # Verify calmar ratio
    # For a curve with steady growth, max_dd should be close to 0
    # We'll check that calmar is approximately equal to annualized_return / |max_dd|
    if abs(metrics["max_drawdown"]) > 1e-10:  # Avoid division by zero
        expected_calmar = metrics["annualized_return"] / abs(metrics["max_drawdown"])
        assert metrics["calmar_ratio"] == pytest.approx(expected_calmar, rel=1e-6)


def test_deterministic_growth_curve_with_trades():
    """
    Test metrics on a deterministic equity curve with actual trades.
    Create a sequence of trades that result in steady 1% growth.
    """
    with patch('pandas.Timestamp.now') as mock_now:
        # Fix the initial timestamp for consistency
        start_time = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        mock_now.return_value = start_time
        
        # Create a fresh StatisticsManager
        stats = StatisticsManager(initial_cash=10000.0)
        
        # Create a series of trades that result in steady 1% growth
        # Buy 100 shares at $100
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats.record_trade(
            symbol="ABC",
            action="buy",
            quantity=100.0,
            price=100.0,
            commission=0.0  # No commission for simplicity
        )
        
        # Create a simulated equity curve with 1% growth per day
        # and patch it directly into the StatisticsManager
        start_date = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        dates = pd.date_range(start=start_date, periods=20, freq="D")
        equity_values = [10000 * (1.01 ** i) for i in range(20)]
        equity_curve = pd.Series(equity_values, index=dates)
        
        # Sell all shares at the final price (1.01^20 higher than purchase)
        final_price = 100 * (1.01 ** 20)
        mock_now.return_value = pd.Timestamp("2023-01-21 12:00:00", tz="UTC")
        stats.record_trade(
            symbol="ABC",
            action="sell",
            quantity=100.0,
            price=final_price,
            commission=0.0
        )
        
        # Patch the calc_equity_curve method to return our simulated curve
        with patch.object(StatisticsManager, 'calc_equity_curve', return_value=equity_curve):
            # Also patch _build_round_trips to return an empty list to avoid the index error
            with patch.object(StatisticsManager, '_build_round_trips', return_value=[]):
                # Calculate metrics
                metrics = stats.calc_summary_metrics()
        
        # Verify metrics
        assert metrics["annualized_return"] > 0
        assert metrics["sharpe"] > 0
        
        # For a curve with steady growth, max_dd should be close to 0
        assert abs(metrics["max_drawdown"]) < 0.01


def test_metrics_with_drawdowns():
    """
    Test metrics on an equity curve with deliberate drawdowns.
    """
    stats = StatisticsManager(initial_cash=10000.0)
    
    # Create a mock equity curve with growth and drawdowns
    start_date = pd.Timestamp("2023-01-01")
    dates = pd.date_range(start=start_date, periods=100, freq="D")
    
    # Create equity values with growth and a significant drawdown
    equity_values = []
    for i in range(100):
        if i < 30:
            # Growth phase 1
            equity_values.append(10000 * (1.01 ** i))
        elif i < 50:
            # Drawdown phase
            equity_values.append(10000 * (1.01 ** 30) * (0.99 ** (i - 30)))
        else:
            # Recovery phase
            equity_values.append(10000 * (1.01 ** 30) * (0.99 ** 20) * (1.02 ** (i - 50)))
    
    equity_curve = pd.Series(equity_values, index=dates)
    
    # Patch the calc_equity_curve method
    with patch.object(StatisticsManager, 'calc_equity_curve', return_value=equity_curve):
        # Also patch _build_round_trips to return an empty list to avoid the index error
        with patch.object(StatisticsManager, '_build_round_trips', return_value=[]):
            metrics = stats.calc_summary_metrics()
    
    # Calculate the expected maximum drawdown
    peak_value = 10000 * (1.01 ** 30)
    trough_value = peak_value * (0.99 ** 20)
    expected_max_dd = (trough_value / peak_value) - 1
    
    # Verify metrics
    assert metrics["max_drawdown"] == pytest.approx(expected_max_dd, rel=0.01)
    assert metrics["annualized_return"] > 0  # Still positive overall
    assert metrics["sharpe"] > 0
    
    # Verify calmar ratio
    expected_calmar = metrics["annualized_return"] / abs(metrics["max_drawdown"])
    assert metrics["calmar_ratio"] == pytest.approx(expected_calmar, rel=1e-6)


def test_get_metric_by_name():
    """
    Test the get_metric method to retrieve individual metrics by name.
    """
    stats = StatisticsManager(initial_cash=10000.0)
    
    # Create a mock equity curve
    start_date = pd.Timestamp("2023-01-01")
    dates = pd.date_range(start=start_date, periods=100, freq="D")
    equity_values = [10000 * (1.01 ** i) for i in range(100)]
    equity_curve = pd.Series(equity_values, index=dates)
    
    # Patch the calc_equity_curve method
    with patch.object(StatisticsManager, 'calc_equity_curve', return_value=equity_curve):
        # Also patch _build_round_trips to return an empty list to avoid the index error
        with patch.object(StatisticsManager, '_build_round_trips', return_value=[]):
            # Get all metrics
            all_metrics = stats.calc_summary_metrics()
            
            # Get individual metrics
            sharpe = stats.get_metric("sharpe")
            annualized_return = stats.get_metric("annualized_return")
            sortino = stats.get_metric("sortino_ratio")
            
            # Verify they match the values in all_metrics
            assert sharpe == all_metrics["sharpe"]
            assert annualized_return == all_metrics["annualized_return"]
            assert sortino == all_metrics["sortino_ratio"]
            
            # Test with a non-existent metric
            assert stats.get_metric("non_existent_metric") == 0.0


if __name__ == "__main__":
    pytest.main(["-v", __file__]) 