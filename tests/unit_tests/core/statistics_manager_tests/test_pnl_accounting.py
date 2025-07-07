"""
Tests for StatisticsManager P&L accounting functionality (Section C).

Tests in this module verify that:
- FIFO accounting is used for realized P&L calculations
- Unrealized P&L is correctly calculated based on remaining positions and latest prices
- Combined P&L (realized + unrealized) is calculated correctly
"""

import pytest
import pandas as pd
from unittest.mock import patch

from StrateQueue.core.statistics_manager import StatisticsManager


def test_fifo_realized_pnl():
    """
    Test FIFO accounting for realized P&L:
    - Buy 100 shares @ $10 (realized = 0)
    - Buy 100 shares @ $12 (realized = 0)
    - Sell 150 shares @ $14 (realized = (14-10)*100 + (14-12)*50 = $500)
    """
    # Use monotonically increasing timestamps to avoid issues with calc_summary_metrics
    with patch('pandas.Timestamp.now') as mock_now:
        # Fix the initial timestamp for consistency
        mock_now.return_value = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
        
        # Create a fresh StatisticsManager with our mocked time
        stats_manager = StatisticsManager(initial_cash=10000.0)
        
        # Buy first lot - 100 shares @ $10
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=10.0,
            commission=0.0,
        )
        
        # Buy second lot - 100 shares @ $12
        mock_now.return_value = pd.Timestamp("2023-01-01 13:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=12.0,
            commission=0.0,
        )
        
        # At this point, realized P&L should be 0
        metrics_before_sell = stats_manager.calc_summary_metrics()
        assert "realised_pnl" in metrics_before_sell
        assert metrics_before_sell["realised_pnl"] == pytest.approx(0.0)
        
        # Sell 150 shares @ $14 (crosses both lots)
        mock_now.return_value = pd.Timestamp("2023-01-01 14:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="sell",
            quantity=150.0,
            price=14.0,
            commission=0.0,
        )
        
        # Update price to $14 for the unrealized calculation
        mock_now.return_value = pd.Timestamp("2023-01-01 15:00:00", tz="UTC")
        stats_manager.update_market_prices(
            {"XYZ": 14.0}
        )
        
        # Calculate expected realized P&L using FIFO
        # First 100 shares: (14 - 10) * 100 = $400
        # Next 50 shares: (14 - 12) * 50 = $100
        # Total realized: $400 + $100 = $500
        expected_realized_pnl = 400.0 + 100.0
        
        # Get the metrics and check the realized P&L
        metrics = stats_manager.calc_summary_metrics()
        
        assert "realised_pnl" in metrics
        assert metrics["realised_pnl"] == pytest.approx(expected_realized_pnl)
        assert metrics["realised_pnl"] == pytest.approx(500.0)


def test_unrealized_pnl():
    """
    Test unrealized P&L calculation:
    - Continuing from previous scenario, we have 50 shares remaining @ $12
    - Update price to $15
    - Unrealized P&L = (15 - 12) * 50 = $150
    """
    # Use monotonically increasing timestamps to avoid issues with calc_summary_metrics
    with patch('pandas.Timestamp.now') as mock_now:
        # Fix the initial timestamp for consistency
        mock_now.return_value = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
        
        # Create a fresh StatisticsManager with our mocked time
        stats_manager = StatisticsManager(initial_cash=10000.0)
        
        # Buy first lot - 100 shares @ $10
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=10.0,
            commission=0.0,
        )
        
        # Buy second lot - 100 shares @ $12
        mock_now.return_value = pd.Timestamp("2023-01-01 13:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=12.0,
            commission=0.0,
        )
        
        # Sell 150 shares @ $14 (crosses both lots)
        mock_now.return_value = pd.Timestamp("2023-01-01 14:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="sell",
            quantity=150.0,
            price=14.0,
            commission=0.0,
        )
        
        # At this point, unrealized P&L is $0 because we have no price data
        mock_now.return_value = pd.Timestamp("2023-01-01 15:00:00", tz="UTC")
        metrics_before_price_update = stats_manager.calc_summary_metrics()
        
        # There might not be an unrealized_pnl key if there's no price data
        if "unrealised_pnl" in metrics_before_price_update:
            assert metrics_before_price_update["unrealised_pnl"] == pytest.approx(0.0)
        
        # Update price to $15
        mock_now.return_value = pd.Timestamp("2023-01-01 16:00:00", tz="UTC")
        stats_manager.update_market_prices(
            {"XYZ": 15.0}
        )
        
        # Calculate expected unrealized P&L
        # 50 shares remaining at cost basis of $12
        # Unrealized P&L = (15 - 12) * 50 = $150
        expected_unrealized_pnl = (15.0 - 12.0) * 50.0
        
        # Get the metrics and check the unrealized P&L
        mock_now.return_value = pd.Timestamp("2023-01-01 17:00:00", tz="UTC")
        metrics = stats_manager.calc_summary_metrics()
        
        assert "unrealised_pnl" in metrics
        assert metrics["unrealised_pnl"] == pytest.approx(expected_unrealized_pnl)
        assert metrics["unrealised_pnl"] == pytest.approx(150.0)
        
        # Combined P&L (realized + unrealized) should be $500 + $150 = $650
        expected_total_pnl = 500.0 + 150.0
        
        # There might be a 'total_pnl' or we might need to sum them manually
        if "total_pnl" in metrics:
            assert metrics["total_pnl"] == pytest.approx(expected_total_pnl)
        else:
            assert metrics["realised_pnl"] + metrics["unrealised_pnl"] == pytest.approx(expected_total_pnl)


def test_complex_fifo_pnl():
    """
    Test more complex FIFO P&L scenarios with multiple buys and sells:
    - Buy 100 @ $10
    - Sell 50 @ $12 (realized = (12-10)*50 = $100)
    - Buy 200 @ $11
    - Sell 150 @ $13 (realized = (13-10)*50 + (13-11)*100 = $350)
    - Final realized = $100 + $350 = $450
    """
    with patch('pandas.Timestamp.now') as mock_now:
        # Fix the initial timestamp for consistency
        mock_now.return_value = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
        
        # Create a fresh StatisticsManager with our mocked time
        stats_manager = StatisticsManager(initial_cash=10000.0)
        
        # Buy first lot - 100 shares @ $10
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=10.0,
            commission=0.0,
        )
        
        # Sell 50 shares @ $12
        mock_now.return_value = pd.Timestamp("2023-01-01 13:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="sell",
            quantity=50.0,
            price=12.0,
            commission=0.0,
        )
        
        # First realized P&L should be (12-10)*50 = $100
        metrics_after_first_sell = stats_manager.calc_summary_metrics()
        assert metrics_after_first_sell["realised_pnl"] == pytest.approx(100.0)
        
        # Buy second lot - 200 shares @ $11
        mock_now.return_value = pd.Timestamp("2023-01-01 14:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=200.0,
            price=11.0,
            commission=0.0,
        )
        
        # Sell 150 shares @ $13 (crosses both lots)
        mock_now.return_value = pd.Timestamp("2023-01-01 15:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="sell",
            quantity=150.0,
            price=13.0,
            commission=0.0,
        )
        
        # Second realized P&L should be (13-10)*50 + (13-11)*100 = $350
        # Total realized P&L should be $100 + $350 = $450
        metrics_after_second_sell = stats_manager.calc_summary_metrics()
        assert metrics_after_second_sell["realised_pnl"] == pytest.approx(450.0)
        
        # Update price to $14 for the unrealized calculation
        mock_now.return_value = pd.Timestamp("2023-01-01 16:00:00", tz="UTC")
        stats_manager.update_market_prices(
            {"XYZ": 14.0}
        )
        
        # Remaining position: 100 shares @ $11
        # Unrealized P&L = (14-11)*100 = $300
        metrics_final = stats_manager.calc_summary_metrics()
        assert metrics_final["unrealised_pnl"] == pytest.approx(300.0)
        
        # Total P&L = $450 + $300 = $750
        if "total_pnl" in metrics_final:
            assert metrics_final["total_pnl"] == pytest.approx(750.0)
        else:
            assert metrics_final["realised_pnl"] + metrics_final["unrealised_pnl"] == pytest.approx(750.0)


def test_commission_impact_on_pnl():
    """
    Test that commissions and fees are properly accounted for in P&L calculations.
    """
    with patch('pandas.Timestamp.now') as mock_now:
        # Fix the initial timestamp for consistency
        mock_now.return_value = pd.Timestamp("2023-01-01 00:00:00", tz="UTC")
        
        # Create a fresh StatisticsManager with our mocked time
        stats_manager = StatisticsManager(initial_cash=10000.0)
        
        # Buy 100 shares @ $10 with $20 commission
        mock_now.return_value = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="buy",
            quantity=100.0,
            price=10.0,
            commission=20.0,
        )
        
        # Sell 100 shares @ $15 with $30 commission
        mock_now.return_value = pd.Timestamp("2023-01-01 13:00:00", tz="UTC")
        stats_manager.record_trade(
            symbol="XYZ",
            action="sell",
            quantity=100.0,
            price=15.0,
            commission=30.0,
        )
        
        # Calculate expected realized P&L
        # Gross profit = (15-10)*100 = $500
        # Commissions = $20 + $30 = $50
        # Net profit = $500 - $50 = $450
        
        metrics = stats_manager.calc_summary_metrics()
        
        # Check if commissions are included in the realized P&L
        # Note: Some implementations might track commissions separately
        if "commissions" in metrics:
            # If commissions are tracked separately
            assert metrics["commissions"] == pytest.approx(50.0)
            assert metrics["realised_pnl"] == pytest.approx(500.0)
            assert metrics["net_pnl"] == pytest.approx(450.0)
        else:
            # If commissions are already factored into realized P&L
            assert metrics["realised_pnl"] == pytest.approx(450.0)


if __name__ == "__main__":
    pytest.main(["-v", __file__]) 