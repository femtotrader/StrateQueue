"""
Tests for StatisticsManager functionality, organized by feature area.

This package contains modularized tests for the StatisticsManager class, 
organized into separate modules by functional area:

A. test_cash_bookkeeping.py - Tests for trade recording and cash balance tracking
B. test_equity_curve.py - Tests for position tracking and equity curve calculation
C. test_pnl_accounting.py - Tests for FIFO P&L accounting
D. test_risk_metrics.py - Tests for exposure and drawdown calculations
E. test_round_trips.py - Tests for round-trip trade statistics
F. test_summary_metrics.py - Tests for high-level summary metrics
G. test_edge_cases.py - Tests for edge cases and robustness

Each module can be run independently, or all tests can be run via the main
test_statistics_manager.py file in the parent directory.
""" 