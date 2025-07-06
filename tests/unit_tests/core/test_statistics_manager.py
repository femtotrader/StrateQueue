"""
Unit tests for StatisticsManager cash bookkeeping and trade recording.

This file imports all tests from the modularized test files in statistics_manager_tests/.

Tests are organized into the following sections:
A. Trade & Cash Bookkeeping - test_cash_bookkeeping.py
B. Position & Equity-Curve Logic - test_equity_curve.py
C. FIFO P&L Accounting - test_pnl_accounting.py
D. Exposure & Draw-down Helpers - test_risk_metrics.py
E. Round-trip & Trade Statistics - test_round_trips.py
F. High-level Summary Metrics - test_summary_metrics.py
G. Edge-cases / Robustness - test_edge_cases.py
"""

# Import all tests from the modularized files
from tests.unit_tests.core.statistics_manager_tests.test_cash_bookkeeping import *
from tests.unit_tests.core.statistics_manager_tests.test_equity_curve import *
from tests.unit_tests.core.statistics_manager_tests.test_pnl_accounting import *
from tests.unit_tests.core.statistics_manager_tests.test_risk_metrics import *
from tests.unit_tests.core.statistics_manager_tests.test_round_trips import *
from tests.unit_tests.core.statistics_manager_tests.test_summary_metrics import *
from tests.unit_tests.core.statistics_manager_tests.test_edge_cases import *

# This allows running pytest on this file to run all tests
if __name__ == "__main__":
    import pytest
    pytest.main(["-v", __file__]) 