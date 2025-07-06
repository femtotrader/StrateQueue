"""
Common test fixtures for StatisticsManager tests.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import patch

from StrateQueue.core.statistics_manager import StatisticsManager


@pytest.fixture
def stats_manager():
    """Create a fresh StatisticsManager for each test."""
    return StatisticsManager(initial_cash=10000.0)


@pytest.fixture
def fixed_timestamps():
    """Generate a sequence of fixed timestamps for testing."""
    base = pd.Timestamp("2023-01-01 12:00:00", tz="UTC")
    return [base + pd.Timedelta(hours=i) for i in range(10)] 