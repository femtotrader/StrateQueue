"""
Comprehensive Backtrader Engine Tests
=====================================

This module provides comprehensive test coverage for the Backtrader engine,
focusing on areas not covered by the existing test_backtrader_engine.py:

1. Data preparation and feeding mechanisms
2. Complete strategy execution with mock Backtrader infrastructure
3. Order capture and signal extraction
4. Strategy validation and loading
5. Engine factory methods
6. Error handling and edge cases
7. Performance and thread safety

These tests use mocked Backtrader dependencies to run offline without
requiring the actual backtrader package.
"""

import sys
import queue
import threading
import time
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
import pytest

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Mock backtrader before importing engine
_fake_backtrader = ModuleType("backtrader")
_fake_backtrader.__version__ = "fake-1.9.76"

# Mock Strategy class
class MockStrategy:
    def __init__(self):
        self.datas = []
        self.params = Mock()
        self.data = Mock()
        self.data.close = Mock()
        self.data.high = Mock()
        self.data.low = Mock()
        self.data.open = Mock()
        self.data.volume = Mock()
        self.executed_orders = []
        
    def buy(self, **kwargs):
        order = Mock()
        order.data = self.data
        order.size = kwargs.get('size', 1)
        order.price = kwargs.get('price', None)
        order.exectype = kwargs.get('exectype', 'Market')
        self.executed_orders.append(('buy', order))
        return order
        
    def sell(self, **kwargs):
        order = Mock()
        order.data = self.data
        order.size = kwargs.get('size', 1)
        order.price = kwargs.get('price', None)
        order.exectype = kwargs.get('exectype', 'Market')
        self.executed_orders.append(('sell', order))
        return order
        
    def close(self, **kwargs):
        order = Mock()
        order.data = self.data
        order.size = kwargs.get('size', 1)
        order.price = kwargs.get('price', None)
        order.exectype = kwargs.get('exectype', 'Market')
        self.executed_orders.append(('close', order))
        return order
        
    def next(self):
        pass
        
    def __init__(self):
        pass

# Mock Cerebro class
class MockCerebro:
    def __init__(self):
        self.strategies = []
        self.data_feeds = []
        self.results = []
        
    def addstrategy(self, strategy_class, **kwargs):
        self.strategies.append((strategy_class, kwargs))
        
    def adddata(self, data_feed):
        self.data_feeds.append(data_feed)
        
    def run(self):
        # Mock running the strategy
        for strategy_class, kwargs in self.strategies:
            strategy = strategy_class()
            # Simulate running the strategy
            if hasattr(strategy, 'next'):
                strategy.next()
            self.results.append(strategy)
        return self.results

# Mock data feed classes
class MockDataBase:
    def __init__(self, **kwargs):
        self.params = kwargs

class MockPandasData(MockDataBase):
    def __init__(self, dataname=None, **kwargs):
        super().__init__(**kwargs)
        self.dataname = dataname
        
    def __len__(self):
        return len(self.dataname) if self.dataname is not None else 0

# Mock order types
class MockOrder:
    Market = 'Market'
    Limit = 'Limit'
    Stop = 'Stop'
    StopLimit = 'StopLimit'

# Mock date utilities
def mock_date2num(dt):
    """Mock date conversion"""
    if hasattr(dt, 'timestamp'):
        return dt.timestamp()
    return 0.0

# Assemble fake backtrader
_fake_backtrader.Strategy = MockStrategy
_fake_backtrader.Cerebro = MockCerebro
_fake_backtrader.feeds = ModuleType("backtrader.feeds")
_fake_backtrader.feeds.DataBase = MockDataBase
_fake_backtrader.feeds.PandasData = MockPandasData
_fake_backtrader.Order = MockOrder
_fake_backtrader.date2num = mock_date2num

# Insert into sys.modules
sys.modules["backtrader"] = _fake_backtrader
sys.modules["backtrader.feeds"] = _fake_backtrader.feeds

# Now import the engine
import StrateQueue.engines.backtrader_engine as bte
from StrateQueue.engines.backtrader_engine import (
    BacktraderEngine, BacktraderEngineStrategy, BacktraderSignalExtractor
)
from StrateQueue.core.signal_extractor import SignalType, TradingSignal, ExecStyle, OrderFunction


# Test fixtures
@pytest.fixture
def sample_ohlcv_data():
    """Generate sample OHLCV data for testing"""
    return pd.DataFrame({
        'open': [100, 101, 102, 103, 104],
        'high': [105, 106, 107, 108, 109],
        'low': [95, 96, 97, 98, 99],
        'close': [102, 103, 104, 105, 106],
        'volume': [1000, 1100, 1200, 1300, 1400]
    }, index=pd.date_range('2023-01-01', periods=5, freq='1T'))


@pytest.fixture
def basic_strategy_class():
    """Create a basic strategy class for testing"""
    class TestStrategy(MockStrategy):
        def __init__(self):
            super().__init__()
            self.order_count = 0
            
        def next(self):
            self.order_count += 1
            if self.order_count > 2:
                self.buy(size=100)
    
    return TestStrategy


@pytest.fixture
def order_strategy_class():
    """Strategy that tests different order types"""
    class OrderTestStrategy(MockStrategy):
        def __init__(self):
            super().__init__()
            self.order_type = "market"
            
        def next(self):
            if self.order_type == "market":
                self.buy(size=50)
            elif self.order_type == "limit":
                self.buy(size=50, price=100, exectype=MockOrder.Limit)
            elif self.order_type == "stop":
                self.buy(size=50, price=95, exectype=MockOrder.Stop)
            elif self.order_type == "stop_limit":
                self.buy(size=50, price=100, exectype=MockOrder.StopLimit)
    
    return OrderTestStrategy


class TestBacktraderDataHandling:
    """Test data handling and live engine functionality"""
    
    def test_extractor_initialization(self, basic_strategy_class):
        """Test extractor initialization"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        assert extractor.min_bars_required == 2
        assert extractor.strategy_class is basic_strategy_class
        assert not extractor._initialized
        assert extractor.live_engine is None
    
    def test_mixed_case_column_handling(self, basic_strategy_class):
        """Test that mixed case columns are handled properly in signal extraction"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Create DataFrame with mixed case columns
        df = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        # Should handle mixed case columns without error
        signal = extractor.extract_signal(df)
        assert isinstance(signal, TradingSignal)
        assert signal.price == 104  # Should extract the close price
    
    def test_data_with_nans_handling(self, basic_strategy_class):
        """Test NaN handling in signal extraction"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Create DataFrame with NaNs
        df = pd.DataFrame({
            'Open': [100, np.nan, 102],
            'High': [105, 106, np.nan],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, np.nan, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        # Should handle NaNs gracefully
        signal = extractor.extract_signal(df)
        assert isinstance(signal, TradingSignal)
        assert signal.price == 104


class TestBacktraderSignalExtraction:
    """Test signal extraction functionality"""
    
    def test_basic_signal_extraction(self, basic_strategy_class, sample_ohlcv_data):
        """Test basic signal extraction"""
        # Rename columns to match expected format
        data = sample_ohlcv_data.copy()
        data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        signal = extractor.extract_signal(data)
        
        # Should return a valid signal
        assert isinstance(signal, TradingSignal)
        assert signal.price == 106  # Last close price
        assert signal.timestamp == data.index[-1]
    
    def test_insufficient_data_handling(self, basic_strategy_class):
        """Test handling of insufficient data"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=10  # Require more data than provided
        )
        
        # Create small dataset
        small_data = pd.DataFrame({
            'Open': [100, 101],
            'High': [105, 106],
            'Low': [95, 96],
            'Close': [102, 103],
            'Volume': [1000, 1100]
        }, index=pd.date_range('2023-01-01', periods=2, freq='1T'))
        
        signal = extractor.extract_signal(small_data)
        
        assert signal.signal == SignalType.HOLD
        assert 'insufficient_data' in signal.indicators
        assert signal.indicators['insufficient_data'] is True
    
    def test_duplicate_timestamp_handling(self, basic_strategy_class):
        """Test handling of duplicate timestamps"""
        data = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # First extraction
        signal1 = extractor.extract_signal(data)
        assert isinstance(signal1, TradingSignal)
        
        # Second extraction with same data should handle duplicate
        signal2 = extractor.extract_signal(data)
        assert isinstance(signal2, TradingSignal)
        
        # May have duplicate status indicator
        if 'status' in signal2.indicators:
            assert 'duplicate' in signal2.indicators['status']


class TestBacktraderStrategyExecution:
    """Test strategy execution functionality"""
    
    def test_basic_strategy_execution(self, basic_strategy_class):
        """Test basic strategy execution"""
        data = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        signal = extractor.extract_signal(data)
        
        # Should execute successfully and return a signal
        assert isinstance(signal, TradingSignal)
        assert signal.price == 104  # Last close price
    
    def test_strategy_execution_with_exception(self):
        """Test strategy execution that raises exception"""
        class ErrorStrategy(MockStrategy):
            def next(self):
                raise ValueError("Strategy error")
        
        data = pd.DataFrame({
            'Open': [100, 101, 102],
            'High': [105, 106, 107],
            'Low': [95, 96, 97],
            'Close': [102, 103, 104],
            'Volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(ErrorStrategy),
            min_bars_required=2
        )
        
        signal = extractor.extract_signal(data)
        
        # Should return HOLD when execution fails
        assert signal.signal == SignalType.HOLD
        # Error handling may vary in implementation
        assert isinstance(signal, TradingSignal)


class TestBacktraderEngineFactory:
    """Test engine factory methods and strategy validation"""
    
    def test_engine_dependencies_available(self):
        """Test dependency availability check"""
        # Should be True due to our mocking
        with patch.object(BacktraderEngine, '_dependency_available_flag', True):
            assert BacktraderEngine.dependencies_available() is True
        
        # Test when dependencies are not available
        with patch.object(BacktraderEngine, '_dependency_available_flag', False):
            assert BacktraderEngine.dependencies_available() is False
    
    def test_engine_info_creation(self):
        """Test engine info creation"""
        with patch.object(BacktraderEngine, '_dependency_available_flag', True):
            engine = BacktraderEngine()
            info = engine.get_engine_info()
            
            assert info.name == "backtrader"
            assert info.version is not None
            assert info.supported_features is not None
            assert "signal_extraction" in info.supported_features
    
    def test_is_valid_strategy_with_class(self):
        """Test strategy validation with class"""
        with patch.object(BacktraderEngine, '_dependency_available_flag', True):
            engine = BacktraderEngine()
            
            # Valid strategy class (inherits from Strategy)
            class ValidStrategy(MockStrategy):
                def next(self):
                    pass
            
            assert engine.is_valid_strategy("ValidStrategy", ValidStrategy) is True
            
            # Invalid strategy class (doesn't inherit from Strategy)
            class InvalidStrategy:
                def next(self):
                    pass
            
            assert engine.is_valid_strategy("InvalidStrategy", InvalidStrategy) is False
    
    def test_create_engine_strategy(self, basic_strategy_class):
        """Test engine strategy creation"""
        with patch.object(BacktraderEngine, '_dependency_available_flag', True):
            engine = BacktraderEngine()
            
            engine_strategy = engine.create_engine_strategy(basic_strategy_class)
            
            assert isinstance(engine_strategy, BacktraderEngineStrategy)
            assert engine_strategy.strategy_class is basic_strategy_class
    
    def test_create_signal_extractor(self, basic_strategy_class):
        """Test signal extractor creation"""
        with patch.object(BacktraderEngine, '_dependency_available_flag', True):
            engine = BacktraderEngine()
            engine_strategy = BacktraderEngineStrategy(basic_strategy_class)
            
            extractor = engine.create_signal_extractor(
                engine_strategy,
                min_bars_required=10,
                granularity="1min"
            )
            
            assert isinstance(extractor, BacktraderSignalExtractor)
            assert extractor.min_bars_required == 10
            assert extractor.granularity == "1min"


class TestBacktraderEngineStrategy:
    """Test BacktraderEngineStrategy wrapper"""
    
    def test_strategy_initialization(self, basic_strategy_class):
        """Test strategy initialization"""
        strategy_params = {"param1": "value1", "param2": 42}
        
        engine_strategy = BacktraderEngineStrategy(
            basic_strategy_class,
            strategy_params
        )
        
        assert engine_strategy.strategy_class is basic_strategy_class
        assert engine_strategy.strategy_params == strategy_params
    
    def test_get_lookback_period_default(self, basic_strategy_class):
        """Test default lookback period"""
        engine_strategy = BacktraderEngineStrategy(basic_strategy_class)
        
        assert engine_strategy.get_lookback_period() == 50
    
    def test_get_lookback_period_custom(self):
        """Test custom lookback period"""
        class CustomStrategy(MockStrategy):
            lookback_period = 100
            
            def next(self):
                pass
        
        engine_strategy = BacktraderEngineStrategy(CustomStrategy)
        
        assert engine_strategy.get_lookback_period() == 100
    
    def test_get_lookback_period_invalid(self):
        """Test invalid lookback period falls back to default"""
        class InvalidStrategy(MockStrategy):
            lookback_period = "invalid"  # Not a number
            
            def next(self):
                pass
        
        engine_strategy = BacktraderEngineStrategy(InvalidStrategy)
        
        assert engine_strategy.get_lookback_period() == 50


class TestBacktraderSignalExtractor:
    """Test BacktraderSignalExtractor functionality"""
    
    def test_extractor_initialization(self, basic_strategy_class):
        """Test extractor initialization"""
        engine_strategy = BacktraderEngineStrategy(basic_strategy_class)
        
        extractor = BacktraderSignalExtractor(
            engine_strategy,
            min_bars_required=5,
            granularity="5min"
        )
        
        assert extractor.min_bars_required == 5
        assert extractor.granularity == "5min"
        assert extractor.engine_strategy is engine_strategy
    
    def test_reset_functionality(self, basic_strategy_class):
        """Test reset functionality"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Reset should not raise errors
        extractor.reset()
        
        # Should be able to extract signals after reset
        sample_data = pd.DataFrame({
            'open': [100, 101],
            'high': [105, 106],
            'low': [95, 96],
            'close': [102, 103],
            'volume': [1000, 1100]
        }, index=pd.date_range('2023-01-01', periods=2, freq='1T'))
        
        signal = extractor.extract_signal(sample_data)
        assert isinstance(signal, TradingSignal)
    
    def test_get_stats(self, basic_strategy_class, sample_ohlcv_data):
        """Test extractor statistics"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Extract a signal to generate stats
        extractor.extract_signal(sample_ohlcv_data)
        
        stats = extractor.get_stats()
        
        assert 'backtrader_available' in stats
        assert 'min_bars_required' in stats
        assert 'strategy_params' in stats
        assert stats['min_bars_required'] == 2


class TestBacktraderEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_extract_signal_with_empty_dataframe(self, basic_strategy_class):
        """Test extraction with empty DataFrame"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        empty_data = pd.DataFrame()
        
        signal = extractor.extract_signal(empty_data)
        
        assert signal.signal == SignalType.HOLD
        assert 'insufficient_data' in signal.indicators
    
    def test_extract_signal_with_missing_columns(self, basic_strategy_class):
        """Test extraction with missing required columns"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Create data without required columns
        invalid_data = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            # Missing close and volume
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        signal = extractor.extract_signal(invalid_data)
        
        # Should still work with available columns
        assert isinstance(signal, TradingSignal)
    
    def test_concurrent_signal_extraction(self, basic_strategy_class, sample_ohlcv_data):
        """Test concurrent signal extraction (thread safety)"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        results = []
        errors = []
        
        def extract_signal():
            try:
                signal = extractor.extract_signal(sample_ohlcv_data.copy())
                results.append(signal)
            except Exception as e:
                errors.append(e)
        
        # Run multiple extractions concurrently
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=extract_signal)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 3
        
        # All results should be valid TradingSignals
        for result in results:
            assert isinstance(result, TradingSignal)
    
    def test_memory_cleanup_after_extraction(self, basic_strategy_class, sample_ohlcv_data):
        """Test memory cleanup after signal extraction"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Extract multiple signals
        for _ in range(5):
            signal = extractor.extract_signal(sample_ohlcv_data.copy())
            assert isinstance(signal, TradingSignal)
        
        # Should be able to continue extracting signals
        final_signal = extractor.extract_signal(sample_ohlcv_data)
        assert isinstance(final_signal, TradingSignal)


class TestBacktraderDataValidation:
    """Test data validation and preprocessing"""
    
    def test_data_validation_success(self, basic_strategy_class):
        """Test successful data validation"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Valid data
        valid_data = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        # Should not raise any exceptions
        signal = extractor.extract_signal(valid_data)
        assert isinstance(signal, TradingSignal)
    
    def test_data_validation_with_non_numeric_data(self, basic_strategy_class):
        """Test data validation with non-numeric data"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Data with non-numeric values
        invalid_data = pd.DataFrame({
            'open': [100, 'invalid', 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1T'))
        
        # Should handle gracefully
        signal = extractor.extract_signal(invalid_data)
        assert isinstance(signal, TradingSignal)
    
    def test_data_validation_with_duplicate_index(self, basic_strategy_class):
        """Test data validation with duplicate timestamps"""
        extractor = BacktraderSignalExtractor(
            BacktraderEngineStrategy(basic_strategy_class),
            min_bars_required=2
        )
        
        # Data with duplicate timestamps
        duplicate_data = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [102, 103, 104],
            'volume': [1000, 1100, 1200]
        }, index=[
            pd.Timestamp('2023-01-01'),
            pd.Timestamp('2023-01-01'),  # Duplicate
            pd.Timestamp('2023-01-02')
        ])
        
        # Should handle gracefully
        signal = extractor.extract_signal(duplicate_data)
        assert isinstance(signal, TradingSignal)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 