"""
Behavioral Contract Tests for Data Providers

These tests verify that all data providers behave consistently according to the
BaseDataIngestion contract, focusing on:
1. DataFrame structure and data quality
2. Caching behavior and object identity
3. Callback mechanism functionality
4. Real-time data flow
5. Error handling consistency

All tests are parametrized to run against every available provider.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, AsyncMock
from typing import List, Tuple, Any

import pandas as pd
import pytest

from StrateQueue.data.sources.data_source_base import BaseDataIngestion, MarketData
from StrateQueue.data.sources.demo import TestDataIngestion


# Mock implementations for providers that require external dependencies
class MockAlpacaProvider(BaseDataIngestion):
    """Mock Alpaca provider for testing without alpaca-py dependency"""
    
    def __init__(self, api_key="test", secret_key="test", paper=True, granularity="1m", is_crypto=False):
        super().__init__()
        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.granularity = granularity
        self.is_crypto = is_crypto
        self._cache = {}
        
    async def fetch_historical_data(self, symbol: str, days_back: int = 30, granularity: str = "1m") -> pd.DataFrame:
        # Use cache key to ensure same object returned for identical calls
        cache_key = (symbol, days_back, granularity)
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Create realistic test data
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        # Create time series based on granularity
        if granularity == "1m":
            freq = "1min"
            periods = min(days_back * 24 * 60, 1000)  # Limit for performance
        elif granularity == "5m":
            freq = "5min"
            periods = min(days_back * 24 * 12, 1000)
        elif granularity == "1h":
            freq = "1H"
            periods = min(days_back * 24, 1000)
        elif granularity == "1d":
            freq = "1D"
            periods = min(days_back, 1000)
        else:
            freq = "1min"
            periods = 100
            
        timestamps = pd.date_range(start=start_time, periods=periods, freq=freq)
        
        # Generate OHLCV data
        base_price = 100.0
        data = []
        for i, ts in enumerate(timestamps):
            price = base_price + i * 0.1
            data.append({
                'Open': price,
                'High': price + 0.5,
                'Low': price - 0.5,
                'Close': price + 0.25,
                'Volume': 1000 + i * 10
            })
            
        df = pd.DataFrame(data, index=timestamps)
        df.index.name = 'timestamp'
        
        # Cache and store
        self._cache[cache_key] = df
        self.historical_data[symbol] = df
        return df
        
    async def subscribe_to_symbol(self, symbol: str):
        pass
        
    def start_realtime_feed(self):
        pass


class MockPolygonProvider(BaseDataIngestion):
    """Mock Polygon provider for testing without polygon dependency"""
    
    def __init__(self, api_key="test"):
        super().__init__()
        self.api_key = api_key
        self._cache = {}
        
    async def fetch_historical_data(self, symbol: str, days_back: int = 30, granularity: str = "1m") -> pd.DataFrame:
        cache_key = (symbol, days_back, granularity)
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Create test data
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        timestamps = pd.date_range(start=start_time, end=end_time, freq="1min")[:500]  # Limit size
        
        data = []
        for i, ts in enumerate(timestamps):
            data.append({
                'Open': 150.0 + i * 0.01,
                'High': 150.5 + i * 0.01,
                'Low': 149.5 + i * 0.01,
                'Close': 150.25 + i * 0.01,
                'Volume': 2000 + i * 5
            })
            
        df = pd.DataFrame(data, index=timestamps)
        df.index.name = 'timestamp'
        
        self._cache[cache_key] = df
        self.historical_data[symbol] = df
        return df
        
    async def subscribe_to_symbol(self, symbol: str):
        pass
        
    def start_realtime_feed(self):
        pass


class MockCoinMarketCapProvider(BaseDataIngestion):
    """Mock CoinMarketCap provider for testing without CMC dependency"""
    
    def __init__(self, api_key="test", granularity="1d"):
        super().__init__()
        self.api_key = api_key
        self.granularity = granularity
        self._cache = {}
        
    async def fetch_historical_data(self, symbol: str, days_back: int = 30, granularity: str = "1d") -> pd.DataFrame:
        cache_key = (symbol, days_back, granularity)
        if cache_key in self._cache:
            return self._cache[cache_key]
            
        # Create daily crypto data
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        timestamps = pd.date_range(start=start_time, end=end_time, freq="1D")
        
        data = []
        for i, ts in enumerate(timestamps):
            data.append({
                'Open': 45000.0 + i * 100,
                'High': 46000.0 + i * 100,
                'Low': 44000.0 + i * 100,
                'Close': 45500.0 + i * 100,
                'Volume': 1000000 + i * 1000
            })
            
        df = pd.DataFrame(data, index=timestamps)
        df.index.name = 'timestamp'
        
        self._cache[cache_key] = df
        self.historical_data[symbol] = df
        return df
        
    async def subscribe_to_symbol(self, symbol: str):
        pass
        
    def start_realtime_feed(self):
        pass


@pytest.fixture
def all_provider_instances():
    """Fixture providing all available provider instances for contract testing"""
    providers = [
        ("demo", TestDataIngestion()),
        ("mock_alpaca", MockAlpacaProvider()),
        ("mock_polygon", MockPolygonProvider()),
        ("mock_coinmarketcap", MockCoinMarketCapProvider()),
    ]
    return providers


class TestDataFrameStructureContract:
    """Test that all providers return properly structured DataFrames"""
    
    @pytest.mark.asyncio
    async def test_dataframe_has_required_columns(self, all_provider_instances):
        """Every provider must return DataFrame with exact OHLCV columns"""
        required_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
        
        for provider_name, provider in all_provider_instances:
            df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
            
            assert isinstance(df, pd.DataFrame), f"{provider_name} did not return DataFrame"
            assert not df.empty, f"{provider_name} returned empty DataFrame"
            
            # Check exact column set
            actual_columns = set(df.columns)
            assert actual_columns == required_columns, (
                f"{provider_name} has incorrect columns. "
                f"Expected: {required_columns}, Got: {actual_columns}"
            )
    
    @pytest.mark.asyncio
    async def test_dataframe_index_is_datetime(self, all_provider_instances):
        """DataFrame index must be DatetimeIndex in ascending order"""
        for provider_name, provider in all_provider_instances:
            df = await provider.fetch_historical_data("AAPL", days_back=3, granularity="1d")
            
            assert isinstance(df.index, pd.DatetimeIndex), (
                f"{provider_name} index is not DatetimeIndex: {type(df.index)}"
            )
            
            # Check ascending order
            assert df.index.is_monotonic_increasing, (
                f"{provider_name} index is not in ascending order"
            )
    
    @pytest.mark.asyncio
    async def test_dataframe_data_types(self, all_provider_instances):
        """OHLC columns must be numeric, Volume must be numeric"""
        for provider_name, provider in all_provider_instances:
            df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
            
            # Check OHLC are numeric
            for col in ['Open', 'High', 'Low', 'Close']:
                assert pd.api.types.is_numeric_dtype(df[col]), (
                    f"{provider_name} column {col} is not numeric: {df[col].dtype}"
                )
            
            # Volume should be numeric (int or float)
            assert pd.api.types.is_numeric_dtype(df['Volume']), (
                f"{provider_name} Volume column is not numeric: {df['Volume'].dtype}"
            )
    
    @pytest.mark.asyncio
    async def test_dataframe_ohlc_logic(self, all_provider_instances):
        """High >= max(Open, Close) and Low <= min(Open, Close)"""
        for provider_name, provider in all_provider_instances:
            df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
            
            if len(df) == 0:
                continue
                
            # Check OHLC relationships
            max_oc = df[['Open', 'Close']].max(axis=1)
            min_oc = df[['Open', 'Close']].min(axis=1)
            
            high_valid = (df['High'] >= max_oc).all()
            low_valid = (df['Low'] <= min_oc).all()
            
            assert high_valid, f"{provider_name} has High < max(Open, Close) in some rows"
            assert low_valid, f"{provider_name} has Low > min(Open, Close) in some rows"


class TestCachingBehaviorContract:
    """Test caching and object identity behavior"""
    
    @pytest.mark.asyncio
    async def test_identical_calls_return_same_object(self, all_provider_instances):
        """Identical fetch_historical_data calls should return the same DataFrame object"""
        for provider_name, provider in all_provider_instances:
            # Make two identical calls
            df1 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
            df2 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
            
            # They should be the same object (identity check)
            assert df1 is df2, (
                f"{provider_name} does not cache results - different objects returned for identical calls"
            )
    
    @pytest.mark.asyncio
    async def test_different_calls_return_different_objects(self, all_provider_instances):
        """Different parameters should return different DataFrame objects"""
        for provider_name, provider in all_provider_instances:
            df1 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
            df2 = await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")  # Different days_back
            
            # They should be different objects
            assert df1 is not df2, (
                f"{provider_name} incorrectly cached results for different parameters"
            )
    
    @pytest.mark.asyncio
    async def test_historical_data_attribute_populated(self, all_provider_instances):
        """historical_data[symbol] should contain the fetched DataFrame"""
        for provider_name, provider in all_provider_instances:
            df = await provider.fetch_historical_data("AAPL", days_back=3, granularity="1d")
            
            assert "AAPL" in provider.historical_data, (
                f"{provider_name} did not populate historical_data attribute"
            )
            
            stored_df = provider.historical_data["AAPL"]
            assert stored_df is df, (
                f"{provider_name} stored different object in historical_data than returned"
            )


class TestCallbackMechanismContract:
    """Test the callback notification system"""
    
    @pytest.mark.asyncio
    async def test_callback_registration(self, all_provider_instances):
        """Callbacks should be registered and stored properly"""
        for provider_name, provider in all_provider_instances:
            callback = MagicMock()
            
            # Register callback
            provider.add_data_callback(callback)
            
            # Check it's stored
            assert callback in provider.data_callbacks, (
                f"{provider_name} did not store registered callback"
            )
    
    @pytest.mark.asyncio
    async def test_callback_notification(self, all_provider_instances):
        """Callbacks should be invoked when _notify_callbacks is called"""
        for provider_name, provider in all_provider_instances:
            callback = MagicMock()
            provider.add_data_callback(callback)
            
            # Create test market data
            test_data = MarketData(
                symbol="TEST",
                timestamp=datetime.now(),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000
            )
            
            # Notify callbacks
            provider._notify_callbacks(test_data)
            
            # Check callback was invoked
            callback.assert_called_once_with(test_data)
    
    @pytest.mark.asyncio
    async def test_callback_error_handling(self, all_provider_instances):
        """Callback errors should not crash the provider"""
        for provider_name, provider in all_provider_instances:
            def failing_callback(data):
                raise ValueError("Test callback error")
            
            def working_callback(data):
                working_callback.called = True
            working_callback.called = False
            
            provider.add_data_callback(failing_callback)
            provider.add_data_callback(working_callback)
            
            test_data = MarketData(
                symbol="TEST",
                timestamp=datetime.now(),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000
            )
            
            # Should not raise exception
            provider._notify_callbacks(test_data)
            
            # Working callback should still be called
            assert working_callback.called, (
                f"{provider_name} failed callback prevented other callbacks from running"
            )


class TestCurrentDataContract:
    """Test current data storage and retrieval"""
    
    @pytest.mark.asyncio
    async def test_current_data_storage(self, all_provider_instances):
        """Current data should be stored and retrievable"""
        for provider_name, provider in all_provider_instances:
            test_data = MarketData(
                symbol="TEST",
                timestamp=datetime.now(),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000
            )
            
            # Store current data
            provider.current_bars["TEST"] = test_data
            
            # Retrieve it
            retrieved = provider.get_current_data("TEST")
            
            assert retrieved is test_data, (
                f"{provider_name} did not store/retrieve current data correctly"
            )
    
    @pytest.mark.asyncio
    async def test_current_data_none_for_missing_symbol(self, all_provider_instances):
        """get_current_data should return None for unknown symbols"""
        for provider_name, provider in all_provider_instances:
            result = provider.get_current_data("NONEXISTENT")
            assert result is None, (
                f"{provider_name} did not return None for missing symbol"
            )


class TestAppendCurrentBarContract:
    """Test append_current_bar functionality"""
    
    @pytest.mark.asyncio
    async def test_append_current_bar_with_data(self, all_provider_instances):
        """append_current_bar should add current data to historical DataFrame"""
        for provider_name, provider in all_provider_instances:
            # First get some historical data
            df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
            original_length = len(df)
            
            # Create current data
            current_time = datetime.now()
            test_data = MarketData(
                symbol="AAPL",
                timestamp=current_time,
                open=200.0,
                high=201.0,
                low=199.0,
                close=200.5,
                volume=2000
            )
            
            # Store as current data
            provider.current_bars["AAPL"] = test_data
            
            # Append current bar
            updated_df = provider.append_current_bar("AAPL")
            
            # Check it was appended
            assert len(updated_df) == original_length + 1, (
                f"{provider_name} did not append current bar correctly"
            )
            
            # Check the new data is present
            last_row = updated_df.iloc[-1]
            assert last_row['Open'] == 200.0, f"{provider_name} appended wrong Open price"
            assert last_row['Close'] == 200.5, f"{provider_name} appended wrong Close price"
            assert last_row['Volume'] == 2000, f"{provider_name} appended wrong Volume"
    
    @pytest.mark.asyncio
    async def test_append_current_bar_without_current_data(self, all_provider_instances):
        """append_current_bar should return existing data when no current data exists"""
        for provider_name, provider in all_provider_instances:
            # Get historical data
            df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
            original_length = len(df)
            
            # Append without current data
            result_df = provider.append_current_bar("AAPL")
            
            # Should return the same data
            assert len(result_df) == original_length, (
                f"{provider_name} modified DataFrame when no current data available"
            )


class TestGranularityContract:
    """Test granularity parsing and validation"""
    
    @pytest.mark.asyncio
    async def test_granularity_parsing(self, all_provider_instances):
        """_parse_granularity should work for all providers"""
        test_granularities = ["1m", "5m", "1h", "1d"]
        
        for provider_name, provider in all_provider_instances:
            for granularity in test_granularities:
                try:
                    parsed = provider._parse_granularity(granularity)
                    assert parsed is not None, (
                        f"{provider_name} failed to parse granularity {granularity}"
                    )
                except Exception as e:
                    pytest.fail(f"{provider_name} raised exception parsing {granularity}: {e}")


class TestAsyncMethodContract:
    """Test async method behavior"""
    
    @pytest.mark.asyncio
    async def test_fetch_historical_data_is_async(self, all_provider_instances):
        """fetch_historical_data should be properly async"""
        for provider_name, provider in all_provider_instances:
            # Should be able to await it
            result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1d")
            assert isinstance(result, pd.DataFrame), (
                f"{provider_name} fetch_historical_data did not return DataFrame"
            )
    
    @pytest.mark.asyncio
    async def test_subscribe_to_symbol_is_async(self, all_provider_instances):
        """subscribe_to_symbol should be properly async"""
        for provider_name, provider in all_provider_instances:
            # Should be able to await it without error
            await provider.subscribe_to_symbol("AAPL")
            # No specific assertion needed - just that it doesn't raise


class TestRealTimeFeedContract:
    """Test real-time feed lifecycle"""
    
    def test_realtime_feed_lifecycle(self, all_provider_instances):
        """start_realtime_feed and stop_realtime_feed should be callable"""
        for provider_name, provider in all_provider_instances:
            # Should be callable without error
            provider.start_realtime_feed()
            provider.stop_realtime_feed()
            
            # No specific assertions needed - just that they don't raise 