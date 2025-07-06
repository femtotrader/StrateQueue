"""
Enhanced Demo Data Provider Tests

Tests for TestDataIngestion (demo provider) that focus on:
1. Deterministic caching behavior
2. Real-time simulation lifecycle
3. Data generation consistency
4. Performance and memory management
5. Configuration and customization
"""

import asyncio
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pytest
import pandas as pd

from StrateQueue.data.sources.demo import TestDataIngestion
from StrateQueue.data.sources.data_source_base import MarketData


class TestDemoProviderConstruction:
    """Test demo provider construction and configuration"""
    
    def test_provider_initialization_defaults(self):
        """Test provider initializes with default values"""
        provider = TestDataIngestion()
        
        # Check default base prices
        assert "AAPL" in provider.base_prices
        assert "BTC" in provider.base_prices
        assert provider.base_prices["AAPL"] == 175.0
        assert provider.base_prices["BTC"] == 45000.0
        
        # Check default parameters
        assert provider.update_interval == 1.0
        assert provider.price_volatility == 0.02
        assert provider.is_connected is False
        assert provider.stop_simulation is False
    
    def test_provider_initialization_custom_prices(self):
        """Test provider initializes with custom base prices"""
        custom_prices = {
            "CUSTOM": 100.0,
            "TEST": 200.0
        }
        
        provider = TestDataIngestion(base_prices=custom_prices)
        
        assert provider.base_prices == custom_prices
        assert provider.current_prices == custom_prices
    
    def test_provider_attributes_initialized(self):
        """Test all required attributes are initialized"""
        provider = TestDataIngestion()
        
        # Check inherited attributes
        assert hasattr(provider, 'current_bars')
        assert hasattr(provider, 'historical_data')
        assert hasattr(provider, 'data_callbacks')
        
        # Check demo-specific attributes
        assert hasattr(provider, 'base_prices')
        assert hasattr(provider, 'current_prices')
        assert hasattr(provider, 'subscribed_symbols')
        assert hasattr(provider, 'simulation_thread')
        assert hasattr(provider, '_historical_cache')


class TestDemoProviderCaching:
    """Test caching behavior and object identity"""
    
    @pytest.mark.asyncio
    async def test_identical_calls_return_same_object(self):
        """Test that identical fetch calls return the same DataFrame object"""
        provider = TestDataIngestion()
        
        # Make two identical calls
        df1 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        df2 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        
        # Should be the same object (identity check)
        assert df1 is df2
        
        # Should also be equal in content
        pd.testing.assert_frame_equal(df1, df2)
    
    @pytest.mark.asyncio
    async def test_different_parameters_return_different_objects(self):
        """Test that different parameters return different DataFrame objects"""
        provider = TestDataIngestion()
        
        # Make calls with different parameters
        df1 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        df2 = await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")
        df3 = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1h")
        df4 = await provider.fetch_historical_data("MSFT", days_back=5, granularity="1d")
        
        # Should all be different objects
        assert df1 is not df2
        assert df1 is not df3
        assert df1 is not df4
        assert df2 is not df3
        assert df2 is not df4
        assert df3 is not df4
    
    @pytest.mark.asyncio
    async def test_cache_key_uniqueness(self):
        """Test that cache keys are unique for different parameter combinations"""
        provider = TestDataIngestion()
        
        # Generate data with different parameters
        await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")
        await provider.fetch_historical_data("AAPL", days_back=5, granularity="1h")
        await provider.fetch_historical_data("MSFT", days_back=5, granularity="1d")
        
        # Should have 4 different cache entries
        assert len(provider._historical_cache) == 4
        
        # Check cache keys
        expected_keys = {
            ("AAPL", 5, "1d"),
            ("AAPL", 10, "1d"),
            ("AAPL", 5, "1h"),
            ("MSFT", 5, "1d")
        }
        assert set(provider._historical_cache.keys()) == expected_keys
    
    @pytest.mark.asyncio
    async def test_legacy_historical_data_sync(self):
        """Test that legacy historical_data attribute stays in sync"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        
        # Check legacy attribute is populated
        assert "AAPL" in provider.historical_data
        assert provider.historical_data["AAPL"] is df


class TestDemoProviderDataGeneration:
    """Test data generation consistency and quality"""
    
    @pytest.mark.asyncio
    async def test_dataframe_structure_consistency(self):
        """Test that generated DataFrames have consistent structure"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        
        # Check structure
        assert isinstance(df, pd.DataFrame)
        assert isinstance(df.index, pd.DatetimeIndex)
        assert list(df.columns) == ['Open', 'High', 'Low', 'Close', 'Volume']
        assert df.index.name == 'timestamp'
        
        # Check data types
        for col in ['Open', 'High', 'Low', 'Close']:
            assert pd.api.types.is_numeric_dtype(df[col])
        assert pd.api.types.is_numeric_dtype(df['Volume'])
    
    @pytest.mark.asyncio
    async def test_ohlc_data_validity(self):
        """Test that OHLC data follows market rules"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")
        
        # Check OHLC relationships
        for idx in df.index:
            row = df.loc[idx]
            assert row['High'] >= row['Open'], f"High < Open at {idx}"
            assert row['High'] >= row['Close'], f"High < Close at {idx}"
            assert row['Low'] <= row['Open'], f"Low > Open at {idx}"
            assert row['Low'] <= row['Close'], f"Low > Close at {idx}"
            assert row['Volume'] > 0, f"Volume <= 0 at {idx}"
    
    @pytest.mark.asyncio
    async def test_granularity_affects_data_points(self):
        """Test that different granularities produce different numbers of data points"""
        provider = TestDataIngestion()
        
        # Test different granularities for same time period
        df_1d = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
        df_1h = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1h")
        df_1m = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1m")
        
        # Should have different numbers of data points
        assert len(df_1d) < len(df_1h) < len(df_1m)
        assert len(df_1d) <= 2  # At most 2 days
        assert len(df_1h) <= 48  # At most 48 hours
    
    @pytest.mark.asyncio
    async def test_price_evolution_realism(self):
        """Test that price evolution looks realistic"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=30, granularity="1d")
        
        # Check price movements are reasonable
        price_changes = df['Close'].pct_change().dropna()
        
        # Most price changes should be within reasonable bounds (e.g., ±10%)
        reasonable_changes = price_changes.abs() < 0.1
        assert reasonable_changes.sum() / len(reasonable_changes) > 0.8
    
    @pytest.mark.asyncio
    async def test_volume_generation(self):
        """Test that volume data is generated realistically"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")
        
        # Check volume properties
        assert (df['Volume'] > 0).all()
        assert df['Volume'].dtype in ['int64', 'float64']
        
        # Volume should vary
        assert df['Volume'].std() > 0
    
    @pytest.mark.asyncio
    async def test_current_price_updates(self):
        """Test that current prices are updated after data generation"""
        provider = TestDataIngestion()
        
        original_price = provider.current_prices.get("AAPL", provider.base_prices["AAPL"])
        
        df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
        
        # Current price should be updated to last close price
        if len(df) > 0:
            expected_price = df['Close'].iloc[-1]
            assert provider.current_prices["AAPL"] == expected_price


class TestDemoProviderRealTimeSimulation:
    """Test real-time simulation functionality"""
    
    def test_simulation_lifecycle(self):
        """Test starting and stopping simulation"""
        provider = TestDataIngestion()
        
        # Initially not running
        assert provider.is_connected is False
        assert provider.simulation_thread is None
        
        # Start simulation
        provider.start_realtime_feed()
        
        # Should be running
        assert provider.is_connected is True
        assert provider.simulation_thread is not None
        assert provider.simulation_thread.is_alive()
        
        # Stop simulation
        provider.stop_realtime_feed()
        
        # Should be stopped
        assert provider.is_connected is False
        assert provider.stop_simulation is True
    
    def test_simulation_generates_data(self):
        """Test that simulation generates real-time data"""
        provider = TestDataIngestion()
        
        # Add callback to capture data
        received_data = []
        def capture_callback(data):
            received_data.append(data)
        
        provider.add_data_callback(capture_callback)
        
        # Subscribe to symbol
        asyncio.run(provider.subscribe_to_symbol("AAPL"))
        
        # Start simulation
        provider.start_realtime_feed()
        
        # Wait for some data
        time.sleep(2.5)  # Wait for at least 2 updates
        
        # Stop simulation
        provider.stop_realtime_feed()
        
        # Should have received data
        assert len(received_data) >= 2
        
        # Check data structure
        for data in received_data:
            assert isinstance(data, MarketData)
            assert data.symbol == "AAPL"
            assert data.open > 0
            assert data.high >= data.open
            assert data.low <= data.open
            assert data.volume > 0
    
    def test_simulation_respects_update_interval(self):
        """Test that simulation respects the update interval"""
        provider = TestDataIngestion()
        
        # Set fast update interval
        provider.set_update_interval(0.5)  # 500ms
        
        # Add callback to capture timestamps
        timestamps = []
        def capture_timestamp(data):
            timestamps.append(time.time())
        
        provider.add_data_callback(capture_timestamp)
        
        # Subscribe and start
        asyncio.run(provider.subscribe_to_symbol("AAPL"))
        provider.start_realtime_feed()
        
        # Wait for several updates
        time.sleep(2.0)
        
        # Stop simulation
        provider.stop_realtime_feed()
        
        # Check intervals
        if len(timestamps) >= 2:
            intervals = [timestamps[i] - timestamps[i-1] for i in range(1, len(timestamps))]
            avg_interval = sum(intervals) / len(intervals)
            
            # Should be approximately 0.5 seconds (with some tolerance)
            assert 0.4 <= avg_interval <= 0.7
    
    def test_multiple_symbol_subscription(self):
        """Test subscribing to multiple symbols"""
        provider = TestDataIngestion()
        
        # Subscribe to multiple symbols
        asyncio.run(provider.subscribe_to_symbol("AAPL"))
        asyncio.run(provider.subscribe_to_symbol("MSFT"))
        asyncio.run(provider.subscribe_to_symbol("GOOGL"))
        
        # Check subscriptions
        assert "AAPL" in provider.subscribed_symbols
        assert "MSFT" in provider.subscribed_symbols
        assert "GOOGL" in provider.subscribed_symbols
        assert len(provider.subscribed_symbols) == 3
    
    def test_simulation_thread_cleanup(self):
        """Test that simulation thread is properly cleaned up"""
        provider = TestDataIngestion()
        
        # Start simulation
        provider.start_realtime_feed()
        thread = provider.simulation_thread
        
        assert thread is not None
        assert thread.is_alive()
        
        # Stop simulation
        provider.stop_realtime_feed()
        
        # Wait for thread to finish
        thread.join(timeout=2.0)
        
        # Thread should be finished
        assert not thread.is_alive()


class TestDemoProviderAppendCurrentBar:
    """Test append_current_bar functionality"""
    
    @pytest.mark.asyncio
    async def test_append_current_bar_with_data(self):
        """Test appending current bar when current data exists"""
        provider = TestDataIngestion()
        
        # Get initial historical data
        df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
        original_length = len(df)
        
        # Create current data
        current_data = MarketData(
            symbol="AAPL",
            timestamp=datetime.now(),
            open=200.0,
            high=201.0,
            low=199.0,
            close=200.5,
            volume=2000
        )
        
        # Store as current data
        provider.current_bars["AAPL"] = current_data
        
        # Append current bar
        updated_df = provider.append_current_bar("AAPL")
        
        # Check it was appended
        assert len(updated_df) == original_length + 1
        
        # Check the new data
        last_row = updated_df.iloc[-1]
        assert last_row['Open'] == 200.0
        assert last_row['High'] == 201.0
        assert last_row['Low'] == 199.0
        assert last_row['Close'] == 200.5
        assert last_row['Volume'] == 2000
    
    @pytest.mark.asyncio
    async def test_append_current_bar_without_historical_data(self):
        """Test appending current bar when no historical data exists"""
        provider = TestDataIngestion()
        
        # Create current data for symbol without historical data
        current_data = MarketData(
            symbol="NEWSTOCK",
            timestamp=datetime.now(),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=1000
        )
        
        provider.current_bars["NEWSTOCK"] = current_data
        
        # Append current bar (should generate minimal historical data first)
        updated_df = provider.append_current_bar("NEWSTOCK")
        
        # Should have some data
        assert len(updated_df) > 0
        assert "NEWSTOCK" in provider.historical_data
    
    @pytest.mark.asyncio
    async def test_append_current_bar_no_current_data(self):
        """Test appending current bar when no current data exists"""
        provider = TestDataIngestion()
        
        # Get historical data
        df = await provider.fetch_historical_data("AAPL", days_back=2, granularity="1d")
        original_length = len(df)
        
        # Append without current data
        result_df = provider.append_current_bar("AAPL")
        
        # Should return existing data unchanged
        assert len(result_df) == original_length
        pd.testing.assert_frame_equal(result_df, df)


class TestDemoProviderConfiguration:
    """Test configuration and customization options"""
    
    def test_set_update_interval(self):
        """Test setting update interval"""
        provider = TestDataIngestion()
        
        provider.set_update_interval(2.0)
        assert provider.update_interval == 2.0
    
    def test_set_volatility(self):
        """Test setting price volatility"""
        provider = TestDataIngestion()
        
        provider.set_volatility(0.05)
        assert provider.price_volatility == 0.05
    
    def test_set_base_price(self):
        """Test setting base price for symbol"""
        provider = TestDataIngestion()
        
        provider.set_base_price("CUSTOM", 500.0)
        assert provider.base_prices["CUSTOM"] == 500.0
        assert provider.current_prices["CUSTOM"] == 500.0
    
    def test_set_update_interval_from_granularity(self):
        """Test setting update interval based on granularity"""
        provider = TestDataIngestion()
        
        # Test different granularities
        test_cases = [
            ("1m", 60),
            ("5m", 300),
            ("1h", 3600),
            ("1d", 86400)
        ]
        
        for granularity, expected_seconds in test_cases:
            provider.set_update_interval_from_granularity(granularity)
            assert provider.granularity_seconds == expected_seconds


class TestDemoProviderPerformance:
    """Test performance and memory management"""
    
    @pytest.mark.asyncio
    async def test_large_data_generation_performance(self):
        """Test performance with large data requests"""
        provider = TestDataIngestion()
        
        start_time = time.time()
        
        # Request large amount of data
        df = await provider.fetch_historical_data("AAPL", days_back=365, granularity="1d")
        
        end_time = time.time()
        
        # Should complete reasonably quickly (less than 5 seconds)
        assert end_time - start_time < 5.0
        
        # Should have reasonable amount of data
        assert len(df) > 0
        assert len(df) <= 365  # Shouldn't exceed requested days
    
    @pytest.mark.asyncio
    async def test_memory_usage_with_cache(self):
        """Test memory usage with caching"""
        provider = TestDataIngestion()
        
        # Generate data for multiple symbols and granularities
        symbols = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA"]
        granularities = ["1d", "1h", "1m"]
        
        for symbol in symbols:
            for granularity in granularities:
                await provider.fetch_historical_data(symbol, days_back=10, granularity=granularity)
        
        # Should have cached all combinations
        expected_cache_size = len(symbols) * len(granularities)
        assert len(provider._historical_cache) == expected_cache_size
        
        # All cached DataFrames should be accessible
        for symbol in symbols:
            for granularity in granularities:
                cache_key = (symbol, 10, granularity)
                assert cache_key in provider._historical_cache
                assert isinstance(provider._historical_cache[cache_key], pd.DataFrame)


class TestDemoProviderEdgeCases:
    """Test edge cases and error conditions"""
    
    @pytest.mark.asyncio
    async def test_zero_days_back(self):
        """Test handling of zero days back"""
        provider = TestDataIngestion()
        
        df = await provider.fetch_historical_data("AAPL", days_back=0, granularity="1d")
        
        # Should return empty or minimal DataFrame
        assert isinstance(df, pd.DataFrame)
        assert len(df) >= 0
    
    @pytest.mark.asyncio
    async def test_unknown_symbol(self):
        """Test handling of unknown symbols"""
        provider = TestDataIngestion()
        
        # Should work with any symbol
        df = await provider.fetch_historical_data("UNKNOWN", days_back=5, granularity="1d")
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "UNKNOWN" in provider.historical_data
    
    @pytest.mark.asyncio
    async def test_invalid_granularity_handling(self):
        """Test handling of invalid granularities"""
        provider = TestDataIngestion()
        
        # Should handle gracefully or raise appropriate error
        try:
            df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="invalid")
            # If it doesn't raise, should return valid DataFrame
            assert isinstance(df, pd.DataFrame)
        except Exception as e:
            # If it raises, should be a reasonable error
            assert "granularity" in str(e).lower() or "invalid" in str(e).lower()
    
    def test_simulation_with_no_subscriptions(self):
        """Test simulation behavior with no subscribed symbols"""
        provider = TestDataIngestion()
        
        # Start simulation without subscriptions
        provider.start_realtime_feed()
        
        # Should not crash
        time.sleep(0.5)
        
        # Stop simulation
        provider.stop_realtime_feed()
        
        # Should have no current bars
        assert len(provider.current_bars) == 0 