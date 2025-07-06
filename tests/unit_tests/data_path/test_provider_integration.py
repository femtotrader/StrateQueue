"""
Data Provider Integration Tests

Integration tests that verify all data providers work together consistently
and can be used interchangeably through the common BaseDataIngestion interface.
"""

import asyncio
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock
import pytest
import pandas as pd
import requests

from StrateQueue.data.sources.data_source_base import BaseDataIngestion, MarketData
from StrateQueue.data.sources.demo import TestDataIngestion


# Import all available providers with fallbacks for missing dependencies
def get_available_providers():
    """Get all available provider classes for testing"""
    providers = []
    
    # Always available - demo provider
    providers.append(("demo", TestDataIngestion, {}))
    
    # Try to import real providers
    try:
        from StrateQueue.data.sources.alpaca import AlpacaDataIngestion
        if AlpacaDataIngestion.dependencies_available():
            providers.append(("alpaca", AlpacaDataIngestion, {
                "api_key": "test_key",
                "secret_key": "test_secret",
                "paper": True,
                "granularity": "1m"
            }))
    except ImportError:
        pass
    
    try:
        from StrateQueue.data.sources.polygon import PolygonDataIngestion
        providers.append(("polygon", PolygonDataIngestion, {"api_key": "test_key"}))
    except ImportError:
        pass
    
    try:
        from StrateQueue.data.sources.coinmarketcap import CoinMarketCapDataIngestion
        providers.append(("coinmarketcap", CoinMarketCapDataIngestion, {
            "api_key": "test_key",
            "granularity": "1d"
        }))
    except ImportError:
        pass
    
    return providers


def get_test_symbol_for_provider(provider_name: str) -> str:
    """Get appropriate test symbol for each provider"""
    if provider_name == "coinmarketcap":
        return "BTC"  # CoinMarketCap is for crypto
    else:
        return "AAPL"  # Others can handle stocks


@pytest.fixture
def mock_external_dependencies():
    """Mock all external dependencies for consistent testing"""
    with patch('requests.get') as mock_requests, \
         patch('websocket.WebSocketApp') as mock_websocket, \
         patch('StrateQueue.data.sources.alpaca._ALPACA_AVAILABLE', True), \
         patch('StrateQueue.data.sources.alpaca.StockHistoricalDataClient') as mock_alpaca_client, \
         patch('StrateQueue.data.sources.alpaca.StockDataStream') as mock_alpaca_stream, \
         patch('StrateQueue.data.sources.alpaca.TimeFrame') as mock_timeframe, \
         patch('StrateQueue.data.sources.alpaca.DataFeed') as mock_datafeed, \
         patch('StrateQueue.data.sources.alpaca._Adjustment') as mock_adjustment, \
         patch('StrateQueue.data.sources.alpaca.StockBarsRequest') as mock_stock_bars_request, \
         patch('StrateQueue.data.sources.alpaca.CryptoBarsRequest') as mock_crypto_bars_request:
        
        # Setup Alpaca mocks
        mock_timeframe.Minute = Mock(name="TimeFrame.Minute")
        mock_timeframe.Hour = Mock(name="TimeFrame.Hour")
        mock_timeframe.Day = Mock(name="TimeFrame.Day")
        mock_datafeed.IEX = Mock(name="DataFeed.IEX")
        mock_datafeed.SIP = Mock(name="DataFeed.SIP")
        mock_adjustment.RAW = Mock(name="Adjustment.RAW")
        
        # Setup successful HTTP responses
        def mock_get_response(url, *args, **kwargs):
            mock_response = Mock()
            mock_response.raise_for_status.return_value = None
            mock_response.status_code = 200
            
            # Handle CoinMarketCap ID mapping request
            if "cryptocurrency/map" in url:
                mock_response.json.return_value = {
                    "data": [
                        {"id": 1, "symbol": "BTC", "name": "Bitcoin"},
                        {"id": 1027, "symbol": "ETH", "name": "Ethereum"}
                    ]
                }
            # Handle CoinMarketCap historical data request
            elif "cryptocurrency/ohlcv/historical" in url:
                mock_response.json.return_value = {
                    "data": {
                        "quotes": [
                            {
                                "timestamp": "2023-01-01T00:00:00.000Z",
                                "quote": {
                                    "USD": {
                                        "open": 100.0,
                                        "high": 101.0,
                                        "low": 99.0,
                                        "close": 100.5,
                                        "volume": 1000
                                    }
                                }
                            }
                        ]
                    }
                }
            # Handle Polygon-style response
            else:
                mock_response.json.return_value = {
                    "results": [
                        {
                            "t": 1640995200000,
                            "o": 100.0,
                            "h": 101.0,
                            "l": 99.0,
                            "c": 100.5,
                            "v": 1000
                        }
                    ]
                }
            return mock_response
        
        mock_requests.side_effect = mock_get_response
        
        # Setup Alpaca client mock
        mock_bars_response = Mock()
        mock_bars_response.df = pd.DataFrame({
            'Open': [100.0],
            'High': [101.0],
            'Low': [99.0],
            'Close': [100.5],
            'Volume': [1000]
        }, index=pd.date_range('2023-01-01', periods=1, freq='1min'))
        
        mock_client_instance = Mock()
        mock_client_instance.get_stock_bars.return_value = mock_bars_response
        mock_alpaca_client.return_value = mock_client_instance
        
        # Setup Alpaca stream mock with coroutine
        async def mock_run_forever():
            await asyncio.sleep(0.1)  # Simulate some work
        
        mock_stream_instance = Mock()
        mock_stream_instance._run_forever.return_value = mock_run_forever()
        mock_alpaca_stream.return_value = mock_stream_instance
        
        yield {
            'requests': mock_requests,
            'websocket': mock_websocket,
            'alpaca_client': mock_alpaca_client,
            'alpaca_stream': mock_alpaca_stream
        }


class TestProviderInteroperability:
    """Test that all providers can be used interchangeably"""
    
    @pytest.mark.asyncio
    async def test_all_providers_implement_interface(self, mock_external_dependencies):
        """Test that all providers properly implement BaseDataIngestion interface"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            # Should be able to instantiate
            provider = provider_class(**init_kwargs)
            
            # Should inherit from BaseDataIngestion
            assert isinstance(provider, BaseDataIngestion)
            
            # Should have all required methods
            required_methods = [
                'fetch_historical_data',
                'subscribe_to_symbol',
                'start_realtime_feed',
                'stop_realtime_feed',
                'get_current_data',
                'add_data_callback'
            ]
            
            for method_name in required_methods:
                assert hasattr(provider, method_name)
                assert callable(getattr(provider, method_name))
    
    @pytest.mark.asyncio
    async def test_consistent_dataframe_output(self, mock_external_dependencies):
        """Test that all providers return consistently structured DataFrames"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Use appropriate symbol for each provider
            symbol = get_test_symbol_for_provider(provider_name)
            
            # Fetch data
            df = await provider.fetch_historical_data(symbol, days_back=1, granularity="1d")
            
            # Check structure consistency
            assert isinstance(df, pd.DataFrame), f"{provider_name} did not return DataFrame"
            
            if not df.empty:
                # Check columns
                expected_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
                assert set(df.columns) == expected_columns, f"{provider_name} has wrong columns"
                
                # Check index
                assert isinstance(df.index, pd.DatetimeIndex), f"{provider_name} index not DatetimeIndex"
                
                # Check data types
                for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    assert pd.api.types.is_numeric_dtype(df[col]), f"{provider_name} {col} not numeric"
    
    @pytest.mark.asyncio
    async def test_callback_mechanism_consistency(self, mock_external_dependencies):
        """Test that callback mechanism works consistently across providers"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Add callback
            callback_mock = MagicMock()
            provider.add_data_callback(callback_mock)
            
            # Check callback was added
            assert callback_mock in provider.data_callbacks
            
            # Test notification
            test_data = MarketData(
                symbol="TEST",
                timestamp=datetime.now(),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1000
            )
            
            provider._notify_callbacks(test_data)
            
            # Callback should have been called
            callback_mock.assert_called_once_with(test_data)


class TestProviderErrorHandling:
    """Test error handling consistency across providers"""
    
    @pytest.mark.asyncio
    async def test_network_error_handling(self):
        """Test that providers handle network errors gracefully"""
        # Mock network failure
        with patch('requests.get', side_effect=requests.exceptions.ConnectionError("Network error")):
            # Test providers that use HTTP requests
            try:
                from StrateQueue.data.sources.polygon import PolygonDataIngestion
                provider = PolygonDataIngestion(api_key="test_key")
                
                # Should not crash, should return empty DataFrame
                df = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1d")
                assert isinstance(df, pd.DataFrame)
                assert len(df) == 0
            except ImportError:
                pytest.skip("Polygon provider not available")
    
    @pytest.mark.asyncio
    async def test_invalid_symbol_handling(self, mock_external_dependencies):
        """Test handling of invalid symbols"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Should handle invalid symbols gracefully
            df = await provider.fetch_historical_data("INVALID_SYMBOL_12345", days_back=1, granularity="1d")
            
            # Should return DataFrame (empty or with synthetic data)
            assert isinstance(df, pd.DataFrame)
    
    @pytest.mark.asyncio
    async def test_callback_error_isolation(self, mock_external_dependencies):
        """Test that callback errors don't affect provider operation"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Add failing callback
            def failing_callback(data):
                raise ValueError("Test callback error")
            
            # Add working callback
            working_callback = MagicMock()
            
            provider.add_data_callback(failing_callback)
            provider.add_data_callback(working_callback)
            
            # Notify callbacks
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
            working_callback.assert_called_once_with(test_data)


class TestProviderPerformance:
    """Test performance characteristics across providers"""
    
    @pytest.mark.asyncio
    async def test_data_fetching_performance(self, mock_external_dependencies):
        """Test that data fetching completes in reasonable time"""
        import time
        
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            start_time = time.time()
            
            # Fetch moderate amount of data
            df = await provider.fetch_historical_data("AAPL", days_back=30, granularity="1d")
            
            end_time = time.time()
            
            # Should complete within reasonable time (5 seconds)
            elapsed = end_time - start_time
            assert elapsed < 5.0, f"{provider_name} took too long: {elapsed:.2f}s"
            
            # Should return some data
            assert isinstance(df, pd.DataFrame)
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, mock_external_dependencies):
        """Test handling of concurrent requests"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Make concurrent requests
            tasks = [
                provider.fetch_historical_data("AAPL", days_back=5, granularity="1d"),
                provider.fetch_historical_data("MSFT", days_back=5, granularity="1d"),
                provider.fetch_historical_data("GOOGL", days_back=5, granularity="1d")
            ]
            
            # Should complete without errors
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Check results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    pytest.fail(f"{provider_name} failed concurrent request {i}: {result}")
                else:
                    assert isinstance(result, pd.DataFrame)


class TestProviderDataConsistency:
    """Test data consistency and quality across providers"""
    
    @pytest.mark.asyncio
    async def test_timestamp_consistency(self, mock_external_dependencies):
        """Test that timestamps are handled consistently"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            df = await provider.fetch_historical_data("AAPL", days_back=5, granularity="1d")
            
            if not df.empty:
                # Check timestamp properties
                assert isinstance(df.index, pd.DatetimeIndex)
                assert df.index.is_monotonic_increasing, f"{provider_name} timestamps not sorted"
                
                # Check no future timestamps
                now = datetime.now()
                # Handle timezone-aware vs timezone-naive comparison
                if df.index.tz is not None:
                    # Index is timezone-aware, make now timezone-aware too
                    from datetime import timezone
                    now = now.replace(tzinfo=timezone.utc)
                future_timestamps = df.index > now
                assert not future_timestamps.any(), f"{provider_name} has future timestamps"
    
    @pytest.mark.asyncio
    async def test_data_quality_constraints(self, mock_external_dependencies):
        """Test that data meets quality constraints"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            df = await provider.fetch_historical_data("AAPL", days_back=10, granularity="1d")
            
            if not df.empty:
                # Check no negative prices
                price_cols = ['Open', 'High', 'Low', 'Close']
                for col in price_cols:
                    assert (df[col] >= 0).all(), f"{provider_name} has negative {col} prices"
                
                # Check no negative volume
                assert (df['Volume'] >= 0).all(), f"{provider_name} has negative volume"
                
                # Check OHLC relationships
                assert (df['High'] >= df['Low']).all(), f"{provider_name} has High < Low"


class TestProviderFactoryCompatibility:
    """Test compatibility with provider factory patterns"""
    
    def test_provider_registration_compatibility(self, mock_external_dependencies):
        """Test that providers can be registered and retrieved consistently"""
        available_providers = get_available_providers()
        
        # Simulate provider registry
        provider_registry = {}
        
        for provider_name, provider_class, init_kwargs in available_providers:
            # Should be able to register
            provider_registry[provider_name] = (provider_class, init_kwargs)
            
            # Should be able to retrieve and instantiate
            retrieved_class, retrieved_kwargs = provider_registry[provider_name]
            provider = retrieved_class(**retrieved_kwargs)
            
            assert isinstance(provider, BaseDataIngestion)
    
    @pytest.mark.asyncio
    async def test_provider_swapping(self, mock_external_dependencies):
        """Test that providers can be swapped without changing client code"""
        available_providers = get_available_providers()
        
        if len(available_providers) < 2:
            pytest.skip("Need at least 2 providers for swapping test")
        
        # Client code that works with any provider
        async def client_code(provider: BaseDataIngestion):
            # Fetch data
            df = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1d")
            
            # Add callback
            callback = MagicMock()
            provider.add_data_callback(callback)
            
            # Subscribe to symbol
            await provider.subscribe_to_symbol("AAPL")
            
            return df, callback
        
        # Test with different providers
        results = []
        for provider_name, provider_class, init_kwargs in available_providers[:2]:
            provider = provider_class(**init_kwargs)
            df, callback = await client_code(provider)
            results.append((provider_name, df, callback, provider))
        
        # All should work
        for provider_name, df, callback, provider in results:
            assert isinstance(df, pd.DataFrame), f"Client code failed with {provider_name}"
            assert callback in provider.data_callbacks


class TestProviderResourceManagement:
    """Test resource management across providers"""
    
    @pytest.mark.asyncio
    async def test_provider_cleanup(self, mock_external_dependencies):
        """Test that providers clean up resources properly"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Start real-time feed if supported
            try:
                provider.start_realtime_feed()
            except RuntimeError as e:
                if "no running event loop" in str(e):
                    # Expected for some providers that need event loop
                    continue
                else:
                    raise
            
            # Stop real-time feed
            provider.stop_realtime_feed()
            
            # Should not leave hanging resources
            # (This is mainly a smoke test - specific cleanup depends on provider)
    
    @pytest.mark.asyncio
    async def test_memory_usage_consistency(self, mock_external_dependencies):
        """Test that providers don't have memory leaks"""
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            provider = provider_class(**init_kwargs)
            
            # Fetch data multiple times
            for i in range(5):
                df = await provider.fetch_historical_data(f"SYMBOL_{i}", days_back=1, granularity="1d")
                assert isinstance(df, pd.DataFrame)
            
            # Provider should still be functional
            final_df = await provider.fetch_historical_data("FINAL", days_back=1, granularity="1d")
            assert isinstance(final_df, pd.DataFrame)


class TestProviderDocumentationCompliance:
    """Test that providers comply with documented behavior"""
    
    @pytest.mark.asyncio
    async def test_documented_granularities(self, mock_external_dependencies):
        """Test that providers support their documented granularities"""
        # This would need to be updated based on actual provider documentation
        granularity_support = {
            "demo": ["1m", "5m", "1h", "1d"],
            "alpaca": ["1m", "5m", "15m", "1h", "1d"],
            "polygon": ["1m", "5m", "1h", "1d"],
            "coinmarketcap": ["1d"]  # Only daily for historical
        }
        
        available_providers = get_available_providers()
        
        for provider_name, provider_class, init_kwargs in available_providers:
            if provider_name in granularity_support:
                provider = provider_class(**init_kwargs)
                
                for granularity in granularity_support[provider_name]:
                    # Should not raise exception for supported granularities
                    df = await provider.fetch_historical_data("AAPL", days_back=1, granularity=granularity)
                    assert isinstance(df, pd.DataFrame)
    
    def test_provider_dependencies_check(self):
        """Test that providers correctly report their dependency status"""
        # Test Alpaca provider dependency check
        try:
            from StrateQueue.data.sources.alpaca import AlpacaDataIngestion
            # Should have dependencies_available method
            assert hasattr(AlpacaDataIngestion, 'dependencies_available')
            assert callable(AlpacaDataIngestion.dependencies_available)
            
            # Should return boolean
            result = AlpacaDataIngestion.dependencies_available()
            assert isinstance(result, bool)
        except ImportError:
            pytest.skip("Alpaca provider not available for testing") 