"""
CoinMarketCap Data Provider Specific Tests

Tests for CoinMarketCapDataIngestion that focus on:
1. API key validation and environment variable handling
2. Symbol ID caching mechanism
3. Granularity restrictions and validation
4. Historical vs real-time data handling
5. Error handling for various API responses
6. Rate limiting and authentication errors
"""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock
import pytest
import pandas as pd
import requests

from StrateQueue.data.sources.coinmarketcap import CoinMarketCapDataIngestion
from StrateQueue.data.sources.data_source_base import MarketData


class TestCoinMarketCapProviderConstruction:
    """Test CoinMarketCap provider construction and initialization"""
    
    def test_provider_initialization_with_api_key(self):
        """Test provider initializes with provided API key"""
        provider = CoinMarketCapDataIngestion(api_key="test_api_key", granularity="1d")
        
        assert provider.api_key == "test_api_key"
        assert provider.granularity == "1d"
        assert provider.rest_base_url == "https://pro-api.coinmarketcap.com"
        assert provider.ws_base_url is None  # CMC doesn't have WebSocket
    
    @patch.dict(os.environ, {'CMC_API_KEY': 'env_api_key'})
    def test_provider_initialization_with_env_var(self):
        """Test provider uses environment variable when no API key provided"""
        provider = CoinMarketCapDataIngestion(granularity="1d")
        
        assert provider.api_key == "env_api_key"
    
    @patch.dict(os.environ, {}, clear=True)
    def test_provider_initialization_no_api_key(self):
        """Test provider raises error when no API key available"""
        with pytest.raises(ValueError, match="CoinMarketCap API key is required"):
            CoinMarketCapDataIngestion(granularity="1d")
    
    def test_provider_initialization_invalid_granularity(self):
        """Test provider raises error for sub-minute granularity"""
        with pytest.raises(Exception, match="CoinMarketCap does not support 30s granularity"):
            CoinMarketCapDataIngestion(api_key="test_key", granularity="30s")
    
    def test_provider_initialization_valid_granularities(self):
        """Test provider accepts valid granularities"""
        valid_granularities = ["1m", "5m", "1h", "1d"]
        
        for granularity in valid_granularities:
            provider = CoinMarketCapDataIngestion(api_key="test_key", granularity=granularity)
            assert provider.granularity == granularity
    
    def test_update_interval_calculation(self):
        """Test update interval is set correctly based on granularity"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1m")
        assert provider.update_interval == 60  # Always 60 seconds for CMC
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1h")
        assert provider.update_interval == 60  # Still 60 seconds


class TestCoinMarketCapSymbolIdCaching:
    """Test symbol ID caching mechanism"""
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_symbol_id_success(self, mock_get):
        """Test successful symbol ID fetching"""
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": [
                {
                    "id": 1,
                    "name": "Bitcoin",
                    "symbol": "BTC"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        symbol_id = await provider._fetch_symbol_id("BTC")
        
        assert symbol_id == 1
        assert provider.symbol_to_id["BTC"] == 1
        
        # Verify request was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[1]["headers"]["X-CMC_PRO_API_KEY"] == "test_key"
        assert call_args[1]["params"]["symbol"] == "BTC"
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_symbol_id_cached(self, mock_get):
        """Test symbol ID caching works correctly"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        # Pre-populate cache
        provider.symbol_to_id["BTC"] = 1
        
        symbol_id = await provider._fetch_symbol_id("BTC")
        
        assert symbol_id == 1
        # Should not make HTTP request
        mock_get.assert_not_called()
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_symbol_id_not_found(self, mock_get):
        """Test handling when symbol is not found"""
        # Mock response with no data
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        symbol_id = await provider._fetch_symbol_id("INVALID")
        
        assert symbol_id is None
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_symbol_id_http_error(self, mock_get):
        """Test handling of HTTP errors during symbol ID fetch"""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        symbol_id = await provider._fetch_symbol_id("BTC")
        
        assert symbol_id is None


class TestCoinMarketCapHistoricalData:
    """Test historical data fetching"""
    
    @pytest.mark.asyncio
    async def test_fetch_historical_data_daily_supported(self):
        """Test that daily granularity is supported for historical data"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        with patch.object(provider, '_fetch_daily_historical_data') as mock_fetch:
            mock_fetch.return_value = pd.DataFrame()
            
            await provider.fetch_historical_data("BTC", days_back=30, granularity="1d")
            
            mock_fetch.assert_called_once_with("BTC", 30)
    
    @pytest.mark.asyncio
    async def test_fetch_historical_data_intraday_unsupported(self):
        """Test that intraday granularities are not supported for historical data"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        with pytest.raises(Exception, match="CoinMarketCap does not support 1m historical data"):
            await provider.fetch_historical_data("BTC", days_back=30, granularity="1m")
    
    @pytest.mark.asyncio
    async def test_fetch_historical_data_sub_minute_unsupported(self):
        """Test that sub-minute granularities are not supported"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        with pytest.raises(Exception, match="CoinMarketCap does not support 30s granularity"):
            await provider.fetch_historical_data("BTC", days_back=30, granularity="30s")
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_success(self, mock_get):
        """Test successful daily historical data fetching"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        # Mock symbol ID fetch
        provider.symbol_to_id["BTC"] = 1
        
        # Mock successful historical data response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": {
                "quotes": [
                    {
                        "timestamp": "2022-01-01T00:00:00.000Z",
                        "quote": {
                            "USD": {
                                "open": 45000.0,
                                "high": 46000.0,
                                "low": 44000.0,
                                "close": 45500.0,
                                "volume": 1000000.0
                            }
                        }
                    },
                    {
                        "timestamp": "2022-01-02T00:00:00.000Z",
                        "quote": {
                            "USD": {
                                "open": 45500.0,
                                "high": 46500.0,
                                "low": 44500.0,
                                "close": 46000.0,
                                "volume": 1100000.0
                            }
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        
        result = await provider._fetch_daily_historical_data("BTC", 30)
        
        # Verify DataFrame structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['Open', 'High', 'Low', 'Close', 'Volume']
        assert isinstance(result.index, pd.DatetimeIndex)
        
        # Verify data values
        assert result.iloc[0]['Open'] == 45000.0
        assert result.iloc[0]['Close'] == 45500.0
        assert result.iloc[0]['Volume'] == 1000000.0
        
        # Verify data is cached
        assert "BTC" in provider.historical_data
        assert provider.historical_data["BTC"] is result
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_symbol_not_found(self, mock_get):
        """Test handling when symbol is not found"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        # Mock symbol ID fetch failure
        with patch.object(provider, '_fetch_symbol_id', return_value=None):
            with pytest.raises(Exception, match="Cannot fetch historical data for INVALID"):
                await provider._fetch_daily_historical_data("INVALID", 30)
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_auth_error(self, mock_get):
        """Test handling of authentication errors"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match="CoinMarketCap API authentication failed"):
            await provider._fetch_daily_historical_data("BTC", 30)
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_rate_limit(self, mock_get):
        """Test handling of rate limit errors"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock 403 response
        mock_response = Mock()
        mock_response.status_code = 403
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match="CoinMarketCap API rate limit exceeded"):
            await provider._fetch_daily_historical_data("BTC", 30)
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_no_data(self, mock_get):
        """Test handling when no historical data is returned"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock response with no data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": {}}  # No quotes
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match="No historical data returned for BTC"):
            await provider._fetch_daily_historical_data("BTC", 30)
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_timeout(self, mock_get):
        """Test handling of timeout errors"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock timeout
        mock_get.side_effect = requests.exceptions.Timeout("Request timeout")
        
        with pytest.raises(Exception, match="CoinMarketCap historical API timeout"):
            await provider._fetch_daily_historical_data("BTC", 30)
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_daily_historical_data_connection_error(self, mock_get):
        """Test handling of connection errors"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock connection error
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        
        with pytest.raises(Exception, match="CoinMarketCap historical API connection error"):
            await provider._fetch_daily_historical_data("BTC", 30)


class TestCoinMarketCapRealTimeData:
    """Test real-time data functionality"""
    
    @patch('requests.get')
    def test_fetch_current_quote_success(self, mock_get):
        """Test successful current quote fetching"""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": {
                "BTC": [
                    {
                        "id": 1,
                        "name": "Bitcoin",
                        "quote": {
                            "USD": {
                                "price": 45000.0,
                                "volume_24h": 1000000.0,
                                "percent_change_24h": 2.5,
                                "last_updated": "2022-01-01T12:00:00.000Z"
                            }
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        result = provider._fetch_current_quote("BTC")
        
        assert isinstance(result, MarketData)
        assert result.symbol == "BTC"
        assert result.close == 45000.0
        assert result.volume == 1000000.0
    
    @patch('requests.get')
    def test_fetch_current_quote_error(self, mock_get):
        """Test error handling in current quote fetching"""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response
        
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        with pytest.raises(Exception, match="CoinMarketCap API returned error status"):
            provider._fetch_current_quote("BTC")
    
    def test_create_simulated_quote(self):
        """Test that simulated quote creation is disabled"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        with pytest.raises(Exception, match="Simulated quotes disabled"):
            provider._create_simulated_quote("BTC")
    
    @pytest.mark.asyncio
    async def test_subscribe_to_symbol(self):
        """Test subscribing to symbols"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        await provider.subscribe_to_symbol("BTC")
        
        assert "BTC" in provider.subscribed_symbols
    
    def test_start_stop_realtime_feed(self):
        """Test starting and stopping real-time feed"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        # Start feed
        provider.start_realtime_feed()
        assert provider.simulation_running is True
        assert provider.simulation_thread is not None
        
        # Stop feed
        provider.stop_realtime_feed()
        assert provider.simulation_running is False


class TestCoinMarketCapDataTransformation:
    """Test data transformation and validation"""
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_timestamp_parsing(self, mock_get):
        """Test timestamp parsing from ISO format"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock response with ISO timestamp
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": {
                "quotes": [
                    {
                        "timestamp": "2022-01-01T00:00:00.000Z",
                        "quote": {
                            "USD": {
                                "open": 45000.0,
                                "high": 46000.0,
                                "low": 44000.0,
                                "close": 45500.0,
                                "volume": 1000000.0
                            }
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        
        result = await provider._fetch_daily_historical_data("BTC", 1)
        
        # Verify timestamp parsing
        assert isinstance(result.index[0], pd.Timestamp)
        assert result.index[0].year == 2022
        assert result.index[0].month == 1
        assert result.index[0].day == 1
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_data_sorting(self, mock_get):
        """Test that data is sorted chronologically"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        provider.symbol_to_id["BTC"] = 1
        
        # Mock response with unsorted data
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "data": {
                "quotes": [
                    {
                        "timestamp": "2022-01-02T00:00:00.000Z",  # Later date first
                        "quote": {
                            "USD": {
                                "open": 45500.0,
                                "high": 46500.0,
                                "low": 44500.0,
                                "close": 46000.0,
                                "volume": 1100000.0
                            }
                        }
                    },
                    {
                        "timestamp": "2022-01-01T00:00:00.000Z",  # Earlier date second
                        "quote": {
                            "USD": {
                                "open": 45000.0,
                                "high": 46000.0,
                                "low": 44000.0,
                                "close": 45500.0,
                                "volume": 1000000.0
                            }
                        }
                    }
                ]
            }
        }
        mock_get.return_value = mock_response
        
        result = await provider._fetch_daily_historical_data("BTC", 2)
        
        # Verify data is sorted chronologically
        assert result.index.is_monotonic_increasing
        assert result.index[0].day == 1  # Jan 1 should be first
        assert result.index[1].day == 2  # Jan 2 should be second


class TestCoinMarketCapGranularityHandling:
    """Test granularity handling and validation"""
    
    def test_granularity_validation_in_constructor(self):
        """Test granularity validation during construction"""
        # Valid granularities should work
        valid_granularities = ["1m", "5m", "15m", "1h", "4h", "1d"]
        for granularity in valid_granularities:
            provider = CoinMarketCapDataIngestion(api_key="test_key", granularity=granularity)
            assert provider.granularity == granularity
        
        # Invalid granularities should raise
        invalid_granularities = ["30s", "15s", "1s"]
        for granularity in invalid_granularities:
            with pytest.raises(Exception, match="CoinMarketCap does not support"):
                CoinMarketCapDataIngestion(api_key="test_key", granularity=granularity)
    
    def test_set_update_interval_from_granularity(self):
        """Test setting update interval based on granularity"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        # Test different granularities
        test_cases = [
            ("1m", 60),
            ("5m", 300),
            ("1h", 3600),
            ("1d", 86400)
        ]
        
        for granularity, expected_interval in test_cases:
            provider.set_update_interval_from_granularity(granularity)
            assert provider.update_interval == expected_interval
    
    def test_set_update_interval_direct(self):
        """Test setting update interval directly"""
        provider = CoinMarketCapDataIngestion(api_key="test_key", granularity="1d")
        
        provider.set_update_interval(120)
        assert provider.update_interval == 120 