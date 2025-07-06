"""
Polygon Data Provider Specific Tests

Tests for PolygonDataIngestion that focus on:
1. HTTP REST API integration
2. WebSocket message handling
3. Error handling and retries
4. Granularity parsing and validation
5. Response data transformation
"""

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, Mock
import pytest
import pandas as pd
import requests

from StrateQueue.data.sources.polygon import PolygonDataIngestion
from StrateQueue.data.sources.data_source_base import MarketData


class TestPolygonProviderConstruction:
    """Test Polygon provider construction and initialization"""
    
    def test_provider_initialization(self):
        """Test provider initializes with correct attributes"""
        provider = PolygonDataIngestion(api_key="test_api_key")
        
        assert provider.api_key == "test_api_key"
        assert provider.rest_base_url == "https://api.polygon.io"
        assert provider.ws_url == "wss://socket.polygon.io/stocks"
        assert provider.ws is None
        assert provider.is_connected is False
    
    def test_provider_initialization_with_custom_params(self):
        """Test provider can be initialized with custom parameters"""
        provider = PolygonDataIngestion(api_key="custom_key")
        
        assert provider.api_key == "custom_key"
        assert hasattr(provider, 'current_bars')
        assert hasattr(provider, 'historical_data')
        assert hasattr(provider, 'data_callbacks')


class TestPolygonHistoricalDataFetching:
    """Test historical data fetching via REST API"""
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_historical_data_success(self, mock_get):
        """Test successful historical data fetching"""
        # Mock successful response
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1640995200000,  # Timestamp in ms
                    "o": 100.0,  # Open
                    "h": 101.0,  # High
                    "l": 99.0,   # Low
                    "c": 100.5,  # Close
                    "v": 1000    # Volume
                },
                {
                    "t": 1640995260000,
                    "o": 100.5,
                    "h": 101.5,
                    "l": 99.5,
                    "c": 101.0,
                    "v": 1100
                }
            ]
        }
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Verify request was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "AAPL" in call_args[0][0]  # URL contains symbol
        assert call_args[1]["params"]["apikey"] == "test_key"
        
        # Verify DataFrame structure
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['Open', 'High', 'Low', 'Close', 'Volume']
        assert isinstance(result.index, pd.DatetimeIndex)
        
        # Verify data values
        assert result.iloc[0]['Open'] == 100.0
        assert result.iloc[0]['Close'] == 100.5
        assert result.iloc[0]['Volume'] == 1000
        
        # Verify data is cached
        assert "AAPL" in provider.historical_data
        assert provider.historical_data["AAPL"] is result
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_historical_data_no_results(self, mock_get):
        """Test handling when no data is returned"""
        # Mock response with no results
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"status": "OK"}  # No "results" key
        
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        
        result = await provider.fetch_historical_data("INVALID", days_back=1, granularity="1m")
        
        # Should return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_historical_data_http_error(self, mock_get):
        """Test handling of HTTP errors"""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Should return empty DataFrame on error
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_fetch_historical_data_network_error(self, mock_get):
        """Test handling of network errors"""
        # Mock network error
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        provider = PolygonDataIngestion(api_key="test_key")
        
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Should return empty DataFrame on error
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_granularity_parsing_in_url(self, mock_get):
        """Test that granularity is correctly parsed and included in URL"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Test different granularities
        test_cases = [
            ("1m", "1/minute"),
            ("5m", "5/minute"),
            ("1h", "1/hour"),
            ("1d", "1/day")
        ]
        
        for granularity, expected_url_part in test_cases:
            await provider.fetch_historical_data("AAPL", days_back=1, granularity=granularity)
            
            # Check that URL contains the expected timespan
            call_args = mock_get.call_args
            url = call_args[0][0]
            assert expected_url_part in url, f"Expected {expected_url_part} in URL for {granularity}"


class TestPolygonWebSocketHandling:
    """Test WebSocket message handling"""
    
    def test_websocket_message_processing(self):
        """Test processing of WebSocket aggregate messages"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock callback
        callback_mock = MagicMock()
        provider.add_data_callback(callback_mock)
        
        # Mock WebSocket message
        message = json.dumps([
            {
                "ev": "AM",  # Aggregate message
                "sym": "AAPL",
                "s": 1640995200000,  # Start timestamp
                "o": 100.0,  # Open
                "h": 101.0,  # High
                "l": 99.0,   # Low
                "c": 100.5,  # Close
                "v": 1000    # Volume
            }
        ])
        
        # Process message
        provider._on_ws_message(None, message)
        
        # Verify data was processed
        assert "AAPL" in provider.current_bars
        market_data = provider.current_bars["AAPL"]
        assert market_data.symbol == "AAPL"
        assert market_data.open == 100.0
        assert market_data.high == 101.0
        assert market_data.low == 99.0
        assert market_data.close == 100.5
        assert market_data.volume == 1000
        
        # Verify callback was called
        callback_mock.assert_called_once_with(market_data)
    
    def test_websocket_message_non_aggregate(self):
        """Test handling of non-aggregate WebSocket messages"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock callback
        callback_mock = MagicMock()
        provider.add_data_callback(callback_mock)
        
        # Mock non-aggregate message
        message = json.dumps([
            {
                "ev": "T",  # Trade message (not aggregate)
                "sym": "AAPL",
                "p": 100.0,  # Price
                "s": 100     # Size
            }
        ])
        
        # Process message
        provider._on_ws_message(None, message)
        
        # Should not process non-aggregate messages
        assert "AAPL" not in provider.current_bars
        callback_mock.assert_not_called()
    
    def test_websocket_message_invalid_json(self):
        """Test handling of invalid JSON messages"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock callback
        callback_mock = MagicMock()
        provider.add_data_callback(callback_mock)
        
        # Invalid JSON message
        message = "invalid json"
        
        # Should not crash
        provider._on_ws_message(None, message)
        
        # Should not have processed anything
        assert len(provider.current_bars) == 0
        callback_mock.assert_not_called()
    
    def test_websocket_message_missing_fields(self):
        """Test handling of messages with missing required fields"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock callback
        callback_mock = MagicMock()
        provider.add_data_callback(callback_mock)
        
        # Message with missing fields
        message = json.dumps([
            {
                "ev": "AM",
                "sym": "AAPL",
                # Missing required fields like 'o', 'h', 'l', 'c', 'v'
            }
        ])
        
        # Should not crash
        provider._on_ws_message(None, message)
        
        # Should not have processed anything
        assert len(provider.current_bars) == 0
        callback_mock.assert_not_called()


class TestPolygonWebSocketLifecycle:
    """Test WebSocket connection lifecycle"""
    
    def test_websocket_open_handler(self):
        """Test WebSocket open event handling"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock WebSocket
        mock_ws = MagicMock()
        
        # Call open handler
        provider._on_ws_open(mock_ws)
        
        # Should set connected status
        assert provider.is_connected is True
        
        # Should send authentication message
        mock_ws.send.assert_called_once()
        sent_message = mock_ws.send.call_args[0][0]
        auth_data = json.loads(sent_message)
        assert auth_data["action"] == "auth"
        assert auth_data["params"] == "test_key"
    
    def test_websocket_close_handler(self):
        """Test WebSocket close event handling"""
        provider = PolygonDataIngestion(api_key="test_key")
        provider.is_connected = True
        
        # Call close handler
        provider._on_ws_close(None, None, None)
        
        # Should set connected status to False
        assert provider.is_connected is False
    
    def test_websocket_error_handler(self):
        """Test WebSocket error event handling"""
        provider = PolygonDataIngestion(api_key="test_key")
        provider.is_connected = True
        
        # Call error handler
        provider._on_ws_error(None, "Test error")
        
        # Should set connected status to False
        assert provider.is_connected is False
    
    @patch('websocket.WebSocketApp')
    def test_start_realtime_feed(self, mock_websocket_app):
        """Test starting real-time feed"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Mock WebSocket app
        mock_ws_app = MagicMock()
        mock_websocket_app.return_value = mock_ws_app
        
        # Start real-time feed
        provider.start_realtime_feed()
        
        # Verify WebSocket was created with correct parameters
        mock_websocket_app.assert_called_once_with(
            "wss://socket.polygon.io/stocks",
            on_message=provider._on_ws_message,
            on_error=provider._on_ws_error,
            on_close=provider._on_ws_close,
            on_open=provider._on_ws_open
        )
        
        # Verify WebSocket was started
        mock_ws_app.run_forever.assert_called_once()


class TestPolygonSubscription:
    """Test symbol subscription functionality"""
    
    @pytest.mark.asyncio
    async def test_subscribe_to_symbol(self):
        """Test subscribing to a symbol"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Subscribe to symbol
        await provider.subscribe_to_symbol("AAPL")
        
        # Should add to subscribed symbols
        assert "AAPL" in provider.subscribed_symbols
    
    @pytest.mark.asyncio
    async def test_subscribe_multiple_symbols(self):
        """Test subscribing to multiple symbols"""
        provider = PolygonDataIngestion(api_key="test_key")
        
        # Subscribe to multiple symbols
        await provider.subscribe_to_symbol("AAPL")
        await provider.subscribe_to_symbol("GOOGL")
        await provider.subscribe_to_symbol("MSFT")
        
        # Should have all symbols
        assert "AAPL" in provider.subscribed_symbols
        assert "GOOGL" in provider.subscribed_symbols
        assert "MSFT" in provider.subscribed_symbols
        assert len(provider.subscribed_symbols) == 3


class TestPolygonDataTransformation:
    """Test data transformation and validation"""
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_timestamp_conversion(self, mock_get):
        """Test timestamp conversion from milliseconds to datetime"""
        # Mock response with timestamp in milliseconds
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1640995200000,  # Jan 1, 2022 00:00:00 UTC
                    "o": 100.0,
                    "h": 101.0,
                    "l": 99.0,
                    "c": 100.5,
                    "v": 1000
                }
            ]
        }
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Verify timestamp conversion
        assert isinstance(result.index[0], pd.Timestamp)
        expected_time = datetime.fromtimestamp(1640995200000 / 1000)
        assert result.index[0] == expected_time
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_data_type_conversion(self, mock_get):
        """Test proper data type conversion"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {
            "results": [
                {
                    "t": 1640995200000,
                    "o": "100.0",    # String values should be converted
                    "h": "101.0",
                    "l": "99.0",
                    "c": "100.5",
                    "v": "1000"
                }
            ]
        }
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Verify data types
        assert pd.api.types.is_numeric_dtype(result['Open'])
        assert pd.api.types.is_numeric_dtype(result['High'])
        assert pd.api.types.is_numeric_dtype(result['Low'])
        assert pd.api.types.is_numeric_dtype(result['Close'])
        assert pd.api.types.is_numeric_dtype(result['Volume'])
    
    @patch('requests.get')
    @pytest.mark.asyncio
    async def test_column_naming_consistency(self, mock_get):
        """Test that column names are consistent with BaseDataIngestion contract"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
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
        mock_get.return_value = mock_response
        
        provider = PolygonDataIngestion(api_key="test_key")
        result = await provider.fetch_historical_data("AAPL", days_back=1, granularity="1m")
        
        # Verify exact column names
        expected_columns = {'Open', 'High', 'Low', 'Close', 'Volume'}
        assert set(result.columns) == expected_columns 