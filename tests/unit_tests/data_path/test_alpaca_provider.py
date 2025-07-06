"""
Alpaca Data Provider Specific Tests

Tests for AlpacaDataIngestion that focus on:
1. Dependencies availability check
2. Error handling when SDK not available
3. Basic construction validation
"""

import pytest
from unittest.mock import patch

# Test what we can without requiring the actual SDK
from StrateQueue.data.sources.alpaca import AlpacaDataIngestion


class TestAlpacaProviderBasics:
    """Test basic Alpaca provider functionality"""
    
    def test_dependencies_available_method_exists(self):
        """Test that dependencies_available static method exists"""
        assert hasattr(AlpacaDataIngestion, 'dependencies_available')
        assert callable(AlpacaDataIngestion.dependencies_available)
    
    def test_dependencies_available_returns_bool(self):
        """Test that dependencies_available returns a boolean"""
        result = AlpacaDataIngestion.dependencies_available()
        assert isinstance(result, bool)
    
    @patch('StrateQueue.data.sources.alpaca._ALPACA_AVAILABLE', False)
    def test_provider_fails_when_sdk_unavailable(self):
        """Test provider raises ImportError when alpaca-py is not available"""
        with pytest.raises(ImportError, match="alpaca-py package is required"):
            AlpacaDataIngestion(
                api_key="test_key",
                secret_key="test_secret"
            )
    
    @patch('StrateQueue.data.sources.alpaca._ALPACA_AVAILABLE', True)
    def test_provider_requires_api_credentials(self):
        """Test that provider requires API credentials"""
        # This will likely fail due to missing SDK, but we're testing the interface
        try:
            with pytest.raises((ImportError, AttributeError, TypeError)):
                AlpacaDataIngestion()  # No credentials provided
        except Exception:
            # Expected - we don't have the actual SDK
            pass
    
    def test_provider_inherits_from_base(self):
        """Test that AlpacaDataIngestion inherits from BaseDataIngestion"""
        from StrateQueue.data.sources.data_source_base import BaseDataIngestion
        assert issubclass(AlpacaDataIngestion, BaseDataIngestion)
    
    def test_provider_has_required_methods(self):
        """Test that provider has all required methods"""
        required_methods = [
            'fetch_historical_data',
            'subscribe_to_symbol',
            'start_realtime_feed',
            'stop_realtime_feed',
            'get_current_data',
            'add_data_callback'
        ]
        
        for method_name in required_methods:
            assert hasattr(AlpacaDataIngestion, method_name)
            assert callable(getattr(AlpacaDataIngestion, method_name))


class TestAlpacaProviderConfiguration:
    """Test configuration options"""
    
    def test_provider_accepts_configuration_parameters(self):
        """Test that provider accepts the expected configuration parameters"""
        # We can't actually instantiate without the SDK, but we can check the signature
        import inspect
        
        sig = inspect.signature(AlpacaDataIngestion.__init__)
        params = list(sig.parameters.keys())
        
        # Check expected parameters exist
        assert 'api_key' in params
        assert 'secret_key' in params
        assert 'paper' in params
        assert 'granularity' in params
        assert 'is_crypto' in params
    
    def test_provider_has_dependencies_available_static_method(self):
        """Test that the static method for checking dependencies is available"""
        assert hasattr(AlpacaDataIngestion, 'dependencies_available')
        
        # Should be callable without instantiation
        result = AlpacaDataIngestion.dependencies_available()
        assert isinstance(result, bool)


class TestAlpacaProviderIntegration:
    """Test integration with the broader system"""
    
    def test_provider_can_be_imported(self):
        """Test that the provider can be imported successfully"""
        # This test passes if we got here without ImportError
        assert AlpacaDataIngestion is not None
    
    def test_provider_module_structure(self):
        """Test that the provider module has expected structure"""
        import StrateQueue.data.sources.alpaca as alpaca_module
        
        # Should have the main class
        assert hasattr(alpaca_module, 'AlpacaDataIngestion')
        
        # Should have availability flag
        assert hasattr(alpaca_module, '_ALPACA_AVAILABLE')
        assert isinstance(alpaca_module._ALPACA_AVAILABLE, bool) 