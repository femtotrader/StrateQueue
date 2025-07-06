from __future__ import annotations

"""
Alpaca Market Data Source

Provides historical and real-time market data (stocks & crypto) via Alpaca's
Market Data API.  Design mirrors the existing Polygon & Yahoo providers so the
rest of StrateQueue can treat it interchangeably.

Requirements
------------
    pip install "alpaca-py>=0.12"  # official SDK

Environment variables recognised (kept consistent with broker helpers):
    ALPACA_API_KEY
    ALPACA_SECRET_KEY
    ALPACA_PAPER           # any truthy value ⇒ use free/paper IEX feed
                            # falsy ⇒ use live/SIP feed (requires paid plan)

Limitations
-----------
* Only bar (aggregate) data is supported for now.
* Granularity mapping supports 1m / 5m / 15m / 1h / 1d.  Extend as needed.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import pandas as pd

from .data_source_base import BaseDataIngestion, MarketData

# Conditional import so StrateQueue still imports if SDK is missing
try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.timeframe import TimeFrame
    from alpaca.data.requests import StockBarsRequest, CryptoBarsRequest
    from alpaca.data.enums import DataFeed, Adjustment as _Adjustment
    try:
        from alpaca.data.historical import CryptoHistoricalDataClient
    except ImportError:
        CryptoHistoricalDataClient = None                       # optional
    # Stock streaming client – optional (requires 'alpaca-py[async]')
    try:
        from alpaca.data.live import StockDataStream
    except ImportError:
        StockDataStream = None                               # optional

    try:
        from alpaca.data.live import CryptoDataStream
    except ImportError:
        CryptoDataStream = None                              # optional
    _ALPACA_AVAILABLE = True
except ImportError:
    _ALPACA_AVAILABLE = False
    # Set to None for testing/mocking purposes
    StockHistoricalDataClient = None
    TimeFrame = None
    StockBarsRequest = None
    CryptoBarsRequest = None
    DataFeed = None
    _Adjustment = None
    StockDataStream = None
    CryptoHistoricalDataClient = None
    CryptoDataStream = None

logger = logging.getLogger(__name__)


class AlpacaDataIngestion(BaseDataIngestion):
    """Alpaca Market Data ingestion (historical + realtime)."""

    # ---------------------------------------------------------------------
    # Public helpers
    # ---------------------------------------------------------------------
    @staticmethod
    def dependencies_available() -> bool:
        """Return True if *alpaca-py* SDK is importable."""
        return _ALPACA_AVAILABLE

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    def __init__(
        self,
        api_key: str,
        secret_key: str,
        *,
        paper: bool = True,
        granularity: str = "1m",
        is_crypto: bool = False,
    ) -> None:
        if not _ALPACA_AVAILABLE:
            raise ImportError(
                "alpaca-py package is required for AlpacaDataIngestion.\n"
                "Install via: pip install 'alpaca-py>=0.12'"
            )

        super().__init__()

        self.api_key = api_key
        self.secret_key = secret_key
        self.paper = paper
        self.is_crypto = is_crypto
        self.default_granularity = granularity

        # Map StrateQueue granularities → Alpaca TimeFrame enum
        # The alpaca-py TimeFrame enum uses names like Minute, Minute5, Minute15, Minute30, Hour, Day, Week, etc.
        # Build the mapping defensively to avoid AttributeError when certain granularities are not present in the
        # installed SDK version (older versions omit some convenience constants).

        self._tf_map: Dict[str, TimeFrame] = {
            "1m": TimeFrame.Minute,
            "5m": getattr(TimeFrame, "Minute5", TimeFrame.Minute),
            "15m": getattr(TimeFrame, "Minute15", TimeFrame.Minute),
            "1h": TimeFrame.Hour,
            "1d": TimeFrame.Day,
        }

        # Optional, only add if the enum member exists in current alpaca-py version
        if hasattr(TimeFrame, "Minute2"):
            self._tf_map["2m"] = TimeFrame.Minute2
        if hasattr(TimeFrame, "Minute10"):
            self._tf_map["10m"] = TimeFrame.Minute10
        if hasattr(TimeFrame, "Minute30"):
            self._tf_map["30m"] = TimeFrame.Minute30
        if hasattr(TimeFrame, "Hour2"):
            self._tf_map["2h"] = TimeFrame.Hour2
        if hasattr(TimeFrame, "Hour4"):
            self._tf_map["4h"] = TimeFrame.Hour4
        if hasattr(TimeFrame, "Week"):
            self._tf_map["1w"] = TimeFrame.Week

        if granularity not in self._tf_map:
            msg = (
                f"Granularity '{granularity}' not supported by Alpaca provider. "
                f"Supported: {list(self._tf_map.keys())}"
            )
            raise ValueError(msg)

        # Clients ----------------------------------------------------
        if self.is_crypto:
            if CryptoHistoricalDataClient is None or CryptoDataStream is None:
                raise ImportError("Crypto feed not available in installed alpaca-py")
            self._hist_client = CryptoHistoricalDataClient(api_key, secret_key)
            feed = "us"  # default crypto feed label – Alpaca only offers one
            self._stream: CryptoDataStream = CryptoDataStream(api_key, secret_key, feed=feed)  # type: ignore
        else:
            # Choose IEX (free/paper) or SIP (live) feed explicitly
            feed_enum = DataFeed.IEX if paper else DataFeed.SIP

            # Store for later use in _build_bars_request
            self._feed_enum = feed_enum

            # Historical client – feed handled at request level, SDK ctor has no 'feed' param.
            self._hist_client = StockHistoricalDataClient(api_key, secret_key)

            if StockDataStream is None:
                raise ImportError(
                    "alpaca-py installed without streaming support. "
                    "Install with: pip install 'alpaca-py[async]'"
                )
            self._stream: StockDataStream = StockDataStream(api_key, secret_key, feed=feed_enum)  # type: ignore
            feed = feed_enum.value  # For logging clarity

        # Prepare reusable bar handler
        async def _handle_bar(bar):  # noqa: N802 – callback sig
            # Ensure consistent timezone handling - convert to timezone-naive datetime
            timestamp = bar.timestamp
            if timestamp.tzinfo is not None:
                timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
            
            md = MarketData(
                symbol=bar.symbol,
                timestamp=timestamp,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
            )
            self.current_bars[md.symbol] = md
            self._notify_callbacks(md)

        self._on_bar = _handle_bar  # store for later subscription

        # Background task handle for run_forever
        self._stream_task: Optional[asyncio.Task] = None

        logger.info(
            "AlpacaDataIngestion initialised (paper=%s, crypto=%s, granularity=%s, feed=%s)",
            paper,
            is_crypto,
            granularity,
            feed,
        )

    def is_running(self) -> bool:
        """Check if the real-time feed is currently running"""
        return self._stream_task is not None and not self._stream_task.done()

    def _build_bars_request(self, symbol: str, tf_enum: TimeFrame, start: datetime, end: datetime):
        """Build appropriate bars request object for historical data API"""
        if self.is_crypto:
            return CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_enum,
                start=start,
                end=end
            )
        else:
            return StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf_enum,
                start=start,
                end=end,
                feed=getattr(self, "_feed_enum", None),
                adjustment=getattr(_Adjustment, 'RAW', getattr(_Adjustment, 'Raw', None)) or _Adjustment.RAW,
            )

    # ------------------------------------------------------------------
    # Historical data ---------------------------------------------------
    # ------------------------------------------------------------------
    async def fetch_historical_data(
        self,
        symbol: str,
        days_back: int = 30,
        granularity: str = "1m",
    ) -> pd.DataFrame:
        """Fetch **bars** using Alpaca Historical API.

        *symbol* is sent to `/v2/stocks/{symbol}/bars` or `/v2/crypto/bars`.
        """
        if granularity not in self._tf_map:
            raise ValueError(
                f"Granularity '{granularity}' not supported. Supported: {list(self._tf_map)}"
            )
        tf_enum = self._tf_map[granularity]

        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)

        # Wrap sync SDK call in thread executor to avoid blocking event loop
        def _download():
            # Build request object for new API
            request = self._build_bars_request(symbol, tf_enum, start, end)
            
            if self.is_crypto:
                bars = self._hist_client.get_crypto_bars(request)
            else:
                bars = self._hist_client.get_stock_bars(request)
                
            return bars.df if hasattr(bars, "df") else pd.DataFrame()

        df: pd.DataFrame = await asyncio.to_thread(_download)
        if df.empty:
            logger.warning("Alpaca returned no data for %s (%s)", symbol, granularity)
            return df

        # Normalise columns ------------------------------------------------
        rename_cols = {
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
        df.rename(columns=rename_cols, inplace=True)

        # ── NEW: flatten MultiIndex returned by Alpaca ─────────────────────
        if isinstance(df.index, pd.MultiIndex):
            # Keep only the timestamp level → simplifies downstream handling
            try:
                df.index = df.index.droplevel(0)
            except (KeyError, ValueError):
                # Fallback for SDK versions where droplevel fails
                df.reset_index(inplace=True)
                ts_col = next(
                    (c for c in ("timestamp", "Timestamp", "time") if c in df.columns),
                    None,
                )
                if ts_col:
                    df.set_index(ts_col, inplace=True)

        # Ensure datetime index & sort
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(None)
        df.sort_index(inplace=True)

        self.historical_data[symbol] = df
        logger.info(
            "Alpaca: fetched %d bars for %s (%s)", len(df), symbol, granularity
        )
        return df

    # ------------------------------------------------------------------
    # Real-time feed -----------------------------------------------------
    # ------------------------------------------------------------------
    def _configure_stream(self):
        """(Deprecated) left for backward compatibility – no-op now."""
        pass

    # Public API ---------------------------------------------------------
    async def subscribe_to_symbol(self, symbol: str):
        """Subscribe to live bars for *symbol*. Must be awaited."""
        try:
            sub_func = self._stream.subscribe_bars
            if asyncio.iscoroutinefunction(sub_func):
                await sub_func(self._on_bar, symbol)
            else:
                # Synchronous subscribe
                sub_func(self._on_bar, symbol)
            logger.debug("Alpaca subscribed to %s", symbol)
        except Exception as e:
            logger.error(f"Failed to subscribe to {symbol}: {e}")
            raise

    def start_realtime_feed(self):
        """Start the real-time feed (runs in background)."""
        if self.is_running():
            logger.warning("Real-time feed is already running")
            return
            
        if self._stream is None:
            logger.warning("No stream client available – cannot start real-time feed")
            return

        # Create and start the background task using the internal coroutine
        if hasattr(self._stream, "_run_forever"):
            self._stream_task = asyncio.create_task(self._stream._run_forever())
        else:
            # Fallback: run() is blocking, so start in executor
            loop = asyncio.get_event_loop()
            self._stream_task = loop.run_in_executor(None, self._stream.run)
        logger.info("Alpaca real-time feed started")

    def stop_realtime_feed(self):
        """Stop the real-time feed."""
        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
            self._stream_task = None
            logger.info("Alpaca real-time feed stopped")
        else:
            logger.debug("Real-time feed was not running")

    # Convenience helpers -----------------------------------------------
    def set_update_interval_from_granularity(self, granularity: str):
        """Method kept for API parity with Demo provider (no-op here)."""
        pass 