#!/usr/bin/env python3
"""
Polygon.io Data Fetcher

This script fetches data from two reliable Polygon.io endpoints:
1. Ticker details (company information, market cap, sector, etc.)
2. Price aggregates (historical price data)

The data is combined into DataFrames that can be saved as CSV for analysis.
"""

import os
import pandas as pd
from polygon import RESTClient
from datetime import datetime, timedelta
from typing import List, Optional
import logging
import time
from pathlib import Path
from polygon_tools.ticker_details import fetch_ticker_details

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API configuration
API_KEY = (
    os.getenv("POLYGON_API_KEY")
    or os.getenv("POLYGON_KEY")
    or "YOUR_POLYGON_KEY_HERE"
)

if API_KEY == "YOUR_POLYGON_KEY_HERE":
    logger.error("Please set POLYGON_API_KEY or POLYGON_KEY environment variable")
    exit(1)

masked_key = f"{API_KEY[:4]}***" if API_KEY else "<not set>"
logger.info("Using API Key: %s", masked_key)

client = RESTClient(API_KEY)

def fetch_price_data(tickers: List[str],
                    from_date: Optional[str] = None,
                    to_date: Optional[str] = None,
                    timespan: str = "day",
                    multiplier: int = 1,
                    output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch historical price data for tickers.

    Args:
        tickers: List of ticker symbols
        from_date: Start date in YYYY-MM-DD format (defaults to 30 days ago)
        to_date: End date in YYYY-MM-DD format (defaults to today)
        timespan: Time span (day, week, month, quarter, year)
        multiplier: Size of the time window
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with price data
    """
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Fetching price data for {len(tickers)} tickers from {from_date} to {to_date}")

    records = []
    for i, ticker in enumerate(tickers):
        try:
            logger.info(f"Processing {i+1}/{len(tickers)}: {ticker}")

            # Get price aggregates
            aggs = client.list_aggs(
                ticker=ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_=from_date,
                to=to_date,
                adjusted=True,
                limit=50000
            )

            for bar in aggs:
                record = {
                    'ticker': ticker,
                    'timestamp': bar.timestamp,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume,
                    'vwap': getattr(bar, 'vwap', None),
                    'transactions': getattr(bar, 'transactions', None),
                }
                records.append(record)

            # Rate limiting
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error fetching price data for {ticker}: {e}")

    df = pd.DataFrame(records)

    # Convert timestamp to datetime
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date

    logger.info(f"Successfully fetched {len(df)} price records")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "price_data.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved price data to {csv_file}")

    return df


def fetch_crypto_snapshots(tickers: Optional[List[str]] = None,
                          output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch comprehensive snapshot data for cryptocurrency tickers.

    This endpoint retrieves a full market snapshot for all crypto tickers (or specific ones)
    in a single API call, consolidating current pricing, volume, trade activity, and today's
    performance metrics. Snapshot data is cleared daily at 12:00 AM EST and begins repopulating
    as exchanges report new data (can start as early as 4:00 AM EST).

    Args:
        tickers: Optional list of crypto ticker symbols to filter (e.g., ["X:BTCUSD", "X:ETHUSD"])
                If None, returns all available crypto tickers
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with comprehensive snapshot data for crypto tickers

    Note:
        This endpoint provides real-time market snapshot data including current day stats,
        previous day comparison, last trade info, and percentage changes. Essential for
        market overview, screening, and trading decisions.
    """
    logger.info(f"Fetching crypto snapshots" + (f" for {len(tickers)} tickers" if tickers else " (all tickers)"))

    records = []
    try:
        # Get all crypto snapshots
        snapshots = client.get_snapshot_all("crypto")

        # Debug: Check if we got any snapshots
        snapshot_list = list(snapshots) if snapshots else []
        logger.info(f"Received {len(snapshot_list)} crypto snapshots from API")

        if len(snapshot_list) == 0:
            logger.warning("No crypto snapshots returned from API - this endpoint may require a paid Polygon plan")
            return pd.DataFrame()

        # Debug: Log first ticker format if available
        if len(snapshot_list) > 0:
            first_ticker = getattr(snapshot_list[0], 'ticker', 'NO_TICKER_ATTR')
            logger.info(f"First snapshot ticker format: {first_ticker}")

        for snapshot in snapshot_list:
            # Extract ticker symbol
            ticker = getattr(snapshot, 'ticker', None)

            # Filter by tickers if specified
            if tickers and ticker not in tickers:
                continue

            # Initialize record with basic info
            record = {
                'ticker': ticker,
                'updated': getattr(snapshot, 'updated', None),
                'todays_change': getattr(snapshot, 'todaysChange', None) or getattr(snapshot, 'todays_change', None),
                'todays_change_perc': getattr(snapshot, 'todaysChangePerc', None) or getattr(snapshot, 'todays_change_perc', None),
            }

            # Extract day (current day) data
            day = getattr(snapshot, 'day', None)
            if day:
                record['day_open'] = getattr(day, 'o', None) or getattr(day, 'open', None)
                record['day_high'] = getattr(day, 'h', None) or getattr(day, 'high', None)
                record['day_low'] = getattr(day, 'l', None) or getattr(day, 'low', None)
                record['day_close'] = getattr(day, 'c', None) or getattr(day, 'close', None)
                record['day_volume'] = getattr(day, 'v', None) or getattr(day, 'volume', None)
                record['day_vwap'] = getattr(day, 'vw', None) or getattr(day, 'vwap', None)
            else:
                record['day_open'] = None
                record['day_high'] = None
                record['day_low'] = None
                record['day_close'] = None
                record['day_volume'] = None
                record['day_vwap'] = None

            # Extract prev_day (previous day) data
            prev_day = getattr(snapshot, 'prevDay', None) or getattr(snapshot, 'prev_day', None)
            if prev_day:
                record['prev_day_open'] = getattr(prev_day, 'o', None) or getattr(prev_day, 'open', None)
                record['prev_day_high'] = getattr(prev_day, 'h', None) or getattr(prev_day, 'high', None)
                record['prev_day_low'] = getattr(prev_day, 'l', None) or getattr(prev_day, 'low', None)
                record['prev_day_close'] = getattr(prev_day, 'c', None) or getattr(prev_day, 'close', None)
                record['prev_day_volume'] = getattr(prev_day, 'v', None) or getattr(prev_day, 'volume', None)
                record['prev_day_vwap'] = getattr(prev_day, 'vw', None) or getattr(prev_day, 'vwap', None)
            else:
                record['prev_day_open'] = None
                record['prev_day_high'] = None
                record['prev_day_low'] = None
                record['prev_day_close'] = None
                record['prev_day_volume'] = None
                record['prev_day_vwap'] = None

            # Extract lastTrade (last trade) data
            last_trade = getattr(snapshot, 'lastTrade', None) or getattr(snapshot, 'last_trade', None)
            if last_trade:
                record['last_trade_price'] = getattr(last_trade, 'p', None) or getattr(last_trade, 'price', None)
                record['last_trade_size'] = getattr(last_trade, 's', None) or getattr(last_trade, 'size', None)
                record['last_trade_exchange'] = getattr(last_trade, 'x', None) or getattr(last_trade, 'exchange', None)
                record['last_trade_timestamp'] = getattr(last_trade, 't', None) or getattr(last_trade, 'timestamp', None)
            else:
                record['last_trade_price'] = None
                record['last_trade_size'] = None
                record['last_trade_exchange'] = None
                record['last_trade_timestamp'] = None

            # Extract min (latest minute bar) data
            min_bar = getattr(snapshot, 'min', None)
            if min_bar:
                record['min_open'] = getattr(min_bar, 'o', None) or getattr(min_bar, 'open', None)
                record['min_high'] = getattr(min_bar, 'h', None) or getattr(min_bar, 'high', None)
                record['min_low'] = getattr(min_bar, 'l', None) or getattr(min_bar, 'low', None)
                record['min_close'] = getattr(min_bar, 'c', None) or getattr(min_bar, 'close', None)
                record['min_volume'] = getattr(min_bar, 'v', None) or getattr(min_bar, 'volume', None)
                record['min_vwap'] = getattr(min_bar, 'vw', None) or getattr(min_bar, 'vwap', None)
                record['min_timestamp'] = getattr(min_bar, 't', None) or getattr(min_bar, 'timestamp', None)
            else:
                record['min_open'] = None
                record['min_high'] = None
                record['min_low'] = None
                record['min_close'] = None
                record['min_volume'] = None
                record['min_vwap'] = None
                record['min_timestamp'] = None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto snapshots: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Convert timestamp columns to datetime
    if not df.empty:
        # Convert updated timestamp (milliseconds)
        if 'updated' in df.columns:
            df['updated'] = pd.to_datetime(df['updated'], unit='ms', errors='coerce')

        # Convert last_trade_timestamp (milliseconds)
        if 'last_trade_timestamp' in df.columns:
            df['last_trade_timestamp'] = pd.to_datetime(df['last_trade_timestamp'], unit='ms', errors='coerce')

        # Convert min_timestamp (milliseconds)
        if 'min_timestamp' in df.columns:
            df['min_timestamp'] = pd.to_datetime(df['min_timestamp'], unit='ms', errors='coerce')

    logger.info(f"Successfully fetched {len(df)} crypto snapshot records")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        if tickers:
            tickers_str = "_".join([t.replace(':', '_') for t in tickers[:3]])  # Limit to 3 for filename
            csv_file = output_dir / f"crypto_snapshots_{tickers_str}.csv"
        else:
            csv_file = output_dir / "crypto_snapshots_all.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved crypto snapshots to {csv_file}")

    return df


def fetch_crypto_last_trade(from_symbol: str,
                           to_symbol: str,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch the most recent trade for a specified cryptocurrency pair.

    This endpoint retrieves the last trade details including price, size, timestamp,
    exchange, and conditions. Provides real-time market information for trading decisions.

    Args:
        from_symbol: The "from" symbol of the crypto pair (e.g., "BTC", "ETH", "SOL")
        to_symbol: The "to" symbol of the crypto pair (e.g., "USD", "USDT", "EUR")
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with the most recent trade data for the crypto pair

    Note:
        This endpoint provides real-time trade data for rapid decision-making,
        market monitoring, and integration into trading systems.
    """
    import json
    from datetime import datetime

    logger.info(f"Fetching crypto last trade for {from_symbol}/{to_symbol}")

    try:
        # Get the last crypto trade
        trade = client.get_last_crypto_trade(from_symbol, to_symbol)

        # Extract trade data from the 'last' object
        last_trade = getattr(trade, 'last', None)

        if not last_trade:
            logger.error(f"No trade data found for {from_symbol}/{to_symbol}")
            return pd.DataFrame()

        # Extract trade details
        record = {
            'symbol': getattr(trade, 'symbol', f"{from_symbol}-{to_symbol}"),
            'price': getattr(last_trade, 'price', None),
            'size': getattr(last_trade, 'size', None),
            'exchange': getattr(last_trade, 'exchange', None),
            'timestamp': getattr(last_trade, 'timestamp', None),
        }

        # Convert timestamp from milliseconds to datetime string
        if record['timestamp']:
            dt = datetime.fromtimestamp(record['timestamp'] / 1000.0)
            record['timestamp_readable'] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]  # Include milliseconds
        else:
            record['timestamp_readable'] = None

        # Handle conditions array - convert to JSON string for CSV storage
        conditions = getattr(last_trade, 'conditions', None)
        if conditions:
            record['conditions'] = json.dumps(conditions) if isinstance(conditions, list) else str(conditions)
        else:
            record['conditions'] = None

        # Additional metadata
        record['request_id'] = getattr(trade, 'request_id', None)
        record['status'] = getattr(trade, 'status', None)

        df = pd.DataFrame([record])

        logger.info(f"Successfully fetched last trade for {from_symbol}/{to_symbol}: price=${record['price']}, size={record['size']}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_last_trade_{from_symbol}_{to_symbol}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto last trade to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto last trade for {from_symbol}/{to_symbol}: {e}")
        return pd.DataFrame()


def fetch_market_holidays(output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch upcoming market holidays from Polygon.io.

    This endpoint retrieves forward-looking market holidays and their corresponding
    open/close times. Use this data to plan ahead for trading activities, system
    operations, and schedule adjustments.

    Args:
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with upcoming market holidays including dates, exchanges, and status

    Note:
        This endpoint is forward-looking only, listing future holidays that affect
        market hours. Data applies to all asset classes (stocks, crypto, forex, etc.).
    """
    logger.info("Fetching upcoming market holidays")

    records = []
    try:
        # Fetch market holidays
        holidays = client.get_market_holidays()

        for holiday in holidays:
            # Extract basic holiday information
            record = {
                'date': getattr(holiday, 'date', None),
                'exchange': getattr(holiday, 'exchange', None),
                'name': getattr(holiday, 'name', None),
                'status': getattr(holiday, 'status', None),
            }

            # Handle optional open/close times (for early-close days)
            # These are typically ISO 8601 datetime strings
            open_time = getattr(holiday, 'open', None)
            close_time = getattr(holiday, 'close', None)

            record['open'] = open_time if open_time else None
            record['close'] = close_time if close_time else None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching market holidays: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Convert date column to proper date format if present
    if not df.empty and 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.strftime('%Y-%m-%d')

    logger.info(f"Successfully fetched {len(df)} upcoming market holidays")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "market_holidays.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved market holidays to {csv_file}")

    return df


def fetch_market_status(output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch current market status from Polygon.io.

    This endpoint retrieves the current trading status for various exchanges and overall
    financial markets, providing real-time indicators of whether markets are open, closed,
    or operating in pre-market/after-hours sessions.

    Args:
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with current market status including exchange and currency market states

    Note:
        This endpoint returns a snapshot of the current market status at the time of the call.
        Crypto markets trade 24/7, but traditional markets have specific hours.
    """
    logger.info("Fetching current market status")

    try:
        # Fetch market status
        status = client.get_market_status()

        # Build the record with flattened structure
        # Try both camelCase (JSON) and snake_case (Python SDK) naming conventions
        record = {
            'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'server_time': getattr(status, 'server_time', None) or getattr(status, 'serverTime', None),
            'market': getattr(status, 'market', None),
            'after_hours': getattr(status, 'after_hours', None) if getattr(status, 'after_hours', None) is not None else getattr(status, 'afterHours', None),
            'early_hours': getattr(status, 'early_hours', None) if getattr(status, 'early_hours', None) is not None else getattr(status, 'earlyHours', None),
        }

        # Extract currencies object
        currencies = getattr(status, 'currencies', None)
        if currencies:
            record['crypto_status'] = getattr(currencies, 'crypto', None)
            record['fx_status'] = getattr(currencies, 'fx', None)
        else:
            record['crypto_status'] = None
            record['fx_status'] = None

        # Extract exchanges object
        exchanges = getattr(status, 'exchanges', None)
        if exchanges:
            record['nasdaq_status'] = getattr(exchanges, 'nasdaq', None)
            record['nyse_status'] = getattr(exchanges, 'nyse', None)
            record['otc_status'] = getattr(exchanges, 'otc', None)
        else:
            record['nasdaq_status'] = None
            record['nyse_status'] = None
            record['otc_status'] = None

        # Extract indicesGroups object - handle all possible index groups dynamically
        # Try both naming conventions: indicesGroups (camelCase) and indices_groups (snake_case)
        indices_groups = getattr(status, 'indices_groups', None) or getattr(status, 'indicesGroups', None)
        if indices_groups:
            # List of known index groups from the documentation
            index_group_keys = ['cccy', 'cgi', 'dow_jones', 'ftse_russell', 'msci',
                               'mstar', 'mstarc', 'nasdaq', 's_and_p', 'societe_generale']
            for key in index_group_keys:
                record[f'index_{key}'] = getattr(indices_groups, key, None)
        else:
            # Add None for all index groups if not present
            index_group_keys = ['cccy', 'cgi', 'dow_jones', 'ftse_russell', 'msci',
                               'mstar', 'mstarc', 'nasdaq', 's_and_p', 'societe_generale']
            for key in index_group_keys:
                record[f'index_{key}'] = None

        df = pd.DataFrame([record])

        logger.info(f"Successfully fetched market status: market={record['market']}, crypto={record['crypto_status']}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / "market_status.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved market status to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching market status: {e}")
        return pd.DataFrame()
