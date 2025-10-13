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


def fetch_crypto_conditions(data_type: Optional[str] = None,
                           condition_id: Optional[int] = None,
                           sip: Optional[str] = None,
                           limit: int = 100,
                           sort: Optional[str] = None,
                           order: Optional[str] = None,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch crypto condition codes from Polygon.io.

    Condition codes explain special circumstances associated with crypto trades and quotes,
    such as trades occurring outside regular sessions or at averaged prices.

    Args:
        data_type: Filter by data type (e.g., "trade", "quote")
        condition_id: Filter for a specific condition ID
        sip: Filter by SIP (Systematic Internalization Provider)
        limit: Maximum number of results (default: 100, max: 1000)
        sort: Field to sort by (e.g., "id", "name")
        order: Sort order - "asc" or "desc"
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with crypto condition codes and their details

    Note:
        This is reference/metadata data that helps interpret crypto trading data.
        Use these conditions to understand trade flags and filter data appropriately.
    """
    import json

    logger.info(f"Fetching crypto conditions (limit={limit})")

    records = []
    try:
        # Build kwargs for the API call
        kwargs = {
            'asset_class': 'crypto',
            'limit': min(limit, 1000)  # Enforce max limit
        }

        if data_type:
            kwargs['data_type'] = data_type
        if condition_id is not None:
            kwargs['id'] = condition_id
        if sip:
            kwargs['sip'] = sip
        if sort:
            kwargs['sort'] = sort
        if order:
            kwargs['order'] = order

        # Fetch conditions from Polygon API
        conditions = client.list_conditions(**kwargs)

        for condition in conditions:
            # Extract basic fields
            record = {
                'id': getattr(condition, 'id', None),
                'name': getattr(condition, 'name', None),
                'abbreviation': getattr(condition, 'abbreviation', None),
                'asset_class': getattr(condition, 'asset_class', None),
                'description': getattr(condition, 'description', None),
                'type': getattr(condition, 'type', None),
                'legacy': getattr(condition, 'legacy', None),
                'exchange': getattr(condition, 'exchange', None),
            }

            # Handle array field - data_types
            data_types = getattr(condition, 'data_types', None)
            if data_types:
                record['data_types'] = json.dumps(data_types) if isinstance(data_types, list) else str(data_types)
            else:
                record['data_types'] = None

            # Handle complex object fields - convert to JSON strings
            sip_mapping = getattr(condition, 'sip_mapping', None)
            if sip_mapping:
                try:
                    # Convert to dict if it's an object, then to JSON
                    if hasattr(sip_mapping, '__dict__'):
                        record['sip_mapping'] = json.dumps(sip_mapping.__dict__)
                    elif isinstance(sip_mapping, dict):
                        record['sip_mapping'] = json.dumps(sip_mapping)
                    else:
                        record['sip_mapping'] = str(sip_mapping)
                except Exception as e:
                    logger.warning(f"Could not serialize sip_mapping: {e}")
                    record['sip_mapping'] = str(sip_mapping)
            else:
                record['sip_mapping'] = None

            update_rules = getattr(condition, 'update_rules', None)
            if update_rules:
                try:
                    # Convert to dict if it's an object, then to JSON
                    if hasattr(update_rules, '__dict__'):
                        record['update_rules'] = json.dumps(update_rules.__dict__)
                    elif isinstance(update_rules, dict):
                        record['update_rules'] = json.dumps(update_rules)
                    else:
                        record['update_rules'] = str(update_rules)
                except Exception as e:
                    logger.warning(f"Could not serialize update_rules: {e}")
                    record['update_rules'] = str(update_rules)
            else:
                record['update_rules'] = None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto conditions: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto conditions")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "crypto_conditions.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved crypto conditions to {csv_file}")

    return df


def fetch_crypto_daily_open_close(ticker: str,
                                  date: str,
                                  adjusted: bool = True,
                                  output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch daily open/close data for a specific cryptocurrency pair on a specific date.

    Args:
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        date: Date in YYYY-MM-DD format
        adjusted: Whether or not results are adjusted for splits (default: True)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with daily open/close data including OHLC and trade details

    Note:
        This endpoint provides the opening and closing trades for a specific crypto pair
        on a given date, enabling daily performance analysis and historical data collection.
    """
    logger.info(f"Fetching crypto daily open/close for {ticker} on {date}")

    try:
        # Get daily open/close data
        result = client.get_daily_open_close_agg(
            ticker=ticker,
            date=date,
            adjusted=adjusted
        )

        # Extract main data
        record = {
            'ticker': ticker,
            'date': date,
            'symbol': getattr(result, 'symbol', None),
            'open': getattr(result, 'open', None),
            'close': getattr(result, 'close', None),
            'high': getattr(result, 'high', None),
            'low': getattr(result, 'low', None),
            'volume': getattr(result, 'volume', None),
            'after_hours': getattr(result, 'after_hours', None),
            'pre_market': getattr(result, 'pre_market', None),
            'is_utc': getattr(result, 'isUTC', None),
        }

        # Process opening trades if available
        opening_trades = getattr(result, 'openTrades', None) or getattr(result, 'opening_trades', None)
        if opening_trades:
            record['num_opening_trades'] = len(opening_trades)
            # Sum up the trade sizes (volumes)
            record['opening_volume_total'] = sum(getattr(trade, 's', 0) or getattr(trade, 'size', 0) for trade in opening_trades)
            # Get the price of the first opening trade
            if len(opening_trades) > 0:
                first_trade = opening_trades[0]
                record['first_opening_trade_price'] = getattr(first_trade, 'p', None) or getattr(first_trade, 'price', None)
        else:
            record['num_opening_trades'] = 0
            record['opening_volume_total'] = 0
            record['first_opening_trade_price'] = None

        # Process closing trades if available
        closing_trades = getattr(result, 'closingTrades', None) or getattr(result, 'closing_trades', None)
        if closing_trades:
            record['num_closing_trades'] = len(closing_trades)
            # Sum up the trade sizes (volumes)
            record['closing_volume_total'] = sum(getattr(trade, 's', 0) or getattr(trade, 'size', 0) for trade in closing_trades)
            # Get the price of the last closing trade
            if len(closing_trades) > 0:
                last_trade = closing_trades[-1]
                record['last_closing_trade_price'] = getattr(last_trade, 'p', None) or getattr(last_trade, 'price', None)
        else:
            record['num_closing_trades'] = 0
            record['closing_volume_total'] = 0
            record['last_closing_trade_price'] = None

        df = pd.DataFrame([record])

        logger.info(f"Successfully fetched daily open/close data for {ticker} on {date}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_daily_open_close_{ticker.replace(':', '_')}_{date}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto daily open/close to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto daily open/close for {ticker} on {date}: {e}")
        return pd.DataFrame()


def fetch_crypto_exchanges(locale: Optional[str] = None,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch cryptocurrency exchange reference data from Polygon.io.

    This endpoint retrieves a list of known crypto exchanges including their identifiers,
    names, market types, and other relevant attributes. This information helps map exchange
    codes, understand market coverage, and integrate exchange details into applications.

    Args:
        locale: Optional locale filter ("us" or "global")
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with crypto exchange metadata

    Note:
        This is reference/metadata data that provides exchange information for mapping
        and understanding crypto market data. Fetch once and use as a lookup table.
    """
    logger.info(f"Fetching crypto exchanges" + (f" (locale={locale})" if locale else ""))

    records = []
    try:
        # Build kwargs for the API call
        kwargs = {'asset_class': 'crypto'}

        if locale:
            kwargs['locale'] = locale

        # Fetch exchanges from Polygon API
        exchanges = client.get_exchanges(**kwargs)

        for exchange in exchanges:
            # Extract all available fields
            record = {
                'id': getattr(exchange, 'id', None),
                'name': getattr(exchange, 'name', None),
                'acronym': getattr(exchange, 'acronym', None),
                'asset_class': getattr(exchange, 'asset_class', None),
                'locale': getattr(exchange, 'locale', None),
                'mic': getattr(exchange, 'mic', None),
                'operating_mic': getattr(exchange, 'operating_mic', None),
                'participant_id': getattr(exchange, 'participant_id', None),
                'type': getattr(exchange, 'type', None),
                'url': getattr(exchange, 'url', None),
            }
            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto exchanges: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto exchanges")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "crypto_exchanges.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved crypto exchanges to {csv_file}")

    return df


def fetch_crypto_grouped_daily_bars(date: str,
                                    adjusted: bool = True,
                                    output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch grouped daily OHLC data for ALL cryptocurrency pairs on a specific date.

    This endpoint retrieves daily aggregate bars for all crypto tickers traded on a given date,
    providing a comprehensive market-wide snapshot in a single API call. Ideal for analyzing
    overall market conditions, identifying trending coins, and bulk data processing.

    Args:
        date: Date in YYYY-MM-DD format (e.g., "2024-01-15")
        adjusted: Whether or not results are adjusted for splits (default: True)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with daily OHLC data for all crypto pairs on the specified date

    Note:
        This endpoint returns data for ALL crypto pairs in one call, making it extremely
        efficient for market-wide analysis. Perfect for daily market snapshots and research.
    """
    logger.info(f"Fetching crypto grouped daily bars for {date}")

    try:
        # Get grouped daily aggregates for all crypto pairs
        result = client.get_grouped_daily_aggs(
            date=date,
            locale="global",
            market_type="crypto",
            adjusted=adjusted
        )

        # Process results into records
        records = []
        for bar in result:
            # The ticker field might be 'T' or 'ticker' or 'symbol' depending on the API version
            ticker = getattr(bar, 'ticker', None) or getattr(bar, 'T', None) or getattr(bar, 'symbol', None)

            record = {
                'ticker': ticker,
                'date': date,
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume,
                'vwap': getattr(bar, 'vwap', None),
                'transactions': getattr(bar, 'transactions', None),
                'timestamp': bar.timestamp,
            }
            records.append(record)

        df = pd.DataFrame(records)

        # Convert timestamp to datetime if present
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        logger.info(f"Successfully fetched {len(df)} crypto pairs for {date}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_grouped_daily_{date.replace('-', '')}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto grouped daily bars to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto grouped daily bars for {date}: {e}")
        return pd.DataFrame()


def fetch_crypto_previous_close(ticker: str,
                                adjusted: bool = True,
                                output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch previous trading day's OHLC data for a cryptocurrency pair.

    This endpoint retrieves the previous day's open, high, low, and close (OHLC) data
    for a specified crypto pair, providing key pricing metrics to assess recent performance
    and inform trading strategies.

    Args:
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        adjusted: Whether results are adjusted for splits (default: True)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with previous day's OHLC data including volume and VWAP

    Note:
        This endpoint provides the most recent completed trading day's data,
        useful for baseline comparisons, technical analysis, and daily reporting.
    """
    logger.info(f"Fetching crypto previous close for {ticker}")

    try:
        # Get previous close aggregate
        result = client.get_previous_close_agg(ticker)

        # Extract data from results array (typically contains one item)
        results = getattr(result, 'results', None)
        if not results or len(results) == 0:
            logger.error(f"No previous close data found for {ticker}")
            return pd.DataFrame()

        # Get the first (and typically only) result
        bar = results[0]

        # Extract OHLCV data
        record = {
            'ticker': getattr(bar, 'T', ticker),
            'timestamp': getattr(bar, 't', None),
            'open': getattr(bar, 'o', None),
            'high': getattr(bar, 'h', None),
            'low': getattr(bar, 'l', None),
            'close': getattr(bar, 'c', None),
            'volume': getattr(bar, 'v', None),
            'vwap': getattr(bar, 'vw', None),
            'transactions': getattr(bar, 'n', None),
        }

        df = pd.DataFrame([record])

        # Convert timestamp to datetime if present
        if not df.empty and 'timestamp' in df.columns and df['timestamp'].iloc[0]:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['date'] = df['timestamp'].dt.date

        logger.info(f"Successfully fetched previous close for {ticker}: close=${record['close']}, volume={record['volume']}")

        # Save to CSV if output directory is provided
        if output_dir and not df.empty:
            csv_file = output_dir / f"crypto_prev_close_{ticker.replace(':', '_')}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"Saved crypto previous close to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto previous close for {ticker}: {e}")
        return pd.DataFrame()


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
