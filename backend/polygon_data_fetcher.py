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


def create_output_folder(base_name: str = "polygon_output") -> Path:
    """
    Create timestamped output folder for saving CSV files.

    Returns:
        Path to the created output folder
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = Path(f"{base_name}_{timestamp}")
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")
    return output_dir


def fetch_ticker_details(tickers: List[str], output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch comprehensive ticker details including company info, market cap, sector, etc.

    Args:
        tickers: List of ticker symbols (e.g., ['AAPL', 'MSFT'])
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with ticker details
    """
    logger.info(f"Fetching ticker details for {len(tickers)} tickers")

    records = []
    for i, ticker in enumerate(tickers):
        try:
            logger.info(f"Processing {i+1}/{len(tickers)}: {ticker}")

            # Get ticker details
            details = client.get_ticker_details(ticker)

            record = {
                'ticker': ticker,
                'name': getattr(details, 'name', None),
                'market_cap': getattr(details, 'market_cap', None),
                'share_class_shares_outstanding': getattr(details, 'share_class_shares_outstanding', None),
                'weighted_shares_outstanding': getattr(details, 'weighted_shares_outstanding', None),
                'primary_exchange': getattr(details, 'primary_exchange', None),
                'type': getattr(details, 'type', None),
                'active': getattr(details, 'active', None),
                'currency_name': getattr(details, 'currency_name', None),
                'cik': getattr(details, 'cik', None),
                'composite_figi': getattr(details, 'composite_figi', None),
                'share_class_figi': getattr(details, 'share_class_figi', None),
                'locale': getattr(details, 'locale', None),
                'description': getattr(details, 'description', None),
                'homepage_url': getattr(details, 'homepage_url', None),
                'total_employees': getattr(details, 'total_employees', None),
                'list_date': getattr(details, 'list_date', None),
                'logo_url': getattr(details, 'logo_url', None),
                'icon_url': getattr(details, 'icon_url', None),
                'sic_code': getattr(details, 'sic_code', None),
                'sic_description': getattr(details, 'sic_description', None),
                'ticker_root': getattr(details, 'ticker_root', None),
                'phone_number': getattr(details, 'phone_number', None),
            }

            # Add address information if available
            address = getattr(details, 'address', None)
            if address:
                record['address_1'] = getattr(address, 'address1', None)
                record['city'] = getattr(address, 'city', None)
                record['state'] = getattr(address, 'state', None)
                record['postal_code'] = getattr(address, 'postal_code', None)
            else:
                record['address_1'] = None
                record['city'] = None
                record['state'] = None
                record['postal_code'] = None

            # Add branding information if available
            branding = getattr(details, 'branding', None)
            if branding:
                record['logo_url'] = getattr(branding, 'logo_url', record['logo_url'])
                record['icon_url'] = getattr(branding, 'icon_url', record['icon_url'])

            records.append(record)

            # Rate limiting - be respectful
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error fetching details for {ticker}: {e}")
            # Add a record with minimal info so we don't lose the ticker
            records.append({
                'ticker': ticker,
                'name': None,
                'market_cap': None,
                'error': str(e)
            })

    df = pd.DataFrame(records)

    # Convert date columns
    if not df.empty and 'list_date' in df.columns:
        df['list_date'] = pd.to_datetime(df['list_date'], errors='coerce')

    logger.info(f"Successfully fetched details for {len(df)} tickers")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / "ticker_details.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved ticker details to {csv_file}")

    return df


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


def fetch_crypto_aggregates(ticker: str,
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           timespan: str = "hour",
                           multiplier: int = 1,
                           limit: int = 50000,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Fetch historical crypto aggregate bars (OHLCV data) for a cryptocurrency pair.

    Args:
        ticker: Crypto ticker symbol in format X:BASEUSD (e.g., "X:BTCUSD", "X:ETHUSD")
        from_date: Start date in YYYY-MM-DD format (defaults to 7 days ago)
        to_date: End date in YYYY-MM-DD format (defaults to today)
        timespan: Time span - minute, hour, day, week, month (default: hour)
        multiplier: Size of the time window (e.g., 5 for 5-hour bars, default: 1)
        limit: Maximum number of bars to fetch (default: 50000, max: 50000)
        output_dir: Optional output directory to save CSV file

    Returns:
        DataFrame with crypto aggregate bars containing OHLCV data

    Note:
        Crypto markets are 24/7, so timestamps span all days and hours.
        The ticker must be in format X:BASEUSD (e.g., X:BTCUSD for Bitcoin/USD).
    """
    if from_date is None:
        from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    logger.info(f"Fetching crypto aggregates for {ticker} from {from_date} to {to_date}")
    logger.info(f"Parameters: timespan={timespan}, multiplier={multiplier}, limit={limit}")

    records = []
    try:
        # Get crypto price aggregates
        aggs = client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=from_date,
            to=to_date,
            adjusted=True,
            limit=limit,
            sort="asc"
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

    except Exception as e:
        logger.error(f"Error fetching crypto aggregates for {ticker}: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Convert timestamp to datetime
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['date'] = df['timestamp'].dt.date

    logger.info(f"Successfully fetched {len(df)} crypto aggregate bars for {ticker}")

    # Save to CSV if output directory is provided
    if output_dir and not df.empty:
        csv_file = output_dir / f"crypto_aggs_{ticker.replace(':', '_')}.csv"
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved crypto aggregates to {csv_file}")

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


def calculate_price_metrics(price_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate price-based metrics for each ticker.

    Args:
        price_df: DataFrame with price data

    Returns:
        DataFrame with calculated metrics per ticker
    """
    if price_df.empty:
        return pd.DataFrame()

    logger.info("Calculating price metrics")

    metrics = []
    for ticker in price_df['ticker'].unique():
        ticker_data = price_df[price_df['ticker'] == ticker].sort_values('timestamp')

        if len(ticker_data) < 2:
            continue

        # Basic metrics
        first_price = ticker_data.iloc[0]['close']
        last_price = ticker_data.iloc[-1]['close']
        high_price = ticker_data['high'].max()
        low_price = ticker_data['low'].min()
        avg_volume = ticker_data['volume'].mean()
        total_volume = ticker_data['volume'].sum()

        # Calculate returns
        total_return = (last_price / first_price - 1) if first_price != 0 else None

        # Calculate daily returns for volatility
        ticker_data = ticker_data.copy()
        ticker_data['daily_return'] = ticker_data['close'].pct_change()
        volatility = ticker_data['daily_return'].std()

        # Price range metrics
        price_range = (high_price - low_price) / last_price if last_price != 0 else None

        metrics.append({
            'ticker': ticker,
            'start_date': ticker_data.iloc[0]['date'],
            'end_date': ticker_data.iloc[-1]['date'],
            'trading_days': len(ticker_data),
            'start_price': first_price,
            'end_price': last_price,
            'high_price': high_price,
            'low_price': low_price,
            'total_return': total_return,
            'volatility': volatility,
            'avg_daily_volume': avg_volume,
            'total_volume': total_volume,
            'price_range_ratio': price_range,
        })

    return pd.DataFrame(metrics)


def create_combined_analysis(tickers: List[str],
                           from_date: Optional[str] = None,
                           to_date: Optional[str] = None,
                           output_dir: Optional[Path] = None) -> pd.DataFrame:
    """
    Create a combined analysis DataFrame with ticker details and price metrics.

    Args:
        tickers: List of ticker symbols
        from_date: Start date for price data
        to_date: End date for price data
        output_dir: Optional output directory to save CSV files

    Returns:
        Combined DataFrame with both ticker details and price metrics
    """
    logger.info("Creating combined analysis")

    # Fetch data from both endpoints
    details_df = fetch_ticker_details(tickers, output_dir)
    price_df = fetch_price_data(tickers, from_date, to_date, output_dir=output_dir)

    # Calculate price metrics
    metrics_df = calculate_price_metrics(price_df)

    # Combine the data
    if not details_df.empty and not metrics_df.empty:
        combined_df = details_df.merge(metrics_df, on='ticker', how='outer')
    elif not details_df.empty:
        combined_df = details_df
    elif not metrics_df.empty:
        combined_df = metrics_df
    else:
        combined_df = pd.DataFrame()

    # Save combined analysis if output directory is provided
    if output_dir and not combined_df.empty:
        csv_file = output_dir / "combined_analysis.csv"
        combined_df.to_csv(csv_file, index=False)
        logger.info(f"Saved combined analysis to {csv_file}")

    # Save price metrics separately if available
    if output_dir and not metrics_df.empty:
        csv_file = output_dir / "price_metrics.csv"
        metrics_df.to_csv(csv_file, index=False)
        logger.info(f"Saved price metrics to {csv_file}")

    logger.info(f"Combined analysis complete with {len(combined_df)} records")
    return combined_df


def main():
    """Main function to demonstrate the data fetcher."""

    # Example tickers - mix of large caps and different sectors
    tickers = [
        'AAPL',  # Technology
        'MSFT',  # Technology
        'GOOGL', # Technology
        'TSLA',  # Automotive/Technology
        'NVDA',  # Semiconductors
        'JPM',   # Financial
        'JNJ',   # Healthcare
        'PG',    # Consumer Goods
        'KO',    # Beverages
        'DIS'    # Entertainment
    ]

    # Set date range for price data (last 60 days)
    to_date = datetime.now().strftime('%Y-%m-%d')
    from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    logger.info(f"Starting analysis for tickers: {', '.join(tickers)}")
    logger.info(f"Price data range: {from_date} to {to_date}")

    # Create output directory
    output_dir = create_output_folder()

    # Create combined analysis
    combined_df = create_combined_analysis(tickers, from_date, to_date, output_dir)

    if not combined_df.empty:
        # Display summary
        print("\n" + "="*50)
        print("ANALYSIS SUMMARY")
        print("="*50)
        print(f"Total tickers analyzed: {len(combined_df)}")
        print(f"Date range: {from_date} to {to_date}")
        print(f"Output directory: {output_dir}")
        print(f"Files created:")
        for file in output_dir.glob("*.csv"):
            print(f"  - {file.name}")

        print(f"\nAll files saved to: {output_dir.absolute()}")

        # Show sample of the data
        print("\nSample data (first 5 rows):")
        print(combined_df.head().to_string())

        # Show some basic statistics
        if 'total_return' in combined_df.columns:
            print(f"\nReturn statistics:")
            print(f"Average return: {combined_df['total_return'].mean():.2%}")
            print(f"Best performer: {combined_df.loc[combined_df['total_return'].idxmax(), 'ticker']} ({combined_df['total_return'].max():.2%})")
            print(f"Worst performer: {combined_df.loc[combined_df['total_return'].idxmin(), 'ticker']} ({combined_df['total_return'].min():.2%})")

        if 'market_cap' in combined_df.columns:
            print(f"\nMarket cap statistics:")
            print(f"Average market cap: ${combined_df['market_cap'].mean():,.0f}")
            print(f"Largest company: {combined_df.loc[combined_df['market_cap'].idxmax(), 'ticker']} (${combined_df['market_cap'].max():,.0f})")

    else:
        logger.error("No data was successfully fetched")


if __name__ == "__main__":
    main()