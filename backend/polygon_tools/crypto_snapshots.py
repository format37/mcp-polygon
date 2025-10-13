import logging
import uuid
import pandas as pd
from typing import Optional, List
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_snapshots(
    polygon_client: RESTClient,
    tickers: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Fetch comprehensive snapshot data for cryptocurrency tickers.

    This endpoint retrieves a full market snapshot for all crypto tickers (or specific ones)
    in a single API call, consolidating current pricing, volume, trade activity, and today's
    performance metrics. Snapshot data is cleared daily at 12:00 AM EST and begins repopulating
    as exchanges report new data (can start as early as 4:00 AM EST).

    Args:
        polygon_client: Initialized Polygon RESTClient
        tickers: Optional list of crypto ticker symbols to filter (e.g., ["X:BTCUSD", "X:ETHUSD"])
                If None, returns all available crypto tickers

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
        snapshots = polygon_client.get_snapshot_all("crypto")

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

    return df


def register_polygon_crypto_snapshots(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_snapshots tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_snapshots(
        tickers: str = ""
    ) -> str:
        """
        Fetch comprehensive snapshot data for cryptocurrency tickers and save to CSV file.

        This endpoint retrieves a full market snapshot consolidating key information like pricing,
        volume, and trade activity in a single API call. Snapshot data is cleared daily at 12:00 AM
        EST and begins repopulating as exchanges report new data (can start as early as 4:00 AM EST).
        By accessing all tickers at once or filtering specific ones, users can efficiently monitor
        market conditions, perform comparative analysis, and power trading applications that require
        complete, current market information.

        Parameters:
            tickers (str, optional): Comma-separated list of cryptocurrency ticker symbols to filter.
                Format: Crypto tickers in X:BASEUSD format
                Examples:
                - "" (empty string, default): Returns ALL available crypto tickers (500+ pairs)
                - "X:BTCUSD": Returns only Bitcoin/USD snapshot
                - "X:BTCUSD,X:ETHUSD,X:SOLUSD": Returns snapshots for Bitcoin, Ethereum, and Solana
                - "X:ADAUSD,X:DOGEUSD,X:MATICUSD": Returns snapshots for Cardano, Dogecoin, and Polygon

                Common Major Crypto Tickers:
                - X:BTCUSD (Bitcoin), X:ETHUSD (Ethereum), X:SOLUSD (Solana)
                - X:ADAUSD (Cardano), X:DOGEUSD (Dogecoin), X:MATICUSD (Polygon/MATIC)
                - X:AVAXUSD (Avalanche), X:LINKUSD (Chainlink), X:DOTUSD (Polkadot)
                - X:UNIUSD (Uniswap), X:ATOMUSD (Cosmos), X:XLMUSD (Stellar)

                Note: The 'X:' prefix indicates crypto exchange aggregation across multiple venues

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            Core Identification & Timestamps:
            - ticker (str): Cryptocurrency ticker symbol in X:BASEUSD format (e.g., "X:BTCUSD")
            - updated (datetime): Last update timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - todays_change (float): Absolute price change from previous close (in USD)
            - todays_change_perc (float): Percentage price change from previous close

            Current Day (Today) Data:
            - day_open (float): Today's opening price in USD
            - day_high (float): Today's highest price in USD
            - day_low (float): Today's lowest price in USD
            - day_close (float): Today's most recent close/current price in USD
            - day_volume (float): Today's total trading volume (in base currency units)
            - day_vwap (float): Today's Volume Weighted Average Price - critical for execution analysis

            Previous Day Data (for comparison):
            - prev_day_open (float): Previous day's opening price in USD
            - prev_day_high (float): Previous day's highest price in USD
            - prev_day_low (float): Previous day's lowest price in USD
            - prev_day_close (float): Previous day's closing price in USD
            - prev_day_volume (float): Previous day's total trading volume
            - prev_day_vwap (float): Previous day's Volume Weighted Average Price

            Last Trade Information:
            - last_trade_price (float): Price of the most recent trade in USD
            - last_trade_size (float): Size of the most recent trade (in base currency)
            - last_trade_exchange (int): Exchange ID where the last trade occurred
            - last_trade_timestamp (datetime): Timestamp of last trade in 'YYYY-MM-DD HH:MM:SS' format

            Latest Minute Bar Data:
            - min_open (float): Latest minute's opening price
            - min_high (float): Latest minute's highest price
            - min_low (float): Latest minute's lowest price
            - min_close (float): Latest minute's closing price
            - min_volume (float): Latest minute's trading volume
            - min_vwap (float): Latest minute's Volume Weighted Average Price
            - min_timestamp (datetime): Minute bar timestamp in 'YYYY-MM-DD HH:MM:SS' format

        Use Cases:
            - **Market Overview**: Get complete crypto market snapshot in one call, see all assets at once
            - **Screening & Discovery**: Filter and identify top movers, high volume, or volatile cryptocurrencies
            - **Portfolio Monitoring**: Track multiple crypto holdings simultaneously with real-time updates
            - **Comparative Analysis**: Compare performance metrics across different crypto assets
            - **Trading Signals**: Identify entry/exit opportunities based on price changes and volume
            - **Risk Assessment**: Monitor volatility through daily ranges and volume patterns
            - **Market Sentiment**: Gauge overall market direction and strength through aggregate data
            - **Automated Trading**: Feed real-time snapshot data into trading algorithms and bots
            - **Research & Analysis**: Collect comprehensive market data for backtesting and strategy development
            - **Dashboard & Visualization**: Power live crypto market dashboards and heat maps

        Important Notes:
            - Snapshot data resets daily at 12:00 AM EST (midnight Eastern Time)
            - Data begins repopulating from 4:00 AM EST as exchanges report new trades
            - Crypto markets trade 24/7, so snapshots are always updating (unlike stocks)
            - Returns ALL crypto pairs when no tickers specified (typically 500+ pairs)
            - Filtering specific tickers significantly reduces response time and size
            - VWAP is crucial for evaluating execution quality and institutional trades
            - Today's change fields compare current price against previous day's close
            - Exchange IDs can be mapped using the polygon_crypto_exchanges tool
            - Minute bar data provides the most recent price action (last 60 seconds)
            - Last trade data shows the absolute latest transaction in the market

        Example Workflow:
            1. Call this tool with specific tickers or leave empty for full market snapshot
            2. Receive CSV filename in response
            3. Use py_eval to load and analyze the CSV file with pandas
            4. Calculate metrics: sort by change %, identify volume leaders, detect trends
            5. Generate insights: "BTC up 3.2%, ETH down 1.5%, SOL highest volume"

        Performance & Best Practices:
            - For quick checks of 3-5 cryptos, specify tickers to minimize response time
            - For full market scans, use empty tickers to get complete dataset
            - Combine with polygon_crypto_aggregates for historical time series analysis
            - Use with polygon_crypto_exchanges to map exchange IDs to exchange names
            - Poll this endpoint regularly (e.g., every 1-5 minutes) for near-real-time monitoring
            - Compare today's metrics vs. previous day to identify significant movements
            - Use VWAP to assess if current prices are favorable for large orders

        Integration with Other Tools:
            - Use with polygon_crypto_aggregates for detailed historical price charts
            - Combine with polygon_news to understand price movements from market events
            - Use with polygon_crypto_exchanges to identify which exchanges are most active
            - Integrate with py_eval for advanced statistical analysis and visualization

        Always use py_eval tool to analyze the saved CSV file for comprehensive market insights.
        """
        logger.info(f"polygon_crypto_snapshots invoked: tickers={tickers if tickers else '(all)'}")

        try:
            # Parse tickers parameter
            ticker_list = None
            if tickers and tickers.strip():
                # Split by comma and clean whitespace
                ticker_list = [t.strip() for t in tickers.split(',') if t.strip()]
                logger.info(f"Filtering for {len(ticker_list)} specific tickers")

            # Fetch crypto snapshots
            df = fetch_crypto_snapshots(
                polygon_client=local_polygon_client,
                tickers=ticker_list,
            )

            if df.empty:
                if ticker_list:
                    return f"No crypto snapshot data were retrieved for the specified tickers: {', '.join(ticker_list)}.\n\nPossible causes:\n1. This endpoint may require a paid/premium Polygon.io plan\n2. The ticker format might be incorrect (should be X:BASEUSD, e.g., X:BTCUSD)\n3. The tickers might not be valid cryptocurrency pairs\n\nAlternative: Use polygon_crypto_aggregates for historical data or polygon_crypto_grouped_daily_bars for daily snapshots."
                else:
                    return "No crypto snapshot data were retrieved.\n\nPossible causes:\n1. This endpoint may require a paid/premium Polygon.io plan (similar to polygon_crypto_last_trade)\n2. The endpoint may be temporarily unavailable\n3. There may be no active crypto tickers at this time\n\nAlternative: Use polygon_crypto_aggregates for historical data or polygon_crypto_grouped_daily_bars for daily snapshots."

            # Always save to CSV file
            if ticker_list and len(ticker_list) <= 5:
                # Include tickers in filename if 5 or fewer
                tickers_str = "_".join([t.replace(':', '_') for t in ticker_list])
                filename = f"crypto_snapshots_{tickers_str}_{uuid.uuid4().hex[:8]}.csv"
            else:
                # Use generic name for all tickers or many tickers
                filename = f"crypto_snapshots_{uuid.uuid4().hex[:8]}.csv"

            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto snapshots to {filename} ({len(df)} crypto pairs)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_snapshots: {e}")
            return f"Error fetching crypto snapshots: {str(e)}"
