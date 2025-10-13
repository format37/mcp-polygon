import logging
import uuid
import pandas as pd
from typing import Literal
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_gainers_losers(
    polygon_client: RESTClient,
    direction: Literal["gainers", "losers"] = "gainers",
) -> pd.DataFrame:
    """
    Fetch top 20 gainers or losers in the crypto market.

    This endpoint retrieves snapshot data highlighting the top 20 gainers or losers in the
    crypto market. Gainers are cryptos with the largest percentage increase since the previous
    day's close, and losers are those with the largest percentage decrease. Snapshot data is
    cleared daily at 12:00 AM EST and begins repopulating as exchanges report new information.
    By focusing on these market movers, users can quickly identify significant price shifts and
    monitor evolving market dynamics.

    Args:
        polygon_client: Initialized Polygon RESTClient
        direction: Direction to fetch - either "gainers" or "losers"

    Returns:
        DataFrame with top 20 crypto gainers or losers with comprehensive snapshot data

    Note:
        This endpoint is essential for identifying market momentum and trading opportunities.
        Gainers/losers are calculated based on percentage change from previous day's close.
    """
    logger.info(f"Fetching crypto {direction}")

    records = []
    try:
        # Get gainers or losers from Polygon API
        snapshots = polygon_client.get_snapshot_direction("crypto", direction)

        # Convert to list to check if we got data
        snapshot_list = list(snapshots) if snapshots else []
        logger.info(f"Received {len(snapshot_list)} crypto {direction} from API")

        if len(snapshot_list) == 0:
            logger.warning(f"No crypto {direction} returned from API - this endpoint may require a paid Polygon plan")
            return pd.DataFrame()

        for snapshot in snapshot_list:
            # Extract ticker symbol
            ticker = getattr(snapshot, 'ticker', None)

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
        logger.error(f"Error fetching crypto {direction}: {e}")
        # Re-raise the exception so the tool function can return the error to the user
        raise

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

        # Sort by percentage change (descending for gainers, ascending for losers)
        if 'todays_change_perc' in df.columns:
            ascending = (direction == "losers")
            df = df.sort_values('todays_change_perc', ascending=ascending)

    logger.info(f"Successfully fetched {len(df)} crypto {direction} records")

    return df


def register_polygon_crypto_gainers_losers(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_gainers_losers tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_gainers_losers(
        direction: str = "gainers"
    ) -> str:
        """
        Fetch top 20 gainers or losers in crypto market and save to CSV file.

        This endpoint retrieves snapshot data highlighting the top 20 gainers or losers in the
        cryptocurrency market based on percentage change since the previous day's close. Gainers
        are cryptos with the largest percentage increase, and losers are those with the largest
        percentage decrease. Snapshot data is cleared daily at 12:00 AM EST and begins repopulating
        as exchanges report new information throughout the day.

        By focusing on these market movers, traders can quickly identify:
        - Significant price shifts and momentum opportunities
        - Assets experiencing unusual volatility or breakouts
        - Market sentiment trends (risk-on vs. risk-off)
        - Potential entry/exit points for swing trades
        - Assets requiring immediate attention in portfolios

        Parameters:
            direction (str): Direction to fetch - either "gainers" or "losers"
                - "gainers": Returns top 20 cryptocurrencies with largest % increase (default)
                - "losers": Returns top 20 cryptocurrencies with largest % decrease

                Note: Both directions return comprehensive snapshot data for each asset,
                not just the percentage change.

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet to load the file.

        CSV Output Structure:
            Core Identification & Performance Metrics:
            - ticker (str): Cryptocurrency ticker symbol in X:BASEUSD format (e.g., "X:BTCUSD")
            - updated (datetime): Last update timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - todays_change (float): Absolute price change from previous close (in USD)
            - todays_change_perc (float): Percentage price change from previous close (THIS IS THE RANKING METRIC)

            Current Day (Today) Data:
            - day_open (float): Today's opening price in USD
            - day_high (float): Today's highest price in USD (shows peak momentum)
            - day_low (float): Today's lowest price in USD (shows support/resistance)
            - day_close (float): Today's most recent close/current price in USD
            - day_volume (float): Today's total trading volume (higher volume = stronger signal)
            - day_vwap (float): Today's Volume Weighted Average Price (indicates average execution price)

            Previous Day Data (for comparison):
            - prev_day_open (float): Previous day's opening price in USD
            - prev_day_high (float): Previous day's highest price in USD
            - prev_day_low (float): Previous day's lowest price in USD
            - prev_day_close (float): Previous day's closing price (baseline for % change calculation)
            - prev_day_volume (float): Previous day's total trading volume (compare with today's volume)
            - prev_day_vwap (float): Previous day's Volume Weighted Average Price

            Last Trade Information (real-time data):
            - last_trade_price (float): Price of the most recent trade in USD
            - last_trade_size (float): Size of the most recent trade (in base currency)
            - last_trade_exchange (int): Exchange ID where the last trade occurred
            - last_trade_timestamp (datetime): Timestamp of last trade in 'YYYY-MM-DD HH:MM:SS' format

            Latest Minute Bar Data (most recent 60 seconds):
            - min_open (float): Latest minute's opening price
            - min_high (float): Latest minute's highest price
            - min_low (float): Latest minute's lowest price
            - min_close (float): Latest minute's closing price
            - min_volume (float): Latest minute's trading volume
            - min_vwap (float): Latest minute's Volume Weighted Average Price
            - min_timestamp (datetime): Minute bar timestamp in 'YYYY-MM-DD HH:MM:SS' format

        Use Cases:
            - **Market Momentum Trading**: Identify cryptos with strongest upward/downward momentum
            - **Breakout Detection**: Spot assets breaking out of consolidation patterns
            - **News Event Impact**: See which cryptos are reacting to market news or events
            - **Portfolio Risk Management**: Monitor losers to identify positions requiring attention
            - **Contrarian Strategies**: Find oversold losers for potential bounce plays
            - **Trend Following**: Ride momentum by entering gainers with strong volume
            - **Market Sentiment Analysis**: Gauge overall crypto market risk appetite
            - **Sector Rotation**: Identify which crypto categories (DeFi, L1s, memes) are moving
            - **Volatility Trading**: Target highest movers for options or swing trades
            - **Alert Generation**: Set up alerts when specific cryptos become top movers

        Important Notes:
            - Returns exactly TOP 20 gainers or losers (not all cryptos, unlike crypto_snapshots)
            - Sorted by percentage change: gainers are highest first, losers are lowest first
            - Snapshot data resets daily at 12:00 AM EST (midnight Eastern Time)
            - Data begins repopulating from 4:00 AM EST as exchanges report new trades
            - Crypto markets trade 24/7, so new gainers/losers emerge throughout the day
            - Percentage changes are calculated against previous day's close (not 24h rolling)
            - High volume movers are generally more reliable signals than low volume ones
            - VWAP comparison (today vs. previous) helps identify institutional interest
            - Exchange IDs can be mapped using the polygon_crypto_exchanges tool
            - Combine gainers + losers data to get market breadth analysis

        Trading Strategy Examples:
            1. **Momentum Continuation**:
               - Find top 3 gainers with volume > 2x previous day
               - Enter on pullbacks to VWAP, ride the trend

            2. **Reversal Trading**:
               - Find top losers down >10% with low volume
               - Look for bounce opportunities at support levels

            3. **Volume Divergence**:
               - Compare day_volume vs prev_day_volume
               - High volume + big % change = strong signal

            4. **Intraday Fading**:
               - Check if gainers are overextended (day_high >> VWAP)
               - Short-term fade opportunities for mean reversion

            5. **Breakout Confirmation**:
               - Gainers breaking above prev_day_high = bullish breakout
               - Losers breaking below prev_day_low = bearish breakdown

        Example Workflow:
            1. Call this tool with direction="gainers" to get top 20 movers
            2. Receive CSV filename in response
            3. Use py_eval to load CSV and analyze:
               - Sort by todays_change_perc to rank by momentum
               - Filter for volume >= 2x prev_day_volume for confirmation
               - Calculate price position: (day_close - day_low) / (day_high - day_low)
               - Identify which gainers are still strong vs. fading
            4. Generate actionable insights:
               - "Top gainer: X:SOLUSD +15.3% with 3x volume - strong momentum"
               - "X:BTCUSD +5.1% but volume only 0.8x previous day - weak signal"

        Performance & Best Practices:
            - Call both gainers AND losers to get complete market picture
            - Re-fetch every 15-30 minutes during active trading hours
            - Always check volume confirmation (day_volume vs prev_day_volume)
            - Compare current price vs. day_high/day_low to time entries
            - Use VWAP as dynamic support/resistance level
            - Cross-reference with polygon_news to understand WHY assets are moving
            - Combine with polygon_crypto_aggregates for historical context
            - Monitor minute bar data (min_*) for real-time momentum shifts

        Integration with Other Tools:
            - Use polygon_news to find news catalysts for top movers
            - Use polygon_crypto_aggregates to see historical price context
            - Use polygon_crypto_snapshots to compare movers vs. full market
            - Use polygon_crypto_exchanges to identify dominant exchange activity
            - Use py_eval for advanced analysis: correlations, volume profiles, etc.

        Risk Warnings:
            - Extreme movers (>30% change) may face reversal or volatility expansion
            - Low volume movers can be illiquid and difficult to trade
            - Crypto markets are 24/7: US timezone gainers may differ from Asian session
            - Flash crashes and pump-and-dumps common in smaller cap cryptos
            - Always use stop losses when trading high volatility assets

        Always use py_eval tool to analyze the saved CSV file for actionable trading insights.
        """
        logger.info(f"polygon_crypto_gainers_losers invoked: direction={direction}")

        # Validate direction parameter
        if direction.lower() not in ["gainers", "losers"]:
            return f"Invalid direction parameter: '{direction}'. Must be either 'gainers' or 'losers'."

        direction = direction.lower()

        try:
            # Fetch crypto gainers or losers
            df = fetch_crypto_gainers_losers(
                polygon_client=local_polygon_client,
                direction=direction,
            )

            if df.empty:
                return f"No crypto {direction} data were retrieved.\n\nPossible causes:\n1. This endpoint may require a paid/premium Polygon.io plan\n2. The endpoint may be temporarily unavailable\n3. Snapshot data may not be populated yet (data refreshes from 4 AM EST)\n\nAlternative: Use polygon_crypto_snapshots to get all crypto data, then sort by todays_change_perc in py_eval."

            # Generate filename with direction
            filename = f"crypto_{direction}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto {direction} to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_gainers_losers: {e}")
            return f"Error fetching crypto {direction}: {str(e)}"
