import logging
import uuid
import json
import pandas as pd
from datetime import datetime
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_trades(
    polygon_client: RESTClient,
    ticker: str,
    timestamp: Optional[str] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    """
    Fetch tick-level trade data for a specified cryptocurrency pair.

    This endpoint retrieves comprehensive trade-by-trade data including price, size,
    exchange, trade conditions, and precise timestamps. Essential for microstructure
    analysis, algorithmic trading, and detailed market research.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Crypto ticker symbol in format X:BASE-QUOTE (e.g., "X:BTC-USD", "X:ETH-USD")
        timestamp: Optional date filter in YYYY-MM-DD format. If provided, fetches trades
                   from this date onwards. If not provided, fetches most recent trades.
        limit: Maximum number of trades to fetch (default: 10000, max: 50000)

    Returns:
        DataFrame with tick-level trade data for the crypto pair

    Note:
        This endpoint provides granular trade data that forms the foundation for all
        aggregate calculations (OHLC bars). Use for detailed analysis of market activity,
        order flow, and trading patterns.
    """
    logger.info(f"Fetching crypto trades for {ticker}, timestamp={timestamp or 'recent'}, limit={limit}")

    records = []
    try:
        # Build API parameters
        api_kwargs = {
            'limit': min(limit, 50000)  # Enforce max limit
        }

        # Add timestamp filter if provided
        if timestamp:
            api_kwargs['timestamp'] = timestamp

        # Fetch trades from Polygon API
        # list_trades returns an iterator
        trades = polygon_client.list_trades(ticker, **api_kwargs)

        # Iterate through trades and collect data
        for trade in trades:
            # Extract trade details
            record = {
                'ticker': ticker,
                'id': getattr(trade, 'id', None),
                'price': getattr(trade, 'price', None),
                'size': getattr(trade, 'size', None),
                'exchange': getattr(trade, 'exchange', None),
                'participant_timestamp': getattr(trade, 'participant_timestamp', None),
            }

            # Convert participant_timestamp from nanoseconds to readable datetime
            if record['participant_timestamp']:
                try:
                    # Convert nanoseconds to seconds for datetime
                    dt = datetime.fromtimestamp(record['participant_timestamp'] / 1_000_000_000)
                    record['timestamp_readable'] = dt.strftime('%Y-%m-%d %H:%M:%S.%f')
                except (ValueError, TypeError, OverflowError) as e:
                    logger.warning(f"Error converting timestamp {record['participant_timestamp']}: {e}")
                    record['timestamp_readable'] = None
            else:
                record['timestamp_readable'] = None

            # Handle conditions array - convert to JSON string for CSV storage
            conditions = getattr(trade, 'conditions', None)
            if conditions:
                record['conditions'] = json.dumps(conditions) if isinstance(conditions, list) else str(conditions)
            else:
                record['conditions'] = None

            records.append(record)

    except Exception as e:
        logger.error(f"Error fetching crypto trades for {ticker}: {e}")
        raise

    df = pd.DataFrame(records)

    logger.info(f"Successfully fetched {len(df)} crypto trades for {ticker}")

    return df


def register_polygon_crypto_trades(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_trades tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_trades(
        ticker: str,
        timestamp: str = "",
        limit: int = 10000
    ) -> str:
        """
        Fetch tick-level trade data for a cryptocurrency pair and save to CSV file.

        This tool retrieves comprehensive, trade-by-trade data for crypto pairs. Each record
        includes price, size, exchange, trade conditions, and precise timestamps. This granular
        data is essential for market microstructure analysis, algorithmic trading development,
        order flow analysis, and detailed market research. All aggregate bars (OHLC) are built
        from this foundational trade data.

        Parameters:
            ticker (str, required): Cryptocurrency ticker symbol in format X:BASE-QUOTE.
                Examples: "X:BTC-USD" (Bitcoin/USD), "X:ETH-USD" (Ethereum/USD),
                         "X:SOL-USD" (Solana/USD), "X:ADA-USD" (Cardano/USD),
                         "X:DOGE-USD" (Dogecoin/USD), "X:MATIC-USD" (Polygon/USD),
                         "X:AVAX-USD" (Avalanche/USD), "X:LINK-USD" (Chainlink/USD)
                Format: Must include "X:" prefix and use hyphen separator (e.g., "X:BTC-USD")
                The "X:" prefix indicates crypto exchange aggregation across multiple venues

            timestamp (str, optional): Date filter in 'YYYY-MM-DD' format.
                If provided, fetches trades from this date onwards.
                If not provided, fetches the most recent trades available.
                Examples: "2023-02-01", "2024-01-15"
                Note: Crypto markets are 24/7, so data is available for all dates

            limit (int, optional): Maximum number of trades to retrieve.
                Range: 1-50000
                Default: 10000
                Note: The API enforces a maximum limit of 50,000 trades per request.
                      For analyzing longer periods, make multiple requests with different timestamps.
                      Trade data can be extremely large - start with smaller limits for testing.

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTC-USD")
            - id (str): Unique trade ID from the exchange where the trade occurred
                Example: "191450340"
                This ID is unique per exchange but may not be globally unique across all exchanges
            - price (float): The price of the trade in the quote currency (e.g., USD)
                Example: 35060.0 means $35,060 per 1 BTC
                This is the actual executed price for this specific trade
            - size (float): The size/volume of the trade in base currency units
                Example: 1.0434526 means 1.0434526 BTC was traded
                For BTC-USD, this would be the number of BTC; multiply by price for dollar volume
            - exchange (int): Exchange ID where this trade occurred
                Example: 1, 2, 4, 6, etc.
                Use polygon_crypto_exchanges tool to map IDs to exchange names
                Common exchanges: 1=Coinbase, 2=Bitfinex, 4=Kraken, 6=Bitstamp
            - participant_timestamp (int): Nanosecond-precision Unix timestamp from the exchange
                Example: 1625097600103000000 (nanoseconds since epoch)
                This is the original timestamp from the exchange's trade feed
                Provides highest precision timing for latency analysis
            - timestamp_readable (str): Human-readable timestamp in 'YYYY-MM-DD HH:MM:SS.ffffff' format
                Example: "2021-07-01 00:00:00.103000"
                Converted from participant_timestamp for easier analysis
                Includes microsecond precision for detailed timing studies
            - conditions (JSON string): Array of condition codes that apply to this trade
                Example: "[1]" or "[1, 2]"
                Parse with json.loads() to get the array in Python
                Use polygon_crypto_conditions tool to decode condition meanings
                Conditions explain trade circumstances (e.g., regular trade, corrected, odd lot)

        Use Cases:
            - **Market Microstructure Analysis**: Study bid-ask spreads, price impact, and order flow
            - **Algorithmic Trading Development**: Build and backtest high-frequency trading strategies
            - **VWAP/TWAP Analysis**: Calculate custom volume/time-weighted average prices
            - **Order Flow Analysis**: Analyze buying vs. selling pressure from trade sequences
            - **Slippage Analysis**: Measure execution quality by comparing order prices to trades
            - **Price Discovery Research**: Understand how prices form through individual trades
            - **Exchange Comparison**: Compare trade prices and volumes across different exchanges
            - **Latency Analysis**: Measure timing between trades using nanosecond timestamps
            - **Liquidity Analysis**: Identify periods of high/low liquidity from trade frequency
            - **Historical Backtesting**: Test trading strategies on actual historical trades
            - **Market Making**: Analyze spreads and trade patterns for market making strategies
            - **Tape Reading**: Study order flow patterns for discretionary trading decisions

        Important Notes:
            - This is the most granular trade data available (tick-by-tick)
            - Trades occur 24/7 in crypto markets (no market hours)
            - Data can be extremely large - 10,000 trades might represent just minutes during active periods
            - Each trade is timestamped with nanosecond precision for detailed timing analysis
            - Trades are aggregated across multiple exchanges (hence the "X:" prefix)
            - Condition codes provide important context about trade circumstances
            - Price represents actual executed price; multiply by size for dollar volume
            - Size is always in base currency units (e.g., BTC for X:BTC-USD)
            - Exchange ID maps to specific crypto venues (use polygon_crypto_exchanges to decode)
            - For OHLC analysis, use polygon_crypto_aggregates instead (more efficient)
            - This data is essential for understanding what happened "inside the bar"

        Performance Considerations:
            - Start with small limits (1000-10000) to understand data size and structure
            - For long-term analysis, use polygon_crypto_aggregates (bars) instead
            - Trade data can be several orders of magnitude larger than aggregate data
            - Consider filtering by timestamp to focus on specific time periods
            - Use py_eval to calculate custom statistics and aggregations efficiently

        Example Workflow:
            1. Call this tool to fetch trades for X:BTC-USD on a specific date
            2. Receive CSV filename in response
            3. Use py_eval to load CSV and analyze trade patterns
            4. Calculate custom metrics (VWAP, spread, trade size distribution)
            5. Cross-reference exchange IDs with polygon_crypto_exchanges
            6. Parse conditions array to filter for specific trade types
            7. Build visualizations or statistical models from the trade data

        Trading Strategy Examples:
            - **Volume Profile**: Build volume-at-price histograms from trade data
            - **Time & Sales**: Replicate traditional "time and sales" tape for pattern recognition
            - **Order Imbalance**: Calculate buy/sell imbalances from trade sequences
            - **Momentum Indicators**: Detect rapid price movements from trade velocity
            - **Smart Order Routing**: Analyze which exchanges have best prices/liquidity

        Always use py_eval tool to analyze the saved CSV file for detailed trade analysis and insights.

        Example usage:
            polygon_crypto_trades(ticker="X:BTC-USD", timestamp="2023-02-01", limit=1000)
        """
        logger.info(f"polygon_crypto_trades invoked: ticker={ticker}, timestamp={timestamp or 'recent'}, limit={limit}")

        try:
            # Convert empty string to None for proper default handling
            timestamp_param = timestamp if timestamp else None

            # Fetch crypto trades
            df = fetch_crypto_trades(
                polygon_client=local_polygon_client,
                ticker=ticker,
                timestamp=timestamp_param,
                limit=limit
            )

            if df.empty:
                return f"No trade data were retrieved for {ticker}. Please check the ticker format (should be X:BASE-QUOTE, e.g., X:BTC-USD) and ensure the timestamp is valid."

            # Always save to CSV file
            # Clean ticker for filename (replace : and - with _)
            clean_ticker = ticker.replace(':', '_').replace('-', '_')
            filename = f"crypto_trades_{clean_ticker}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto trades to {filename} ({len(df)} records)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_trades: {e}")
            raise
