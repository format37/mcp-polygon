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


def fetch_crypto_last_trade(
    polygon_client: RESTClient,
    from_symbol: str,
    to_symbol: str,
) -> pd.DataFrame:
    """
    Fetch the most recent trade for a specified cryptocurrency pair.

    This endpoint retrieves the last trade details including price, size, timestamp,
    exchange, and conditions. Provides real-time market information for trading decisions.

    Args:
        polygon_client: Initialized Polygon RESTClient
        from_symbol: The "from" symbol of the crypto pair (e.g., "BTC", "ETH", "SOL")
        to_symbol: The "to" symbol of the crypto pair (e.g., "USD", "USDT", "EUR")

    Returns:
        DataFrame with the most recent trade data for the crypto pair

    Note:
        This endpoint provides real-time trade data for rapid decision-making,
        market monitoring, and integration into trading systems.
    """
    logger.info(f"Fetching crypto last trade for {from_symbol}/{to_symbol}")

    try:
        # Get the last crypto trade
        trade = polygon_client.get_last_crypto_trade(from_symbol, to_symbol)

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

        return df

    except Exception as e:
        logger.error(f"Error fetching crypto last trade for {from_symbol}/{to_symbol}: {e}")
        raise


def register_polygon_crypto_last_trade(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_last_trade tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_last_trade(
        from_symbol: str,
        to_symbol: str = "USD"
    ) -> str:
        """
        Fetch the most recent trade for a specified cryptocurrency pair and save to CSV file.

        This endpoint retrieves the latest trade details for a crypto pair, including price, size,
        timestamp, exchange, and conditions. Provides up-to-date market information for real-time
        monitoring, rapid decision-making, and integration into crypto trading or analytics tools.

        Parameters:
            from_symbol (str, required): The base cryptocurrency symbol.
                Examples: "BTC" (Bitcoin), "ETH" (Ethereum), "SOL" (Solana), "ADA" (Cardano),
                         "DOGE" (Dogecoin), "MATIC" (Polygon), "AVAX" (Avalanche), "LINK" (Chainlink),
                         "DOT" (Polkadot), "UNI" (Uniswap), "ATOM" (Cosmos), "XLM" (Stellar)
                Note: Use the symbol only (e.g., "BTC"), not the full ticker format (not "X:BTCUSD")

            to_symbol (str, optional): The quote currency symbol.
                Common options: "USD" (US Dollar), "USDT" (Tether), "USDC" (USD Coin), "EUR" (Euro)
                Default: "USD"
                Note: Most liquid pairs use USD or USDT as the quote currency

        Returns:
            str: Formatted response with file info, schema, sample data, and Python snippet to load the CSV.

        CSV Output Structure:
            - symbol (str): The symbol pair evaluated (e.g., "BTC-USD")
            - price (float): The price of the most recent trade in quote currency
                This is the actual dollar value per whole unit of the cryptocurrency
                Example: 45000.50 means $45,000.50 per 1 BTC
            - size (float): The size of the trade (volume in base currency units)
                Example: 0.015 means 0.015 BTC was traded
            - exchange (int): The exchange ID where this trade occurred
                Use polygon_crypto_exchanges tool to map exchange IDs to exchange names
                Example: 4 = Coinbase, 6 = Kraken (see exchanges reference)
            - timestamp (int): Unix millisecond timestamp when the trade occurred
                Example: 1605560885027 represents the exact moment of the trade
            - timestamp_readable (str): Human-readable timestamp in 'YYYY-MM-DD HH:MM:SS.mmm' format
                Example: "2024-01-15 14:23:45.123"
                Includes milliseconds for precise timing analysis
            - conditions (JSON string): Array of condition codes that apply to this trade
                Example: "[1]" indicates condition code 1 (see polygon_crypto_conditions for details)
                Parse with json.loads() in Python to get the array
                Conditions explain special circumstances (e.g., averaged price, off-exchange)
            - request_id (str): Unique identifier for this API request (for debugging/tracking)
            - status (str): Status of the API response (typically "success" or "error")

        Use Cases:
            - **Real-Time Price Monitoring**: Get the absolute latest trade price for a crypto pair
            - **Trading Execution Analysis**: Verify execution prices against last trade data
            - **Market Data Validation**: Compare your data feeds against Polygon's last trade
            - **Price Alerts**: Build alert systems that trigger on the most recent trade price
            - **Order Book Analysis**: Understand the last executed price vs. current bid/ask
            - **Latency Testing**: Measure data feed delays by comparing timestamps
            - **Slippage Analysis**: Calculate slippage by comparing order price vs. last trade
            - **Market Making**: Monitor last trade to adjust quotes and spreads
            - **Arbitrage Detection**: Compare last trade prices across different sources
            - **Algorithm Development**: Use real-time trades to train and validate trading models

        Important Notes:
            - Returns the single most recent trade only (not historical trades)
            - Crypto markets trade 24/7, so this data is always fresh (no market hours)
            - Trade timestamp is extremely precise (millisecond resolution)
            - Exchange ID maps to specific crypto exchanges (use polygon_crypto_exchanges to decode)
            - Condition codes provide context about the trade (use polygon_crypto_conditions to decode)
            - Price represents the actual executed price at that moment
            - Size is denominated in the base currency (e.g., for BTC-USD, size is in BTC)
            - This is snapshot data; for time-series analysis use polygon_crypto_aggregates

        Example Workflow:
            1. Call this tool to fetch the last trade for BTC/USD
            2. Receive CSV filename in response
            3. Use py_eval to load and analyze the CSV file
            4. Extract price, size, timestamp for trading decisions
            5. Cross-reference exchange ID with polygon_crypto_exchanges for venue details
            6. Parse conditions array to understand trade circumstances

        Performance Tips:
            - Very fast endpoint (single trade lookup)
            - Perfect for real-time dashboards and monitoring
            - For historical trade data, use polygon_crypto_aggregates instead
            - Combine with polygon_crypto_conditions to understand trade flags
            - Use polygon_crypto_exchanges to map exchange IDs to exchange names

        Always use py_eval tool to analyze the saved CSV file for trading decisions and real-time insights.
        """
        logger.info(f"polygon_crypto_last_trade invoked: from_symbol={from_symbol}, to_symbol={to_symbol}")

        try:
            # Fetch crypto last trade
            df = fetch_crypto_last_trade(
                polygon_client=local_polygon_client,
                from_symbol=from_symbol,
                to_symbol=to_symbol,
            )

            if df.empty:
                return f"No last trade data were retrieved for {from_symbol}/{to_symbol}. Please check the symbol format (e.g., 'BTC', 'ETH') and ensure it's a valid cryptocurrency pair."

            # Always save to CSV file
            # Clean symbols for filename
            filename = f"crypto_last_trade_{from_symbol}_{to_symbol}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename
            df.to_csv(filepath, index=False)
            logger.info(f"Saved crypto last trade to {filename}")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_last_trade: {e}")
            raise
