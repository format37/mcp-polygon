import logging
import uuid
import pandas as pd
from typing import Optional
from pathlib import Path
from polygon import RESTClient
from mcp_service import format_csv_response

logger = logging.getLogger(__name__)


def fetch_crypto_snapshot_book(
    polygon_client: RESTClient,
    ticker: str,
) -> pd.DataFrame:
    """
    Fetch Level 2 order book snapshot data for a cryptocurrency ticker.

    This endpoint retrieves the full order book (bids and asks) for a single ticker,
    aggregated from all exchanges. Level 2 data provides depth-of-market visibility
    showing all price levels and their corresponding volumes.

    Args:
        polygon_client: Initialized Polygon RESTClient
        ticker: Cryptocurrency ticker symbol in X:BASEUSD format (e.g., "X:BTCUSD")

    Returns:
        DataFrame with one row per price level, containing:
        - ticker: Ticker symbol
        - updated_at: Snapshot timestamp
        - side: "bid" or "ask"
        - price: Price level
        - exchange_id: Exchange identifier
        - price_aggregate: Aggregated price (if available)

    Note:
        - Snapshot data is cleared at 12:00 AM EST daily
        - Data begins updating as exchanges report new information
        - Combined book from all exchanges
    """
    logger.info(f"Fetching L2 order book for ticker: {ticker}")

    try:
        # Get L2 order book snapshot
        # API call: GET /v2/snapshot/locale/global/markets/crypto/tickers/{ticker}/book
        snapshot = polygon_client.get_snapshot_crypto_book(ticker)

        if not snapshot:
            logger.warning(f"No order book data returned for ticker: {ticker}")
            return pd.DataFrame()

        # Extract metadata
        ticker_symbol = getattr(snapshot, 'ticker', ticker)
        updated = getattr(snapshot, 'updated', None)

        # Extract bids and asks
        bids = getattr(snapshot, 'bids', [])
        asks = getattr(snapshot, 'asks', [])

        logger.info(f"Extracted {len(bids)} bid levels and {len(asks)} ask levels for {ticker}")

        records = []

        # Process bid levels
        for bid in bids:
            if hasattr(bid, '__dict__'):
                # Object with attributes
                record = {
                    'ticker': ticker_symbol,
                    'updated_at': updated,
                    'side': 'bid',
                    'price': getattr(bid, 'p', getattr(bid, 'price', None)),
                    'exchange_id': getattr(bid, 'x', getattr(bid, 'exchange', None)),
                }
                # Handle aggregate price if available (some responses include 'a')
                if hasattr(bid, 'a'):
                    record['price_aggregate'] = getattr(bid, 'a', None)
            elif isinstance(bid, dict):
                # Dictionary response
                record = {
                    'ticker': ticker_symbol,
                    'updated_at': updated,
                    'side': 'bid',
                    'price': bid.get('p', bid.get('price')),
                    'exchange_id': bid.get('x', bid.get('exchange')),
                }
                if 'a' in bid:
                    record['price_aggregate'] = bid.get('a')
            else:
                # Fallback for unexpected format
                logger.warning(f"Unexpected bid format: {type(bid)}")
                continue

            records.append(record)

        # Process ask levels
        for ask in asks:
            if hasattr(ask, '__dict__'):
                # Object with attributes
                record = {
                    'ticker': ticker_symbol,
                    'updated_at': updated,
                    'side': 'ask',
                    'price': getattr(ask, 'p', getattr(ask, 'price', None)),
                    'exchange_id': getattr(ask, 'x', getattr(ask, 'exchange', None)),
                }
                # Handle aggregate price if available
                if hasattr(ask, 'a'):
                    record['price_aggregate'] = getattr(ask, 'a', None)
            elif isinstance(ask, dict):
                # Dictionary response
                record = {
                    'ticker': ticker_symbol,
                    'updated_at': updated,
                    'side': 'ask',
                    'price': ask.get('p', ask.get('price')),
                    'exchange_id': ask.get('x', ask.get('exchange')),
                }
                if 'a' in ask:
                    record['price_aggregate'] = ask.get('a')
            else:
                # Fallback for unexpected format
                logger.warning(f"Unexpected ask format: {type(ask)}")
                continue

            records.append(record)

        if not records:
            logger.warning(f"No order book records extracted for {ticker}")
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Convert timestamp columns to datetime
        if not df.empty and 'updated_at' in df.columns:
            df['updated_at'] = pd.to_datetime(df['updated_at'], unit='ms', errors='coerce')

        # Sort by side (asks first, then bids) and price
        # For asks: sort ascending (lowest ask first)
        # For bids: sort descending (highest bid first)
        if not df.empty:
            df_asks = df[df['side'] == 'ask'].sort_values('price', ascending=True)
            df_bids = df[df['side'] == 'bid'].sort_values('price', ascending=False)
            df = pd.concat([df_bids, df_asks], ignore_index=True)

        logger.info(f"Successfully fetched L2 order book for {ticker}: {len(df)} price levels")
        return df

    except Exception as e:
        logger.error(f"Error fetching L2 order book for {ticker}: {e}")
        raise


def register_polygon_crypto_snapshot_book(local_mcp_instance, local_polygon_client, csv_dir):
    """Register the polygon_crypto_snapshot_book tool"""
    @local_mcp_instance.tool()
    def polygon_crypto_snapshot_book(
        ticker: str
    ) -> str:
        """
        Fetch Level 2 (L2) order book snapshot for a cryptocurrency and save to CSV.

        This endpoint retrieves the full depth-of-market order book for a single cryptocurrency,
        aggregated from all exchanges. Level 2 data provides complete visibility into all
        bid and ask price levels with their associated volumes and exchange information.
        This is critical data for understanding market depth, liquidity, and order flow.

        The order book shows all pending orders at different price levels:
        - **Bids**: Buy orders (demand side) - sorted from highest to lowest price
        - **Asks**: Sell orders (supply side) - sorted from lowest to highest price

        Snapshot data is cleared at 12:00 AM EST and begins updating as exchanges
        report new information. This provides a real-time view of the order book state.

        Parameters:
            ticker (str, required): Cryptocurrency ticker symbol in X:BASEUSD format.
                Examples:
                - "X:BTCUSD" (Bitcoin/USD)
                - "X:ETHUSD" (Ethereum/USD)
                - "X:SOLUSD" (Solana/USD)
                - "X:ADAUSD" (Cardano/USD)
                - "X:DOGEUSD" (Dogecoin/USD)
                - "X:MATICUSD" (Polygon/USD)
                - "X:AVAXUSD" (Avalanche/USD)
                - "X:LINKUSD" (Chainlink/USD)

                Note: The 'X:' prefix indicates crypto exchange aggregation across all venues

        Returns:
            str: Formatted response with CSV file info, schema, sample data, and Python snippet.

        CSV Output Columns (one row per price level):
            - ticker (str): Cryptocurrency ticker symbol (e.g., "X:BTCUSD")
            - updated_at (datetime): Snapshot timestamp in 'YYYY-MM-DD HH:MM:SS' format
            - side (str): Order side - "bid" (buy orders) or "ask" (sell orders)
            - price (float): Price level in USD
            - exchange_id (int): Exchange identifier where this order exists
            - price_aggregate (float, optional): Aggregated price across exchanges (if available)

        Row Organization:
            - Bids are sorted from highest to lowest price (best bid first)
            - Asks are sorted from lowest to highest price (best ask first)
            - Best bid = highest bid price (top of demand)
            - Best ask = lowest ask price (top of supply)
            - Spread = best_ask - best_bid (measure of market liquidity)

        Use Cases:
            - **Market Making**: Analyze order book depth to place optimal bid/ask orders
            - **Liquidity Analysis**: Assess market depth and available liquidity at different price levels
            - **Spread Analysis**: Calculate bid-ask spread to measure trading costs
            - **Order Execution**: Determine optimal order sizing based on book depth
            - **Price Impact Estimation**: Model expected slippage for large orders
            - **Market Microstructure Research**: Study order book dynamics and price formation
            - **Arbitrage Detection**: Compare order books across exchanges for opportunities
            - **Support/Resistance Levels**: Identify significant price levels with large orders
            - **Order Flow Analysis**: Track changes in order book structure over time
            - **Trading Algorithm Development**: Feed order book data into execution algorithms
            - **Risk Management**: Monitor liquidity conditions before placing large orders

        Trading Strategy Applications:
            1. **VWAP/TWAP Execution**: Use order book to optimize volume-weighted execution
            2. **Iceberg Order Detection**: Identify hidden liquidity at specific price levels
            3. **Order Book Imbalance**: Calculate bid/ask volume ratio for directional signals
            4. **Liquidity-Based Entries**: Enter positions when sufficient depth is available
            5. **Market Impact Modeling**: Estimate price impact before execution

        Important Notes:
            - Snapshot data resets daily at 12:00 AM EST (midnight Eastern Time)
            - Data begins repopulating from 4:00 AM EST as exchanges report trades
            - Crypto markets trade 24/7, so order book updates continuously
            - Returns combined order book from ALL exchanges (aggregated view)
            - Order book is a snapshot - changes constantly in real-time
            - Large orders may be split across multiple exchanges
            - Exchange IDs can be mapped using polygon_crypto_exchanges tool
            - Not all exchanges report full order book depth
            - Some price levels may have partial fills between snapshot updates

        Performance Tips:
            - Poll regularly (e.g., every 5-10 seconds) for near-real-time order book tracking
            - Combine with polygon_crypto_last_trade to correlate trades with book changes
            - Use polygon_crypto_snapshot_ticker for summary metrics (VWAP, day ranges)
            - Compare with polygon_crypto_aggregates for historical price context
            - Monitor spread changes over time to assess market volatility

        Data Quality Considerations:
            - Order book depth varies by cryptocurrency and time of day
            - More liquid pairs (BTC, ETH) typically have deeper books
            - During high volatility, order book may thin out rapidly
            - Exchange outages can temporarily reduce visible liquidity
            - Weekend trading may show reduced depth vs. weekday trading

        Example Workflow:
            1. Call polygon_crypto_snapshot_book(ticker="X:BTCUSD")
            2. Receive CSV filename with full order book data
            3. Use py_eval to load CSV and analyze:
               - Calculate spread = min(ask_price) - max(bid_price)
               - Sum bid volumes vs. ask volumes (order book imbalance)
               - Identify price levels with unusually large orders
               - Estimate market impact for a specific trade size
            4. Make trading decisions based on order book structure

        Example Analysis (py_eval):
            ```python
            import pandas as pd
            df = pd.read_csv('data/mcp-polygon/crypto_book_X_BTCUSD_<id>.csv')

            # Calculate best bid and ask
            best_bid = df[df['side'] == 'bid']['price'].max()
            best_ask = df[df['side'] == 'ask']['price'].min()
            spread = best_ask - best_bid

            # Calculate order book imbalance
            bid_count = len(df[df['side'] == 'bid'])
            ask_count = len(df[df['side'] == 'ask'])
            imbalance = (bid_count - ask_count) / (bid_count + ask_count)

            print(f"Best Bid: ${best_bid:.2f}")
            print(f"Best Ask: ${best_ask:.2f}")
            print(f"Spread: ${spread:.2f}")
            print(f"Order Book Imbalance: {imbalance:.2%}")
            print(f"Bid Levels: {bid_count}, Ask Levels: {ask_count}")
            ```

        Always use py_eval tool to analyze the saved CSV file for insights.

        Example usage:
            polygon_crypto_snapshot_book(ticker="X:BTCUSD")
        """
        logger.info(f"polygon_crypto_snapshot_book invoked for ticker: {ticker}")

        # Validate ticker parameter
        if not ticker or not ticker.strip():
            return "Error: 'ticker' parameter is required. Please provide a cryptocurrency ticker in X:BASEUSD format (e.g., 'X:BTCUSD')"

        ticker = ticker.strip()

        try:
            # Fetch L2 order book snapshot
            df = fetch_crypto_snapshot_book(
                polygon_client=local_polygon_client,
                ticker=ticker,
            )

            if df.empty:
                return f"No order book data found for ticker '{ticker}'.\n\nPossible causes:\n1. Ticker format might be incorrect (should be X:BASEUSD, e.g., X:BTCUSD)\n2. Ticker might not exist or be valid\n3. Order book data might not be available yet (resets at 12 AM EST)\n4. This endpoint may require a paid Polygon.io plan\n5. Market might be closed or experiencing connectivity issues\n\nPlease verify the ticker symbol and try again."

            # Generate filename with ticker in name
            ticker_clean = ticker.replace(':', '_')
            filename = f"crypto_book_{ticker_clean}_{uuid.uuid4().hex[:8]}.csv"
            filepath = csv_dir / filename

            # Save to CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Saved L2 order book to {filename} ({len(df)} price levels)")

            # Return formatted response
            return format_csv_response(filepath, df)

        except Exception as e:
            logger.error(f"Error in polygon_crypto_snapshot_book: {e}")
            return f"Error fetching L2 order book for ticker '{ticker}': {str(e)}"
