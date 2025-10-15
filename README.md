# Polygon MCP Server

MCP server providing Polygon.io API access for building AI-powered cryptocurrency market analysis and trading agents through the Model Context Protocol.

## Architecture

**CSV-First Data Design**

All tools return structured CSV files instead of raw JSON responses, encouraging systematic data analysis workflows:

- **Output**: Every tool saves results to CSV files in the server's `data/mcp-polygon/` folder
- **Analysis**: CSV files are analyzed using the `py_eval` tool with pandas/numpy pre-loaded
- **Pattern**: Fetch data → Receive CSV path → Analyze with Python → Make decisions
- **Benefits**: Promotes data-driven reasoning, enables complex multi-step analysis, supports historical tracking

**Modular Design**

Each tool is implemented in a separate file (`backend/polygon_tools/*.py`) with comprehensive parameter documentation, minimizing token usage and improving maintainability.

## Available Tools

### Market News & Reference Data
- `polygon_news` - Market news articles with filtering by ticker and date range
- `polygon_ticker_details` - Detailed information about specific tickers
- `polygon_market_holidays` - Market holiday calendar
- `polygon_market_status` - Current market status (open/closed)

### Real-Time Price Data
- `polygon_crypto_last_trade` - Most recent trade execution for a crypto pair
- `polygon_crypto_snapshot_ticker` - Real-time snapshot with current price, volume, and 24h statistics
- `polygon_crypto_snapshot_book` - Current order book with bid/ask prices and sizes
- `polygon_crypto_snapshots` - Multi-ticker snapshots for quick market overview
- `polygon_crypto_gainers_losers` - Top gaining and losing cryptocurrencies

### Historical OHLCV Data
- `polygon_crypto_aggregates` - Historical aggregate bars (OHLCV) with customizable timeframes
- `polygon_crypto_previous_close` - Previous day's closing data
- `polygon_crypto_daily_open_close` - Daily OHLC data for specific dates
- `polygon_crypto_grouped_daily` - Grouped daily bars across multiple crypto pairs
- `polygon_crypto_trades` - Historical trade-by-trade data

### Technical Indicators
- `polygon_crypto_rsi` - Relative Strength Index (overbought/oversold momentum)
- `polygon_crypto_ema` - Exponential Moving Average (trend following)
- `polygon_crypto_macd` - Moving Average Convergence Divergence (trend and momentum)
- `polygon_crypto_sma` - Simple Moving Average (trend identification)

### Reference Data
- `polygon_crypto_tickers` - List of available cryptocurrency tickers
- `polygon_crypto_exchanges` - Cryptocurrency exchange information
- `polygon_crypto_conditions` - Trade condition codes and descriptions

### Analysis Tools
- `py_eval` - Execute Python code with pandas/numpy for data analysis
- `polygon_price_data` - General price data fetching (supports stocks and crypto)

## Typical Workflow

```python
# 1. Fetch cryptocurrency aggregate data (OHLCV)
polygon_crypto_aggregates(
    ticker="X:BTCUSD",
    from_date="2025-09-01",
    to_date="2025-10-15",
    timespan="hour"
)
# Returns: "✓ Data saved to CSV\nFile: crypto_aggs_X_BTCUSD_a1b2c3d4.csv..."

# 2. Fetch RSI technical indicator
polygon_crypto_rsi(
    ticker="X:BTCUSD",
    timespan="day",
    window=14,
    limit=120
)
# Returns: "✓ Data saved to CSV\nFile: crypto_rsi_X_BTCUSD_w14_day_e5f6g7h8.csv..."

# 3. Analyze the CSV files with Python
py_eval("""
import pandas as pd
import numpy as np

# Load price data
df_price = pd.read_csv('data/mcp-polygon/crypto_aggs_X_BTCUSD_a1b2c3d4.csv')
df_price['timestamp'] = pd.to_datetime(df_price['timestamp'])

# Load RSI data
df_rsi = pd.read_csv('data/mcp-polygon/crypto_rsi_X_BTCUSD_w14_day_e5f6g7h8.csv')
df_rsi['timestamp'] = pd.to_datetime(df_rsi['timestamp'])

# Calculate key metrics
current_price = df_price.iloc[-1]['close']
price_change_24h = ((current_price - df_price.iloc[-24]['close']) / df_price.iloc[-24]['close']) * 100
current_rsi = df_rsi.iloc[0]['rsi_value']  # Most recent RSI

print(f"Bitcoin (BTC/USD) Analysis")
print(f"Current Price: ${current_price:,.2f}")
print(f"24h Change: {price_change_24h:+.2f}%")
print(f"RSI (14-day): {current_rsi:.2f}")

# Trading signal logic
if current_rsi < 30:
    print("Signal: OVERSOLD - Potential BUY opportunity")
elif current_rsi > 70:
    print("Signal: OVERBOUGHT - Potential SELL signal")
else:
    print("Signal: NEUTRAL - No clear momentum signal")
""")

# 4. Fetch news for context
polygon_news(ticker="BTC", start_date="2025-10-10", end_date="2025-10-15")

# 5. Make data-driven trading decisions based on combined analysis
```

## Setup

### Requirements
- Docker and Docker Compose
- Polygon.io API key (free tier available, paid for advanced features)

### Environment Configuration

Create `.env.local` file:

```bash
# Polygon API Credentials
POLYGON_API_KEY=your_polygon_api_key_here

# MCP Configuration
MCP_NAME=polygon
MCP_TOKENS=your_secure_token_here
MCP_REQUIRE_AUTH=true
MCP_ALLOW_URL_TOKENS=true
PORT=8009

# Optional: Sentry error tracking
SENTRY_DSN=your_sentry_dsn
CONTAINER_NAME=mcp-polygon-local
```

### Deployment

```bash
# Local development
./compose.local.sh

# Service runs on http://localhost:8009/polygon/

# Production deployment
./compose.prod.sh

# View logs
./logs.sh
```

### Claude Desktop Configuration

Add to your Claude Desktop config file:

```json
{
  "mcpServers": {
    "polygon": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://localhost:8009/polygon/"
      ]
    }
  }
}
```

**Health check endpoint:** `http://localhost:8009/health`

## Security Notes

- **API Key Management**: Store your Polygon.io API key securely in environment variables
- **Read-Only Tools**: All tools are read-only data fetching operations
- **Token Authentication**: Required for production use (MCP_REQUIRE_AUTH=true)
- **Rate Limits**: Polygon.io enforces rate limits based on your subscription tier
- **Data Privacy**: All CSV files are stored locally on the server

## CSV Data Persistence

All CSV files are stored in `data/mcp-polygon/` with unique identifiers, enabling:
- Historical market data tracking and backtesting
- Performance analysis over time
- Systematic research workflows
- Reproducible analysis and audit trails
- Multi-session data accumulation for pattern recognition

## Tool Documentation

Each tool includes comprehensive inline documentation with:
- Detailed parameter descriptions and valid ranges
- Return value schemas with data types
- Use cases and trading strategy examples
- Technical indicator interpretation guides
- Python analysis suggestions and best practices

Run tools without parameters to see full documentation.

## Use Cases

- **Market Research**: Analyze crypto price trends, volatility, and correlations
- **Technical Analysis**: Calculate and visualize RSI, MACD, EMA, and other indicators
- **Trading Signals**: Identify overbought/oversold conditions and trend reversals
- **News Sentiment**: Track market-moving news and events
- **Backtesting**: Test trading strategies with historical OHLCV data
- **Market Monitoring**: Real-time snapshots and gainers/losers tracking
- **Risk Management**: Analyze volatility and price patterns before making decisions

## API Coverage

This MCP server focuses on cryptocurrency market data from Polygon.io, including:
- Real-time and historical crypto prices (Bitcoin, Ethereum, and 100+ altcoins)
- Technical indicators (RSI, EMA, MACD, SMA)
- Market news and sentiment data
- Trade-level data for detailed execution analysis
- Reference data (exchanges, tickers, conditions)

For stock market data, use `polygon_price_data` and `polygon_ticker_details` tools.

## License

MIT License - See LICENSE file for details.
