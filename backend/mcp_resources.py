import logging

logger = logging.getLogger(__name__)


def register_mcp_resources(local_mcp_instance, safe_name):
    """Register MCP resources (documentation, etc.)"""

    @local_mcp_instance.resource(
        f"{safe_name}://documentation",
        name="Polygon API Documentation",
        description="Documentation for Polygon.io financial market data API",
        mime_type="text/markdown"
    )
    def get_documentation_resource() -> str:
        """Expose Polygon API documentation as an MCP resource."""
        return """# Polygon API Documentation

## Available Tools

### polygon_news
Fetches market news from Polygon.io financial news aggregator.

**Parameters:**
- `start_date` (optional): Start date in 'YYYY-MM-DD' format
- `end_date` (optional): End date in 'YYYY-MM-DD' format
- `save_csv` (optional): If True, saves data to CSV file

**Returns:** Formatted table with datetime and topic columns, or CSV filename if save_csv=True.

### polygon_ticker_details
Fetch comprehensive ticker details including company info, market cap, sector, etc.

**Parameters:**
- `tickers` (required): List of ticker symbols (e.g., ['AAPL', 'MSFT'])
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_data
Fetch historical price data for tickers.

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `timespan` (optional): Time span (day, week, month, quarter, year)
- `multiplier` (optional): Size of the time window
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

### polygon_price_metrics
Calculate price-based metrics for tickers (volatility, returns, etc.).

**Parameters:**
- `tickers` (required): List of ticker symbols
- `from_date` (optional): Start date in YYYY-MM-DD format (defaults to 30 days ago)
- `to_date` (optional): End date in YYYY-MM-DD format (defaults to today)
- `save_csv` (optional): If True, saves data to CSV file and returns filename

**Returns:** Success message with CSV filename or summary information.

---

## py_eval Tool - Python Data Analysis

Execute Python code with pandas/numpy pre-loaded and access to CSV folder.

**Parameters:**
- `code` (required): Python code to execute
- `timeout_sec` (optional): Execution timeout in seconds (default: 5.0)

**⚠️ IMPORTANT NOTES:**
- **Variable Persistence**: Each py_eval call starts fresh - variables do NOT persist between calls
- **Reload Data**: You must reload CSV files in each py_eval call that needs them
- **Temporary Files**: You can save temporary files within a py_eval session and read them back in the same call
- **Libraries**: pandas (pd), numpy (np) are pre-loaded. Standard library modules available.

### Available Variables
- `pd`: pandas library (version 2.2.3+)
- `np`: numpy library (version 1.26.4+)
- `CSV_PATH`: string path to `/work/csv/` folder containing all CSV files

### CSV File Types and Structures

#### 1. Price Data Files (`price_data_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `timestamp` (str): Trading session timestamp 'YYYY-MM-DD HH:MM:SS'
- `open`, `high`, `low`, `close` (float): OHLC prices
- `volume` (float): Trading volume
- `vwap` (float): Volume Weighted Average Price
- `transactions` (int): Number of transactions
- `date` (str): Trading date 'YYYY-MM-DD'

#### 2. Ticker Details Files (`ticker_details_*.csv`)
**Key Columns:**
- `ticker` (str): Stock ticker symbol
- `name` (str): Company full name
- `market_cap` (float): Market capitalization in USD
- `total_employees` (int): Number of employees
- `description` (str): Company description
- `homepage_url` (str): Company website
- `sic_code` (int): Standard Industrial Classification
- `sic_description` (str): Industry sector description
- Plus 20+ additional company details columns

#### 3. Price Metrics Files (`price_metrics_*.csv`)
**Columns:**
- `ticker` (str): Stock ticker symbol
- `start_date`, `end_date` (str): Analysis period 'YYYY-MM-DD'
- `trading_days` (int): Number of trading days
- `start_price`, `end_price` (float): Period start/end prices
- `high_price`, `low_price` (float): Period high/low prices
- `total_return` (float): Total return as decimal (0.12 = 12%)
- `volatility` (float): Price volatility (std dev of daily returns)
- `avg_daily_volume`, `total_volume` (float): Volume metrics
- `price_range_ratio` (float): (High-Low)/Low ratio

#### 4. News Files (`news_*.csv`)
**Columns:**
- `datetime` (str): Publication date/time 'YYYY-MM-DD HH:MM:SS'
- `topic` (str): News headline/title

---

## Python Analysis Examples

### 1. CSV File Discovery and Loading

```python
import os

# List all CSV files
print("=== CSV FILES AVAILABLE ===")
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
print(f"Found {len(files)} CSV files:")

# Group by type
price_data_files = [f for f in files if 'price_data_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]
metrics_files = [f for f in files if 'price_metrics_' in f]
news_files = [f for f in files if 'news_' in f]

print(f"Price data: {len(price_data_files)} files")
print(f"Ticker details: {len(ticker_files)} files")
print(f"Price metrics: {len(metrics_files)} files")
print(f"News: {len(news_files)} files")

# Load the most recent files (if they exist)
if price_data_files:
    price_df = pd.read_csv(f'{CSV_PATH}/{price_data_files[-1]}')
    print(f"\nLoaded price data: {price_df.shape} - {list(price_df.columns)}")

if ticker_files:
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')
    print(f"Loaded ticker details: {ticker_df.shape}")
```

### 2. Portfolio Performance Analysis

```python
# Reload data (required in each py_eval call)
import os
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    # Load latest files
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Risk-adjusted performance analysis
    metrics_df['risk_adj_return'] = metrics_df['total_return'] / metrics_df['volatility']

    print("=== TOP PERFORMERS (Risk-Adjusted) ===")
    top_performers = metrics_df.nlargest(5, 'risk_adj_return')
    for _, stock in top_performers.iterrows():
        print(f"{stock['ticker']}: Return={stock['total_return']*100:.1f}%, "
              f"Vol={stock['volatility']*100:.1f}%, Risk-Adj={stock['risk_adj_return']:.2f}")

    # Market cap analysis
    combined = pd.merge(metrics_df, ticker_df[['ticker', 'market_cap']], on='ticker', how='left')
    combined['market_cap_billions'] = combined['market_cap'] / 1e9

    print("\n=== LARGE CAP PERFORMANCE ===")
    large_caps = combined[combined['market_cap_billions'] > 1000]
    large_caps_sorted = large_caps.sort_values('total_return', ascending=False)
    print(large_caps_sorted[['ticker', 'market_cap_billions', 'total_return']].round(3))
```

### 3. Technical Analysis with Price Data

```python
# Load price data for technical analysis
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
price_files = [f for f in files if 'price_data_' in f]

if price_files:
    df = pd.read_csv(f'{CSV_PATH}/{price_files[-1]}')

    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])

    print("=== TECHNICAL ANALYSIS ===")
    for ticker in df['ticker'].unique():
        ticker_data = df[df['ticker'] == ticker].sort_values('date')

        # Calculate moving averages
        ticker_data['sma_5'] = ticker_data['close'].rolling(window=5).mean()
        ticker_data['sma_20'] = ticker_data['close'].rolling(window=20).mean()

        # Calculate daily returns
        ticker_data['daily_return'] = ticker_data['close'].pct_change()

        # Recent metrics
        recent_return = ticker_data['daily_return'].tail(5).mean() * 100
        current_price = ticker_data['close'].iloc[-1]
        sma5 = ticker_data['sma_5'].iloc[-1]
        sma20 = ticker_data['sma_20'].iloc[-1]

        print(f"{ticker}: Price=${current_price:.2f}, 5-day avg return={recent_return:.2f}%")
        if sma5 > sma20:
            print(f"  ↗️ Bullish: SMA5 (${sma5:.2f}) > SMA20 (${sma20:.2f})")
        else:
            print(f"  ↘️ Bearish: SMA5 (${sma5:.2f}) < SMA20 (${sma20:.2f})")
```

### 4. News Sentiment and Timeline Analysis

```python
# Load and analyze news data
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
news_files = [f for f in files if 'news_' in f]

if news_files:
    news_df = pd.read_csv(f'{CSV_PATH}/{news_files[-1]}')
    news_df['datetime'] = pd.to_datetime(news_df['datetime'])

    print("=== NEWS ANALYSIS ===")
    print(f"Total news articles: {len(news_df)}")

    # Daily news volume
    news_df['date'] = news_df['datetime'].dt.date
    daily_counts = news_df.groupby('date').size().sort_values(ascending=False)
    print(f"\nBusiest news days:")
    print(daily_counts.head())

    # Keyword analysis
    keywords = ['Fed', 'interest', 'earnings', 'AI', 'tech', 'market']
    print(f"\nKeyword frequency:")
    for keyword in keywords:
        count = news_df['topic'].str.contains(keyword, case=False, na=False).sum()
        print(f"  {keyword}: {count} mentions")

    # Recent headlines
    print(f"\nMost recent headlines:")
    recent_news = news_df.nlargest(5, 'datetime')
    for _, article in recent_news.iterrows():
        print(f"  {article['datetime'].strftime('%m/%d %H:%M')}: {article['topic'][:80]}...")
```

### 5. Saving and Loading Temporary Analysis Files

```python
# Create combined analysis and save as temp file for later use
files = [f for f in os.listdir(CSV_PATH) if f.endswith('.csv')]
metrics_files = [f for f in files if 'price_metrics_' in f]
ticker_files = [f for f in files if 'ticker_details_' in f]

if metrics_files and ticker_files:
    metrics_df = pd.read_csv(f'{CSV_PATH}/{metrics_files[-1]}')
    ticker_df = pd.read_csv(f'{CSV_PATH}/{ticker_files[-1]}')

    # Create comprehensive analysis
    analysis = pd.merge(metrics_df,
                       ticker_df[['ticker', 'name', 'market_cap', 'total_employees', 'sic_description']],
                       on='ticker', how='left')

    # Add calculated fields
    analysis['market_cap_billions'] = analysis['market_cap'] / 1e9
    analysis['risk_adj_return'] = analysis['total_return'] / analysis['volatility']
    analysis['return_pct'] = analysis['total_return'] * 100
    analysis['employees_per_billion_cap'] = analysis['total_employees'] / analysis['market_cap_billions']

    # Save temporary analysis file
    temp_file = f'{CSV_PATH}/temp_comprehensive_analysis.csv'
    analysis.to_csv(temp_file, index=False)
    print(f"✅ Saved comprehensive analysis to: temp_comprehensive_analysis.csv")
    print(f"   Shape: {analysis.shape}")
    print(f"   Columns: {list(analysis.columns)}")

    # Demonstrate reading it back in same session
    reloaded = pd.read_csv(temp_file)
    print(f"✅ Successfully reloaded: {reloaded.shape}")

    # Show summary
    print("\n=== COMPREHENSIVE ANALYSIS SUMMARY ===")
    summary = reloaded.nlargest(3, 'risk_adj_return')[['ticker', 'name', 'return_pct', 'market_cap_billions']]
    print(summary.round(2))
```

### 6. Error Handling and Robust File Loading

```python
# Robust file loading with error handling
def load_latest_csv(file_pattern):
    \"\"\"Load the most recent CSV file matching the pattern\"\"\"
    try:
        files = [f for f in os.listdir(CSV_PATH) if file_pattern in f and f.endswith('.csv')]
        if not files:
            print(f"❌ No files found matching pattern: {file_pattern}")
            return None

        latest_file = sorted(files)[-1]  # Get the last one alphabetically
        df = pd.read_csv(f'{CSV_PATH}/{latest_file}')
        print(f"✅ Loaded {latest_file}: {df.shape}")
        return df

    except Exception as e:
        print(f"❌ Error loading {file_pattern}: {e}")
        return None

# Use robust loading
price_data = load_latest_csv('price_data_')
ticker_details = load_latest_csv('ticker_details_')
price_metrics = load_latest_csv('price_metrics_')

# Only proceed if we have the required data
if price_metrics is not None:
    print("\n=== PORTFOLIO RISK ANALYSIS ===")

    # Volatility analysis
    high_vol = price_metrics[price_metrics['volatility'] > price_metrics['volatility'].mean()]
    low_vol = price_metrics[price_metrics['volatility'] <= price_metrics['volatility'].mean()]

    print(f"High volatility stocks ({len(high_vol)}):")
    for _, stock in high_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")

    print(f"\\nLow volatility stocks ({len(low_vol)}):")
    for _, stock in low_vol.iterrows():
        print(f"  {stock['ticker']}: {stock['volatility']*100:.1f}% volatility, {stock['total_return']*100:.1f}% return")
```

---

## Best Practices

1. **Always reload data** - Start each py_eval with file discovery and loading
2. **Use error handling** - Check if files exist before loading
3. **Save intermediate results** - Use temporary CSV files for complex multi-step analysis
4. **Keep analysis focused** - Each py_eval call should accomplish one analytical goal
5. **Print progress** - Use print statements to show what your code is doing
6. **Combine datasets** - Merge price_metrics with ticker_details for comprehensive analysis
"""
