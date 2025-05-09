"""
run_deephaven_test_server.py
----------------------
This script launches a Deephaven server using the deephaven-server Python package, with configurable JVM arguments for memory and authentication.

This is a test server for use with the MCP community tools. It is not intended for production use.

Features:
- Starts the server on a user-specified host and port (defaults: localhost:10000)
- Allocates 8GB RAM to the JVM
- Disables authentication using AnonymousAuthenticationHandler
- Creates example tables for demonstration or testing, with three groups: simple, financial, or all
    - simple: Demo tables for general data (t1, t2, t3, people, products, time_series, mixed_demo)
    - financial: Market/finance tables (quotes, trades, ohlcv, news, reference_data)
    - all: Both simple and financial tables
- Requires the --table-group argument to select which group of tables to create
- Keeps the server running until interrupted (Ctrl+C)

Usage:
    uv run run_deephaven_test_server.py --table-group {simple|financial|all} [--host HOST] [--port PORT]

Arguments:
    --table-group {simple|financial|all}   REQUIRED. Which group of tables to create.
    --host HOST   Hostname or IP address to bind the server (default: localhost)
    --port PORT   Port number for the Deephaven server (default: 10000)

Requirements:
    - deephaven-server Python package installed
    - Java available in PATH
    - Sufficient memory for JVM
"""

import argparse
import datetime
import random
import time

from deephaven_server import Server

# Parse command-line arguments for host and port
parser = argparse.ArgumentParser(description="Launch a Deephaven server with configurable host and port and table group.")
parser.add_argument('--host', type=str, default='localhost', help='Hostname or IP address to bind the server (default: localhost)')
parser.add_argument('--port', type=int, default=10000, help='Port number for the Deephaven server (default: 10000)')
parser.add_argument('--table-group', type=str, required=True, choices=['simple', 'financial', 'all'], help='Which group of tables to create: simple, financial, or all (required)')
args = parser.parse_args()

host = args.host
port = args.port
table_group = args.table_group

# Set JVM args for 8GB RAM and disable authentication
jvm_args = [
    "-Xmx8g",  # Allocate 8GB heap memory
    "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"  # Disable authentication
]

print(f"Starting Deephaven server on {host}:{port} with 8GB RAM and no authentication ...")

# Initialize and start the Deephaven server
server = Server(
    host=host,
    port=port,
    jvm_args=jvm_args,
)
server.start()

# The server must be created before importing deephaven
from deephaven import agg
from deephaven import dtypes as dht
from deephaven import empty_table, new_table, time_table
from deephaven.column import bool_col, datetime_col, double_col, int_col, string_col

# Table creation by group
if table_group == 'simple' or table_group == 'all':
    print("Creating simple example tables...")
    # t1: Large static table with 3 columns
    t1 = empty_table(1000000).update(["C1 = i", "C2 = ii", "C3 = `abc`"])

    # t2: Derived from t1, adds a computed column C4
    t2 = t1.update(["C4 = C1 + 1000000"])

    # t3: Periodic time table with computed columns
    t3 = time_table("PT15m").update(["C1 = ii%2 == 0 ? `abc` : `def`", "C2 = i", "C3 = sqrt(C2)"])

    # Table of people with demographic info
    people = new_table([
        string_col("Name", ["Alice", "Bob", "Carol", "David"]),
        int_col("Age", [34, 28, 45, 23]),
        string_col("City", ["New York", "San Francisco", "Chicago", "Austin"]),
        string_col("Occupation", ["Engineer", "Designer", "Manager", "Analyst"]),
    ])

    # Table of products with price and stock
    products = new_table([
        string_col("ProductID", ["P001", "P002", "P003", "P004"]),
        string_col("ProductName", ["Widget", "Gadget", "Thingamajig", "Doodad"]),
        string_col("Category", ["Tools", "Electronics", "Toys", "Office"]),
        int_col("Price", [25, 99, 15, 5]),
        int_col("Stock", [100, 50, 200, 500]),
    ])

    # Simple time series table
    time_series = new_table([
        datetime_col("Timestamp", [datetime.datetime(2025, 5, 5, 14, 0) + datetime.timedelta(minutes=5*i) for i in range(4)]),
        double_col("Value", [10.1, 10.5, 9.8, 11.0]),
    ])

    # Table with mixed data types
    mixed_demo = new_table([
        string_col("Text", ["foo", "bar", "baz", "qux"]),
        int_col("IntVal", [1, 2, 3, 4]),
        double_col("FloatVal", [1.1, 2.2, 3.3, 4.4]),
        bool_col("Flag", [True, False, True, False]),
    ])

if table_group == 'financial' or table_group == 'all':
    print("Creating financial example tables...")
    SYMBOLS = dht.array(dht.string, ["AAPL", "AMZN", "GOOG", "MSFT"])

    # Single quotes table with all symbols
    quotes = (
        time_table("PT1s")
        .update([
            "Symbol = (String) SYMBOLS[i % 4]",
            "Bid = 100 + (random() * 10)",
            "Ask = Bid + (random() * 2)",
            "BidSize = (int) (100 + random() * 900)",
            "AskSize = (int) (100 + random() * 900)",
        ])
    )

    # Single trades table with all symbols
    trades = (
        time_table("PT2s")
        .update([
            "Symbol = (String) SYMBOLS[i % 4]",
            "Price = 100 + (random() * 12)",
            "Size = (int) (1 + random() * 999)",
            "Side = i % 2 == 0 ? `BUY` : `SELL`"
        ])
    )

    # OHLCV bar table (10s bars) using lower_bin and agg_by
    ohlcv = (
        trades
        .update(["Bar = lowerBin(Timestamp, 'PT10s')"])
        .agg_by([
            agg.first('Open = Price'),
            agg.max_('High = Price'),
            agg.min_('Low = Price'),
            agg.last('Close = Price'),
            agg.sum_('Volume = Size')
        ], by=["Symbol", "Bar"])
    )
    news_headlines = [
        "CEO steps down",
        "Product launch announced",
        "Dividend increased",
        "Regulatory investigation",
        "New partnership formed",
        "Quarterly results beat expectations",
        "Stock buyback program",
        "Major outage reported",
        "Expansion into new market",
        "Patent granted"
    ]
    news_types = ["NEWS", "DIVIDEND", "MERGER", "SPLIT", "GUIDANCE", "PRODUCT", "PARTNERSHIP", "REGULATORY"]

    def pick_random(arr) -> str:
        return arr[int(random.random() * len(arr))]

    news = (
        time_table("PT30s")
        .update([
            "Symbol = (String) SYMBOLS[i % 4]",
            f"ActionType = pick_random(news_types)",
            f"Headline = pick_random(news_headlines) + ` for ` + Symbol",
        ])
    )

    # Reference data table (static)
    reference_data = new_table(
        [
            string_col("Symbol", ["AAPL", "AMZN", "GOOG", "MSFT"]),
            string_col("Name", ["Apple Inc.", "Amazon.com Inc.", "Alphabet Inc.", "Microsoft Corp."]),
            string_col("Exchange", ["NASDAQ", "NASDAQ", "NASDAQ", "NASDAQ"]),
            string_col("Sector", ["Technology", "Consumer Discretionary", "Communication Services", "Technology"]),
            string_col("Currency", ["USD", "USD", "USD", "USD"]),
            int_col("LotSize", [100, 100, 100, 100]),
        ]
    )
else:
    raise ValueError(f"Invalid table group: {table_group}")

# Keep the server running until interrupted by user
print("Press Ctrl+C to exit")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Exiting Deephaven...")
    server.stop()
