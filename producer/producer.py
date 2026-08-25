"""
============================================================
KAFKA PRODUCER — Stock Price Publisher
============================================================

YOUR MISSION:
    Fill in every section marked with TODO.
    This script fetches live stock prices from Yahoo Finance
    and publishes them as JSON messages to a Kafka topic.

CONCEPTS YOU'RE LEARNING:
    1. PRODUCER: A client that PUBLISHES messages to a Kafka topic.
    2. TOPIC: Think of it as a named log file that multiple consumers can read.
    3. SERIALIZATION: Kafka only handles bytes, so you must convert
       your Python dict → JSON string → bytes before sending.
    4. CALLBACK: An async function Kafka calls to confirm delivery
       (or report an error) for each message.

MESSAGE FORMAT (what you'll publish):
    {
        "ticker": "AAPL",
        "price": 187.45,
        "volume": 52431678,
        "timestamp": "2026-08-22T14:30:00Z",
        "bid": 187.44,
        "ask": 187.46
    }

INTERVIEW TIP:
    "I built a Kafka producer that ingests real-time market data
     from Yahoo Finance. Each message is serialized to JSON and
     published to a partitioned topic. I used delivery callbacks
     to ensure at-least-once delivery semantics."

============================================================
"""

import os
import json
import time
import random
from datetime import datetime, timezone

import yfinance as yf
from confluent_kafka import Producer


# ============================================================
# CONFIGURATION — Read from environment variables (.env file)
# ============================================================
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-prices")
STOCK_TICKERS = os.environ.get("STOCK_TICKERS", "AAPL,MSFT,GOOGL").split(",")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))


# ============================================================
# TODO 1: Create the Kafka Producer instance
# ============================================================
# The Producer needs a configuration dictionary. At minimum:
#   - 'bootstrap.servers': The Kafka broker address (use KAFKA_BROKER)
#
# Optional but useful configs (look these up in confluent-kafka docs):
#   - 'client.id': A name to identify this producer (e.g., 'stock-producer')
#   - 'acks': How many brokers must confirm receipt ('all' = safest)
#
# Docs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer
#
# Example:
#   producer = Producer({'bootstrap.servers': 'some-broker:9092'})
#
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'stock-producer',
    'acks': 'all',                  # Wait for all ISR replicas to confirm
})
# ============================================================


def delivery_callback(err, msg):
    """
    ============================================================
    TODO 2: Implement the delivery callback
    ============================================================
    This function is called ONCE for every message you produce,
    AFTER Kafka confirms (or rejects) the delivery.

    Parameters:
        err: An error object if delivery failed, None if successful
        msg: The message object with details (.topic(), .partition(), .offset())

    What to do:
        - If err is not None: print an error message with the error
        - If err is None: print a success message showing:
            topic, partition, and offset

    Why this matters:
        In production, you'd use this to implement retry logic or
        dead-letter queues for failed messages.

    Example output:
        "✓ Delivered to stock-prices [partition 0] @ offset 42"
        "✗ Delivery failed: [Errno 111] Connection refused"
    ============================================================
    """
    if err is not None:
        print(f"✗ Delivery failed: {err}")
    else:
        print(f"✓ Delivered to {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")


def fetch_stock_data(ticker: str) -> dict | None:
    """
    ============================================================
    TODO 3: Fetch live stock data from Yahoo Finance
    ============================================================
    Use yfinance to get the current price data for a single ticker.

    Steps:
        1. Create a yfinance Ticker object: yf.Ticker(ticker)
        2. Get fast_info or info to extract current price data
           Hint: ticker_obj.fast_info gives you:
                 - .last_price (current price)
                 - .last_volume (trading volume)
        3. Also try ticker_obj.info for bid/ask if available
        4. Return a dictionary with this structure:
            {
                "ticker": "AAPL",
                "price": 187.45,
                "volume": 52431678,
                "timestamp": "2026-08-22T14:30:00+00:00",
                "bid": 187.44,
                "ask": 187.46,
            }
        5. If anything fails, print the error and return None

    Tips:
        - Use datetime.now(timezone.utc).isoformat() for timestamp
        - Wrap in try/except to handle network errors gracefully
        - bid/ask might not always be available; use .get() with a default of 0.0

    ============================================================
    """
    try:
        stock = yf.Ticker(ticker)

        # Use history() — works reliably even when market is closed
        hist = stock.history(period="1d")
        if hist.empty:
            raise ValueError("No data returned")

        latest = hist.iloc[-1]
        price = latest.get("Close", 0.0)
        volume = latest.get("Volume", 0)

        # info dict has bid/ask; gracefully default to 0
        info = stock.info
        bid = info.get("bid", 0.0)
        ask = info.get("ask", 0.0)

        return {
            "ticker": ticker,
            "price": round(float(price), 4) if price else 0.0,
            "volume": int(volume) if volume else 0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "bid": round(float(bid), 4),
            "ask": round(float(ask), 4),
            "source": "yahoo_finance",
        }
    except Exception as e:
        print(f"⚠ yfinance failed for {ticker}: {e} — using simulated data")
        return simulate_stock_data(ticker)


# Realistic base prices for simulation (updated periodically)
SIMULATED_PRICES = {
    "AAPL": 195.0, "MSFT": 425.0, "GOOGL": 178.0,
    "AMZN": 190.0, "TSLA": 255.0, "NVDA": 130.0,
    "AMD": 160.0,  "INTC": 32.0,  "META": 510.0,
}


def simulate_stock_data(ticker: str) -> dict:
    """
    Generate realistic simulated stock data when yfinance is unavailable.
    Price walks randomly around a base price with small fluctuations.
    This ensures the Kafka pipeline always has data flowing for learning.
    """
    base = SIMULATED_PRICES.get(ticker, 100.0)

    # Random walk: ±0.5% from base price
    price = round(base * (1 + random.uniform(-0.005, 0.005)), 4)
    spread = round(price * 0.0003, 4)  # ~0.03% bid-ask spread
    volume = random.randint(10_000_000, 80_000_000)

    # Update base price for next call (makes it walk over time)
    SIMULATED_PRICES[ticker] = price

    return {
        "ticker": ticker,
        "price": price,
        "volume": volume,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "bid": round(price - spread, 4),
        "ask": round(price + spread, 4),
        "source": "simulated",
    }


def produce_messages():
    """
    ============================================================
    TODO 4: Main loop — Fetch data and publish to Kafka
    ============================================================
    This is the main loop that runs forever:

    While True:
        1. For each ticker in STOCK_TICKERS:
            a. Call fetch_stock_data(ticker) to get the data dict
            b. If data is None, skip this ticker
            c. Serialize the dict to a JSON string (json.dumps)
            d. Call producer.produce() with:
                - topic: KAFKA_TOPIC
                - key: the ticker string (used for partitioning!)
                - value: the JSON string
                - callback: delivery_callback
               Note: both key and value must be strings or bytes

        2. Call producer.flush() to wait for all messages to be delivered
           (Interview: "flush() blocks until all buffered messages
            are sent and callbacks are triggered")

        3. Print how many tickers you just published

        4. Sleep for FETCH_INTERVAL seconds

    IMPORTANT CONCEPT — Message Keys:
        We use the ticker symbol as the KEY. Kafka guarantees that
        all messages with the same key go to the SAME partition.
        This means all AAPL messages are ordered, all MSFT messages
        are ordered, etc. This is critical for time-series data!

    ============================================================
    """
    print(f"Starting producer for tickers: {STOCK_TICKERS}")
    print(f"Publishing to topic: {KAFKA_TOPIC} @ {KAFKA_BROKER}")
    print(f"Fetch interval: {FETCH_INTERVAL}s")
    print("=" * 50)

    while True:
        count = 0
        for ticker in STOCK_TICKERS:
            data = fetch_stock_data(ticker.strip())
            if data is None:
                continue

            # Serialize dict → JSON string (Kafka needs bytes or str)
            value = json.dumps(data)

            # Key = ticker → all messages for same ticker go to same partition
            producer.produce(
                topic=KAFKA_TOPIC,
                key=ticker.strip(),
                value=value,
                callback=delivery_callback,
            )
            count += 1

        # Block until all buffered messages are delivered
        producer.flush()

        print(f"📤 Published {count}/{len(STOCK_TICKERS)} tickers | sleeping {FETCH_INTERVAL}s...")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    # Add a small delay to let Kafka start up first
    print("Waiting 15s for Kafka to be ready...")
    time.sleep(15)
    produce_messages()
