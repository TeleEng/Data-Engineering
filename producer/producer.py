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
# YOUR CODE HERE:
# producer = Producer({ ... })
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
    # YOUR CODE HERE:
    pass


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
    # YOUR CODE HERE:
    pass


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

    # YOUR CODE HERE:
    pass


if __name__ == "__main__":
    # Add a small delay to let Kafka start up first
    print("Waiting 15s for Kafka to be ready...")
    time.sleep(15)
    produce_messages()
