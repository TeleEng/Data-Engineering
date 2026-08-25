"""
============================================================
KAFKA CONSUMER — Stock Price Reader
============================================================

YOUR MISSION:
    Fill in every section marked with TODO.
    This script subscribes to the Kafka topic, reads stock price
    messages, and prints them to the console.

CONCEPTS YOU'RE LEARNING:
    1. CONSUMER: A client that SUBSCRIBES to topics and reads messages.
    2. CONSUMER GROUP: Multiple consumers with the same group.id
       SHARE the work — Kafka assigns different partitions to each.
       If you run 2 consumers in group "stock-readers", each gets
       half the partitions. This is how Kafka scales horizontally!
    3. OFFSET: Each message in a partition has a sequential offset.
       The consumer tracks "where am I?" so it can resume after restarts.
    4. DESERIALIZATION: Reverse of the producer — bytes → string → dict.
    5. POLLING: Unlike a queue where you get pushed messages,
       Kafka consumers PULL (poll) messages in batches.

CONSUMER GROUP VISUAL:
    Topic: stock-prices (3 partitions)
    
    Consumer Group: "stock-readers"
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │ Partition 0  │  │ Partition 1  │  │ Partition 2  │
    │  AAPL, TSLA  │  │  MSFT, META  │  │  GOOGL, AMZN │
    └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
           │                │                │
           ▼                ▼                ▼
    ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
    │  Consumer 1  │  │  Consumer 2  │  │  Consumer 3  │
    └─────────────┘  └─────────────┘  └─────────────┘
    
    If Consumer 2 dies, its partitions get REBALANCED to 1 and 3.

INTERVIEW TIP:
    "I implemented a Kafka consumer with consumer group semantics.
     If we need to scale read throughput, we simply add more
     consumers to the same group — Kafka automatically rebalances
     the partition assignments."

============================================================
"""

import os
import json
import time

from confluent_kafka import Consumer, KafkaError


# ============================================================
# CONFIGURATION
# ============================================================
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-prices")


# ============================================================
# TODO 1: Create the Kafka Consumer instance
# ============================================================
# The Consumer needs a configuration dictionary:
#
# Required:
#   - 'bootstrap.servers': Kafka broker address (use KAFKA_BROKER)
#   - 'group.id': Consumer group name (e.g., 'stock-readers')
#       All consumers with the same group.id SHARE partition work.
#       Use a DIFFERENT group.id if you want each consumer to
#       read ALL messages independently.
#
# Important:
#   - 'auto.offset.reset': What to do when there's no saved offset?
#       'earliest' = read from the very beginning of the topic
#       'latest'   = only read NEW messages from now on
#       For learning, use 'earliest' so you see all historical messages.
#
# Optional:
#   - 'enable.auto.commit': 'true' (default) = Kafka auto-saves
#       your read position. Set to 'false' for manual control.
#
# Docs: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#consumer
#
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'stock-readers',
    'auto.offset.reset': 'earliest',       # Read all historical messages
    'enable.auto.commit': True,
})
# ============================================================


def consume_messages():
    """
    ============================================================
    TODO 2: Subscribe to the topic and poll for messages
    ============================================================
    Steps:
        1. Subscribe to the topic:
           consumer.subscribe([KAFKA_TOPIC])

        2. Enter an infinite loop (while True):
            a. Poll for a message with a timeout:
               msg = consumer.poll(timeout=1.0)
               (timeout=1.0 means wait up to 1 second for a message)

            b. If msg is None: no message arrived, continue the loop

            c. If msg.error() is not None:
               - Check if it's just a partition EOF (not a real error):
                 if msg.error().code() == KafkaError._PARTITION_EOF:
                     # Just means we've read all available messages
                     # in this partition — print info and continue
                 else:
                     # Real error — print it and break
               
            d. If no error, you have a real message!
               - Decode the value: msg.value().decode('utf-8')
               - Parse JSON: json.loads(decoded_value)
               - Get the key: msg.key().decode('utf-8') if msg.key() else None
               - Print it nicely, including:
                   * The ticker (from key or from the message data)
                   * The price
                   * The partition: msg.partition()
                   * The offset: msg.offset()
               
               Example output:
               "📈 [Partition 0 | Offset 42] AAPL: $187.45 | Vol: 52.4M"

        3. In a finally block, call consumer.close()
           This commits offsets and leaves the consumer group cleanly.
           (Interview: "Failing to close() causes a rebalance delay
            because the group coordinator has to wait for a timeout
            before reassigning the dead consumer's partitions.")

    ============================================================
    """
    print(f"Starting consumer for topic: {KAFKA_TOPIC}")
    print(f"Connecting to broker: {KAFKA_BROKER}")
    print("=" * 50)

    consumer.subscribe([KAFKA_TOPIC])
    print(f"Subscribed to: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message within timeout — just keep polling
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — not an error, just informational
                    print(f"ℹ Reached end of {msg.topic()} [partition {msg.partition()}]")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic doesn't exist yet — producer hasn't sent first message
                    print(f"⏳ Topic '{KAFKA_TOPIC}' not ready yet, retrying...")
                    time.sleep(5)
                else:
                    # Real error — log but don't crash, keep trying
                    print(f"✗ Consumer error: {msg.error()}")
                    time.sleep(2)
                continue

            # Decode the message
            key = msg.key().decode("utf-8") if msg.key() else "N/A"
            value = json.loads(msg.value().decode("utf-8"))

            # Format volume for readability (e.g., 52431678 → 52.4M)
            vol = value.get("volume", 0)
            if vol >= 1_000_000:
                vol_str = f"{vol / 1_000_000:.1f}M"
            elif vol >= 1_000:
                vol_str = f"{vol / 1_000:.1f}K"
            else:
                vol_str = str(vol)

            print(
                f"📈 [Partition {msg.partition()} | Offset {msg.offset()}] "
                f"{key}: ${value.get('price', 0):.2f} | "
                f"Bid: ${value.get('bid', 0):.2f} | Ask: ${value.get('ask', 0):.2f} | "
                f"Vol: {vol_str}"
            )

    except KeyboardInterrupt:
        print("\n⏹ Consumer stopped by user.")
    finally:
        # Commits offsets and leaves the consumer group cleanly
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    # Add a delay to let Kafka and Producer start up first
    print("Waiting 30s for Kafka and Producer to be ready...")
    time.sleep(30)
    consume_messages()
