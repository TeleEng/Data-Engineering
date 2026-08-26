"""
============================================================
PYSPARK STRUCTURED STREAMING — Real-Time Aggregation
============================================================

YOUR MISSION:
    Fill in the TODOs to read the real-time Kafka stream,
    parse the JSON data, aggregate the average price per 
    ticker using a tumbling window, and write it to the console.

CONCEPTS YOU'RE LEARNING:
    1. STRUCTURED STREAMING: Treating a live stream like a growing table.
    2. KAFKA INTEGRATION: Reading/Writing from Kafka using Spark SQL.
    3. WINDOWING: Aggregating data over time chunks (e.g., every 1 min).
    4. WATERMARKING: Handling late-arriving data.

INTERVIEW TIP:
    "I built a PySpark Structured Streaming job that consumed JSON events 
    from Kafka. I applied a 1-minute tumbling window with watermarking 
    to calculate the moving average price per ticker, then output the 
    results for downstream analytics."

============================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType

# Configurations
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "stock-prices")

# ============================================================
# TODO 1: Initialize the SparkSession
# ============================================================
# Create a SparkSession named "StockPriceStreaming".
# We need to make sure it includes the Kafka package (this is 
# handled in the spark-submit command in docker-compose, but 
# you just need to initialize the session here).
#
# Example: 
# spark = SparkSession.builder.appName("MyApp").getOrCreate()
#
# YOUR CODE HERE:
# spark = ...
# ============================================================
spark = None # Replace this

# Prevent excessive INFO logging
if spark:
    spark.sparkContext.setLogLevel("WARN")

# ============================================================
# Define the Schema of our JSON Kafka Messages
# ============================================================
# We expect: {"ticker": "AAPL", "price": 187.45, "volume": 52M, "timestamp": "..."}
schema = StructType() \
    .add("ticker", StringType()) \
    .add("price", DoubleType()) \
    .add("volume", IntegerType()) \
    .add("timestamp", TimestampType()) \
    .add("bid", DoubleType()) \
    .add("ask", DoubleType()) \
    .add("source", StringType())

# ============================================================
# TODO 2: Read the Stream from Kafka
# ============================================================
# Use spark.readStream.format("kafka") to read from the topic.
# Options needed:
#   - "kafka.bootstrap.servers" -> KAFKA_BROKER
#   - "subscribe" -> KAFKA_TOPIC
#   - "startingOffsets" -> "earliest" (so we see past data)
#
# Example:
# raw_df = spark.readStream \
#    .format("kafka") \
#    .option("...", "...") \
#    .load()
#
# YOUR CODE HERE:
# raw_df = ...
# ============================================================
raw_df = None # Replace this

# ============================================================
# TODO 3: Parse the Kafka Value (Bytes -> JSON)
# ============================================================
# Kafka messages have a 'key' and 'value' as binary.
# 1. Cast 'value' to String.
# 2. Use the from_json() function with the schema defined above.
# 3. Select the nested fields so your DataFrame has columns:
#    ticker, price, timestamp, etc.
#
# Example:
# parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
#    .select(from_json(col("value"), schema).alias("data")) \
#    .select("data.*")
#
# YOUR CODE HERE:
# parsed_df = ...
# ============================================================
parsed_df = None # Replace this

# ============================================================
# TODO 4: Windowed Aggregation (Tumbling Window)
# ============================================================
# We want the Average Price per Ticker over a 1-minute window.
# 1. Use .withWatermark("timestamp", "1 minute") to handle late data.
# 2. Group by window("timestamp", "1 minute") AND "ticker".
# 3. Aggregate using avg("price").alias("avg_price").
#
# Example:
# agg_df = parsed_df \
#    .withWatermark("timestamp", "2 minutes") \
#    .groupBy(window("timestamp", "1 minute"), "ticker") \
#    .agg(avg("price").alias("avg_price"))
#
# YOUR CODE HERE:
# agg_df = ...
# ============================================================
agg_df = None # Replace this

# ============================================================
# TODO 5: Write the Stream out to the Console
# ============================================================
# For now, let's output the result to the console so we can see it.
# Use agg_df.writeStream.format("console")
# Options needed:
#   - "outputMode" -> "update" (only prints changed rows)
#   - "truncate" -> False (so we see the full timestamp)
#
# Don't forget to call .start() and .awaitTermination()
#
# YOUR CODE HERE:
# query = ...
# query.awaitTermination()
# ============================================================
