# Real-Time Financial Data Engineering Platform

A complete end-to-end data engineering pipeline demonstrating real-time market data ingestion, stream processing, and data warehousing. Built to showcase modern distributed system concepts.

## 🏗️ Architecture

Currently, the project is in **Phase 1**, focusing on robust event streaming infrastructure using Apache Kafka.

![Architecture Status](https://img.shields.io/badge/Phase-1_Kafka_Streaming-blue)
![Docker](https://img.shields.io/badge/Docker-Enabled-success)

### Core Components
* **Zookeeper**: Coordinates the Kafka brokers.
* **Apache Kafka**: The core message broker handling high-throughput, fault-tolerant event streams.
* **Producer (Python)**: Fetches live stock data (or generates realistic simulated data as a fallback) and publishes JSON events to Kafka. Keys messages by stock ticker to guarantee partition ordering.
* **Consumer (Python)**: Subscribes to the Kafka topic, handles partition assignments, decodes JSON payloads, and gracefully manages offsets and errors.
* **Kafka UI**: Web-based interface for monitoring topics, consumers, and cluster health.

---

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose
* Git

### Run the Stack
Start the entire infrastructure locally with a single command:

```bash
docker-compose up -d --build
```

### Accessing the Web UIs
Once the stack is running, you can monitor the system using these interfaces:

| Service | URL | Description |
|---------|-----|-------------|
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | View topics, inspect live messages, and check consumer group lag. |
| **Portainer** | `http://localhost:9000` (or `9443`) | Monitor Docker container health and logs. |

---

## 🔬 Phase 1 Features & Concepts Demonstrated

* **Data Partitioning**: Messages are keyed by ticker symbol (e.g., `AAPL`), guaranteeing that all updates for a specific stock are processed in exact time order within the same Kafka partition.
* **Resiliency & Fallbacks**: The producer attempts to fetch real data via `yfinance`. If the network is restricted or markets are closed, it automatically falls back to a realistic random-walk price simulator so the pipeline never breaks.
* **Consumer Groups**: The consumer is part of the `stock-readers` group, enabling horizontal scalability. Adding more consumers to the group automatically rebalances the partitions among them.
* **At-Least-Once Delivery**: The producer is configured with `acks='all'`, ensuring all in-sync replicas acknowledge the message before it is considered published.
* **Graceful Degradation**: The consumer handles `UNKNOWN_TOPIC_OR_PART` errors by retrying instead of crashing, allowing it to survive situations where it boots up faster than the producer can create the topic.

---

## 🗺️ Project Roadmap

- [x] **Phase 1**: Kafka Streaming Infrastructure (Producer/Consumer)
- [ ] **Phase 2**: Stream Processing with PySpark Structured Streaming
- [ ] **Phase 3**: Data Warehousing (Dimensional Modeling)
- [ ] **Phase 4**: Workflow Orchestration with Apache Airflow
- [ ] **Phase 5**: Analytics Serving API (FastAPI)

---

## 📝 Troubleshooting

If you don't see data flowing:
1. Check the producer logs: `docker logs stock-producer`
2. Check the consumer logs: `docker logs stock-consumer`
3. Verify that the Kafka UI shows the `stock-prices` topic under the "Topics" menu.
