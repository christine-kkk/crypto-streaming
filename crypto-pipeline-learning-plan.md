# Crypto Streaming Pipeline: Learning Plan

## Instructions for Claude (read this first)

- This is a **learning project**. **Do not write the project code for me.**
- Be my guide. Explain *what* to do next and *why*, point me to docs, ask me guiding questions, and review the code I write.
- Small illustrative snippets are OK only when I'm stuck on syntax and ask for them. Don't hand me full files or full solutions.
- Before starting a new phase, make sure I can explain the design decisions from the previous one.
- **Current status:** Phase 0 (not started yet). Update this line as I progress.

## Goal

Learn Kafka, Flink, Docker and Kubernetes by building a real-time crypto market analytics pipeline, and end with a project I can put on my resume.

## Architecture

```
Exchange WebSockets ──► Producer (Python) ──► Kafka ──► Flink ──► Postgres/ClickHouse ──► Grafana
   (raw trades)          (normalize)        (buffer)   (compute)     (store)               (see)
```

What each part does:
- **Producer:** connects to the exchange and pushes every event into Kafka as fast as it arrives. It does no calculations.
- **Kafka:** a durable buffer between the parts. If Flink crashes or is redeployed, no data is lost, because Flink picks up where it left off.
- **Flink:** does stateful work over time (candles, VWAP, joins, anomaly detection).
- **Storage and Grafana:** let me see and check the results.

Build order: a working laptop version first, then make it robust, then move it to Kubernetes. Don't start with Kubernetes.

## Roadmap

| Phase | What I build | What I learn |
|---|---|---|
| **0** | Look at the raw exchange feed by hand | Understanding the data before building anything |
| **1** | Kafka (KRaft mode) in Docker Compose plus a Python producer → `trades.raw` topic | Topics, partitions, keys, producer guarantees |
| **2** | First Flink job: 1-minute candles with Flink SQL | Windows, event time, watermarks |
| **3** | Write results to a database and build a Grafana dashboard | Sinks, delivery guarantees, end-to-end testing |
| **4** | Second exchange, price-gap join across exchanges, volume-spike alerts | Stream joins, keyed state, the DataStream API |
| **5** | Schema Registry (Avro), checkpoints, failure testing | Exactly-once processing, schema changes over time, recovery |
| **6** | Move to Kubernetes: kind or k3d, Strimzi (Kafka), Flink Kubernetes Operator, Helm, Prometheus | Operators, deployments, observability |
| **7** | GitHub Actions, README, architecture diagram | Making it presentable on a resume |

## Phase 0: Get to know the data

**Goal:** watch real trades arrive and understand their structure before writing any pipeline code.

1. Pick an exchange: **Coinbase** (Advanced Trade WebSocket, `market_trades` channel) or **Kraken**. Both have public market data with no API key. Binance blocks some regions, so check that it connects first if I want to use it.
2. Install websocat (`brew install websocat`), connect, and send the subscribe message from the exchange's docs. Subscribe to BTC-USD and ETH-USD.
3. Watch the feed for a few minutes and save sample messages to a file.

**Questions to answer in writing:**
1. Which fields does a trade have? Which ones do I actually need?
2. Each message has the exchange's timestamp, and I see it at some later time. Why might those differ, and which one should a 1-minute candle use?
3. Do messages ever arrive out of order or in batches?
4. What does the exchange send besides trades (heartbeats, subscription confirmations, errors)?
5. Roughly how many messages per second? (This will drive Kafka partition choices later.)
6. Is there a trade ID? What would I use it for? (Hint: what happens when the producer reconnects?)

**Done when:** I've answered all six and reviewed them with Claude, along with a sample message or two.

## Phase 1 preview

- Run a single-broker Kafka in **KRaft mode** with Docker Compose. Kafka 4.x no longer uses ZooKeeper, so skip any tutorial that sets it up.
- Write a Python producer with `confluent-kafka`: connect to the WebSocket, normalize each trade into **my own** JSON format, and send it to `trades.raw`.
- Decisions I need to be able to explain:
  - **Message key:** why key by symbol? What does that mean for ordering within a partition?
  - **`acks` and idempotence settings:** what can go wrong when the producer retries?
  - **My own format or the exchange's:** why not pass the exchange's raw format straight through?

## Reading

- Kafka docs: the "Introduction" page, and the "Design" section up to replication.
- *Kafka: The Definitive Guide*, first few chapters (Confluent offers it free).
- Later, for Phase 2: Flink docs on "Timely Stream Processing" (event time and watermarks).

## Notes and decisions log

<!-- Record answers, design decisions and lessons learned here as I go. -->
