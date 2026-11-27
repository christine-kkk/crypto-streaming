# Crypto Streaming Pipeline: Learning Plan

## Instructions for Claude (read this first)

- This is a **learning project**. **Do not write the project code for me.**
- Be my guide. Explain *what* to do next and *why*, point me to docs, ask me guiding questions, and review the code I write.
- Small illustrative snippets are OK only when I'm stuck on syntax and ask for them. Don't hand me full files or full solutions.
- Before starting a new phase, make sure I can explain the design decisions from the previous one.
- **Current status:** Phase 1 (starting). Phase 0 feed exploration done; written answers to the six questions skipped for now. Update this line as I progress.

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

## Phase 1: Kafka and a real producer

**Goal:** run Kafka on my laptop and turn `coinbase-trade-websocket.py` into a producer that sends every trade into the `trades.raw` topic.

**Before starting:** read the Kafka docs "Introduction" page (topics, partitions, offsets).

1. **Get Docker running.** Install Docker Desktop (or OrbStack, lighter on Mac). Check with `docker run hello-world`.
2. **Run Kafka in Docker Compose (KRaft mode, single broker).**
   - Use the official `apache/kafka` image and read its Docker Hub page. Kafka 4.x has no ZooKeeper, so skip any tutorial that sets it up.
   - Write `docker-compose.yml` myself. Understand `KAFKA_PROCESS_ROLES`, listeners and advertised listeners.
   - Question: why can a client inside the container connect, but one on my Mac can't? (The classic Docker + Kafka trap.)
   - Send the first draft to Claude for review.
3. **Use Kafka by hand before writing any Python** (CLI tools inside the container):
   - Create the `trades.raw` topic. How many partitions, and why?
   - Send a few messages with `kafka-console-producer.sh`.
   - Read them in another terminal with `kafka-console-consumer.sh --from-beginning`.
   - Read them again. Are they still there? What does that say about Kafka compared with a regular queue?
4. **Turn the script into a producer** (`pip install confluent-kafka`):
   - In `on_message`: skip non-trade messages (`subscriptions`, heartbeats), unpack each event's `trades` list, convert each trade to **my own** JSON format, and `produce()` it to `trades.raw` with a key I chose.
   - Decide what to do with `snapshot` events (see the Phase 0 notes below).
   - On shutdown: call `flush()` so buffered messages aren't lost.
   - Check it works by watching the console consumer while the producer runs.
5. **Decisions I need to be able to explain:**
   - **Message key:** why key by symbol (`product_id`)? What does that mean for ordering within a partition?
   - **`acks` and idempotence settings:** what can go wrong when the producer retries?
   - **My own format or the exchange's:** why not pass the exchange's raw format straight through?
   - **One Kafka message per trade:** why split in the producer?

**Done when:** trades from BTC-USD and ETH-USD show up in the console consumer one trade per message, the producer shuts down cleanly, and I can explain all the decisions in step 5.

## Reading

- Kafka docs: the "Introduction" page, and the "Design" section up to replication.
- *Kafka: The Definitive Guide*, first few chapters (Confluent offers it free).
- Later, for Phase 2: Flink docs on "Timely Stream Processing" (event time and watermarks).

## Notes and decisions log

<!-- Record answers, design decisions and lessons learned here as I go. -->

### Phase 0 (2026-11-24)

**What I built:** `coinbase-trade-websocket.py` subscribes to `market_trades` for BTC-USD and ETH-USD (no API key needed) and appends raw messages to `trade_messages.jsonl` (JSON Lines, one message per line, git-ignored).

**What the feed looks like:**
- Message → `events` (list) → `trades` (list). One message can hold several trades, newest first (descending `trade_id`).
- Two timestamps: the trade's `time` (when it happened) and the message's `timestamp` (when Coinbase sent it, about 100 ms later).
- `price` and `size` are strings, to avoid floating-point rounding.
- Non-trade messages: `channel: "subscriptions"` (subscription confirmation).
- The first event after subscribing has `type: "snapshot"`. After that, events are `update`.

**Checkpoint answers (reviewed with Claude):**
1. *Which timestamp for a 1-minute candle?* The trade's `time` (event time). ✅ The message `timestamp` and my receive time include network delay, so trades near a minute boundary would land in the wrong candle.
2. *One Kafka message per trade or per WebSocket message?* My first answer was "per WebSocket message, Kafka shouldn't do the splitting". **Corrected:** Kafka never does processing, it only stores messages. The **producer** does the splitting before sending, so send **one Kafka message per trade**. Each trade then gets its own key (`product_id`), and Flink and deduplication work on single trades.
3. *What's a snapshot and why does it matter on reconnect?* A snapshot is the batch of **recent trades** (history) Coinbase sends right after subscribing, before live `update` events. On reconnect, the new snapshot repeats trades I've already sent, so I get **duplicates**. Deduplicate using **`(product_id, trade_id)`**. `trade_id` alone isn't safe because IDs are per product.

**The six Phase 0 questions:**
1. *Which fields does a trade have? Which do I need?*
   `product_id`, `trade_id`, `price`, `size`, `time`, `side`. Candles need `product_id`, `price`, `size`, `time`. `trade_id` is for deduplication. `side` (BUY/SELL) is useful later for buy/sell volume.
2. *Exchange timestamp vs when I see it: why different, and which for a candle?*
   Network and processing delay (~100 ms in my sample, could be much more on a slow connection). Candles use the trade's `time` (event time). See checkpoint answer 1.
3. *Out of order or in batches?*
   **Batches: yes.** One message can hold several trades, and within a batch they're **newest first** (descending `trade_id`). **Out of order across messages:** not checked yet ⬜.
4. *What else does the exchange send?*
   `subscriptions` messages (subscription confirmation) and `snapshot` events (recent trade history after subscribing). **Heartbeats:** check the docs for the `heartbeats` channel and whether Coinbase closes idle connections ⬜.
5. *Roughly how many messages per second?*
   Not measured yet ⬜. Count trades (not lines) in `update` events over a 30s run.
6. *Is there a trade ID? What would I use it for?*
   Yes, `trade_id`, numbered per product. Use `(product_id, trade_id)` to remove duplicates after a reconnect (the new snapshot repeats trades). Confirm with the duplicate experiment ⬜.

**Still to do for Phase 0:**
- [ ] Duplicate experiment: `rm trade_messages.jsonl`, run the script twice (30s each), count repeated `(product_id, trade_id)` pairs (question 6).
- [ ] Measure trades per second per product (question 5).
- [ ] Check whether `trade_id`s are ever out of order *across* messages (question 3).
- [ ] Read the docs on the `heartbeats` channel (question 4).
- [ ] Tidy the script (remove the Q&A comments at the bottom, fix the "BTC-USD only" print, write the raw `message`) and commit it.
- [ ] Confirm the old Coinbase API key is revoked.

**Lessons:** a pip package name isn't always the import name (`jwt`/`websocket` imposters). Never hard-code secrets. python.org Python on macOS needs `Install Certificates.command`. Callbacks: `run_forever()` is the loop, and `on_message` handles one message per call.
