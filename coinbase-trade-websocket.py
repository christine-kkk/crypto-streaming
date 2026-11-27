import websocket
import json
import threading

# Market Data WebSocket URL
WS_URL = "wss://advanced-trade-ws.coinbase.com"

def on_open(ws):
    # Subscribe to the market trades channel for BTC-USD
    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD", "ETH-USD"],
        "channel": "market_trades"
    }
    ws.send(json.dumps(subscribe_message))
    print("Subscribed to BTC-USD market trades channel")

def on_message(ws, message):
    data = json.loads(message)
    with open("trade_messages.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    print(f"Received message {data}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, status_cd, status_msg):
    print(f"Connection closed with status_cd = {status_cd}: {status_msg}")

# Create the WebSocket connection
ws = websocket.WebSocketApp(
    WS_URL,
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

threading.Timer(30, ws.close).start()
ws.run_forever()

# Phase 0 questions and answers
# Which timestamp should a 1-minute candle use: the trade's time or the message's timestamp?
    # Trade's time
# One message can hold several trades. Should the producer send one Kafka message per trade, or one per WebSocket message?
    # 1 per websocket message, we should not be making kafka do the text cleaning and splitting right?
# You reconnect and Coinbase sends a snapshot. What problem does that create, and which field(s) help you deal with it?
    # idk what snapshot is tbh 