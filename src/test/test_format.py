import asyncio
import json
import websockets

# 1) Pick your symbol here:
symbol = "btcusdt"

# 2) Build the WebSocket URL directly:
ws_url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"


async def test_stream():
    # 3) Connect to Binance
    async with websockets.connect(ws_url) as ws:
        print(f"✅ Connected to {ws_url}\n")

        # 4) Receive & print 5 messages, then exit
        for i in range(5):
            raw_msg = await ws.recv()  # wait for the next trade JSON
            print(f"[RAW #{i + 1}]\n{raw_msg}\n")

            parsed = json.loads(raw_msg)  # parse into a Python dict
            print(f"[PARSED #{i + 1}]\n{parsed}\n")

        print("🔔 Done receiving 5 messages. Closing connection.")


# 5) Kick off the coroutine
loop = asyncio.get_event_loop()
loop.run_until_complete(test_stream())
loop.close()
