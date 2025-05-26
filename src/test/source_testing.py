import asyncio
import json
import websockets


async def fetch_one_trade(symbol: str):
    url = f"wss://stream.binance.com:9443/ws/{symbol}@trade"
    async with websockets.connect(url) as ws:
        raw_msg = await ws.recv()  # 1) get the raw JSON string
        print("RAW:", raw_msg)  # 2) inspect the payload
        data = json.loads(raw_msg)  # 3) parse into a Python dict
        print("PARSED:", data)  # 4) now you can extract fields


# Kick off the coroutine for BTCUSDT
loop = asyncio.get_event_loop()
loop.run_until_complete(fetch_one_trade("btcusdt"))
loop.close()
