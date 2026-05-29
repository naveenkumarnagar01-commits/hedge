import asyncio
import websockets
import json

async def test():
    uri = "wss://fstream.binance.com/stream?streams=btc-260515-80000-c@ticker/btc-260515-80000-c@depth10@100ms"
    try:
        async with websockets.connect(uri) as ws:
            print('Connected!')
            for i in range(3):
                msg = await ws.recv()
                print(f"Message {i}: {msg}")
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(test())
