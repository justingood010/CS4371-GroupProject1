import asyncio
import json
import websockets
from confluent_kafka import Producer

# 1. Kafka Configuration
config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "bsky-producer",
}

producer = Producer(config)

URI = (
    "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
    "?collections=app.bsky.feed.post&kinds=commit&langs=en"
)

# 2. Delivery Callback
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

async def listen():
    print("Connecting to Bluesky Jetstream...")
    while True:
        try:
            async with websockets.connect(URI, subprotocols=["xrpc.v1.json"]) as ws:
                print("Connected! Listening for posts...")
                async for frame in ws:
                    try:
                        # Parse the frame
                        data = json.loads(frame)
                        event = data.get("payload", {})
                        
                        # Check if it's a commit and not a delete
                        if event.get("operation") != "delete":
                            record = event.get("record", {})
                            text = record.get("text")
                            
                            # Only produce if there is actual text
                            if text:
                                # Produce to Kafka with a callback to catch errors
                                producer.produce("bsky-posts", value=text, callback=delivery_report)

                                # Trigger callbacks and keep the queue moving
                                producer.poll(0) 
                                
                        # ASYNC sleep to prevent blocking the websocket
                        await asyncio.sleep(0.1) 

                    except json.JSONDecodeError:
                        continue # Skip malformed frames
                    except Exception as e:
                        print(f"Error processing frame: {e}")

        except websockets.exceptions.ConnectionClosedError as e:
            print(f"Connection dropped: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)      

if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("\nStopping script...")
    finally:
        print("Flushing remaining messages to Kafka...")
        remaining = producer.flush(timeout=10)
        print(f"Messages remaining in queue: {remaining}")