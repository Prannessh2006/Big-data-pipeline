import os
import asyncio
import aiohttp
import json
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer

load_dotenv()

TARGETS = {
    "reddit_ai": "https://www.reddit.com/r/MachineLearning/hot.json?limit=10",
    "hn_ai": "https://hn.algolia.com/api/v1/search?query=AI&tags=story",
    "github_trending": "https://api.github.com/search/repositories?q=topic:machine-learning&sort=stars&order=desc"
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

async def fetch_and_produce(session, source_name, url, producer):
    headers = {'User-Agent': 'Mozilla/5.0'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            payload = {
                "source": source_name,
                "raw_data": data,
                "ingested_at": asyncio.get_event_loop().time()
            }
            await producer.send_and_wait("ai_social_trends", payload)
            print(f"Captured {source_name} AI trends")

async def run_producer():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    await producer.start()
    
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_and_produce(session, name, url, producer) for name, url in TARGETS.items()]
            await asyncio.gather(*tasks)
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(run_producer())