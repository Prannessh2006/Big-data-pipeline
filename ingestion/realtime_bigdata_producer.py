import asyncio
import aiohttp
import time
import random
import re
import orjson

from aiokafka import AIOKafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "ai-events"


# -----------------------------
# DATA SOURCES
# -----------------------------

NEWS_SOURCES = {
    "bbc": "https://feeds.bbci.co.uk/news/rss.xml",
    "cnn": "http://rss.cnn.com/rss/edition.rss",
    "guardian": "https://www.theguardian.com/world/rss",
    "reuters": "https://www.reutersagency.com/feed/?best-topics=technology",
    "hackernews": "https://hnrss.org/frontpage",
    "techcrunch": "https://techcrunch.com/feed/",
    "wired": "https://www.wired.com/feed/rss"
}

GITHUB_TRENDING = "https://github.com/trending"

STACKOVERFLOW = "https://stackoverflow.com/questions"


# -----------------------------
# PIPELINE CONFIG
# -----------------------------

QUEUE_SIZE = 100000
NEWS_WORKERS = 12
SCRAPER_WORKERS = 4
SOCIAL_GENERATORS = 8
BATCH_SIZE = 1000


event_queue = asyncio.Queue(maxsize=QUEUE_SIZE)


# -----------------------------
# RSS PARSER
# -----------------------------

def parse_rss(xml, source):

    items = re.findall(r"<item>(.*?)</item>", xml, re.DOTALL)

    events = []

    for item in items[:30]:

        title = re.search(r"<title>(.*?)</title>", item)
        link = re.search(r"<link>(.*?)</link>", item)

        if title and link:

            events.append({
                "source": source,
                "title": title.group(1),
                "link": link.group(1),
                "timestamp": int(time.time()*1000)
            })

    return events


# -----------------------------
# NEWS SCRAPER
# -----------------------------

async def fetch_news(session, name, url):

    try:

        async with session.get(url) as r:

            text = await r.text()

            events = parse_rss(text, name)

            for e in events:
                await event_queue.put(e)

    except:
        pass


async def news_worker(session):

    while True:

        tasks = []

        for name, url in NEWS_SOURCES.items():
            tasks.append(fetch_news(session, name, url))

        await asyncio.gather(*tasks)

        await asyncio.sleep(2)


# -----------------------------
# GITHUB TRENDING SCRAPER
# -----------------------------

async def github_scraper(session):

    while True:

        try:

            async with session.get(GITHUB_TRENDING) as r:

                text = await r.text()

                repos = re.findall(
                    r'href="/(.*?)"',
                    text
                )

                for repo in repos[:20]:

                    event = {
                        "source": "github_trending",
                        "repo": repo,
                        "timestamp": int(time.time()*1000)
                    }

                    await event_queue.put(event)

        except:
            pass

        await asyncio.sleep(5)


# -----------------------------
# STACKOVERFLOW SCRAPER
# -----------------------------

async def stackoverflow_scraper(session):

    while True:

        try:

            async with session.get(STACKOVERFLOW) as r:

                text = await r.text()

                titles = re.findall(
                    r'class="s-link">(.*?)</a>',
                    text
                )

                for t in titles[:20]:

                    event = {
                        "source": "stackoverflow",
                        "question": t,
                        "timestamp": int(time.time()*1000)
                    }

                    await event_queue.put(event)

        except:
            pass

        await asyncio.sleep(5)


# -----------------------------
# SOCIAL STREAM SIMULATOR
# -----------------------------

texts = [
    "AI model released",
    "New startup funding round",
    "Tech layoffs discussion",
    "Machine learning breakthrough",
    "Cybersecurity vulnerability discovered",
    "Developers debating framework",
    "Cloud infrastructure scaling",
    "Data engineering pipeline discussion"
]


async def social_stream():

    while True:

        events = []

        for _ in range(300):

            events.append({
                "source": "social_stream",
                "text": random.choice(texts),
                "likes": random.randint(0, 10000),
                "shares": random.randint(0, 3000),
                "timestamp": int(time.time()*1000)
            })

        for e in events:
            await event_queue.put(e)

        await asyncio.sleep(0.4)


# -----------------------------
# KAFKA BATCH SENDER
# -----------------------------

async def kafka_sender(producer):

    while True:

        tasks = []

        for _ in range(BATCH_SIZE):

            event = await event_queue.get()

            tasks.append(
                producer.send(
                    TOPIC,
                    orjson.dumps(event)
                )
            )

        await asyncio.gather(*tasks)


# -----------------------------
# MAIN
# -----------------------------

async def main():

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: v,
        compression_type="gzip",
        linger_ms=5
    )

    await producer.start()

    connector = aiohttp.TCPConnector(limit=500)

    async with aiohttp.ClientSession(connector=connector) as session:

        workers = []

        for _ in range(NEWS_WORKERS):
            workers.append(news_worker(session))

        for _ in range(SCRAPER_WORKERS):
            workers.append(github_scraper(session))
            workers.append(stackoverflow_scraper(session))

        for _ in range(SOCIAL_GENERATORS):
            workers.append(social_stream())

        workers.append(kafka_sender(producer))

        await asyncio.gather(*workers)

    await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())