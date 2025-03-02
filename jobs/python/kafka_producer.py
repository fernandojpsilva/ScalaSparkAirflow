#!/usr/bin/env python3
# kafka_producer.py

import json
import time
import random
import requests
from kafka import KafkaProducer
from bs4 import BeautifulSoup
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('kafka_producer')

# Configure Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=3,
    max_in_flight_requests_per_connection=1
)

TOPIC_NAME = 'web-data-stream'

# Data sources to pull from
DATA_SOURCES = [
    # News API - replace YOUR_API_KEY with actual key
    {"url": "https://newsapi.org/v2/top-headlines?country=us&apiKey=YOUR_API_KEY", "type": "news_api"},

    # Reddit (public API)
    {"url": "https://www.reddit.com/r/technology/.json", "type": "reddit"},

    # Wikipedia recent changes
    {"url": "https://en.wikipedia.org/w/api.php?action=query&list=recentchanges&rcprop=title|user|comment&format=json", "type": "wikipedia"},

    # HN
    {"url": "https://hacker-news.firebaseio.com/v0/topstories.json", "type": "hackernews"}
]

# Public dataset fallback if API keys aren't available
PUBLIC_DATASETS = [
    "https://raw.githubusercontent.com/fivethirtyeight/data/master/airline-safety/airline-safety.csv",
    "https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv",
    "https://raw.githubusercontent.com/fivethirtyeight/data/master/bad-drivers/bad-drivers.csv"
]

def fetch_data_from_source(source):
    """Fetch data from a web source and return parsed content"""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}
        response = requests.get(source["url"], headers=headers, timeout=10)
        response.raise_for_status()

        if source["type"] == "news_api":
            data = response.json()
            return [{"source": "news_api", "title": article["title"], "content": article["description"], "timestamp": time.time()}
                    for article in data.get("articles", [])]

        elif source["type"] == "reddit":
            data = response.json()
            return [{"source": "reddit", "title": post["data"]["title"], "content": post["data"]["selftext"], "timestamp": time.time()}
                    for post in data.get("data", {}).get("children", [])]

        elif source["type"] == "wikipedia":
            data = response.json()
            return [{"source": "wikipedia", "title": change["title"], "content": change.get("comment", ""), "timestamp": time.time()}
                    for change in data.get("query", {}).get("recentchanges", [])]

        elif source["type"] == "hackernews":
            story_ids = response.json()[:20]  # Get top 20 stories
            stories = []
            for story_id in story_ids:
                story_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
                story_response = requests.get(story_url, headers=headers)
                if story_response.status_code == 200:
                    story = story_response.json()
                    stories.append({
                        "source": "hackernews",
                        "title": story.get("title", ""),
                        "content": story.get("text", ""),
                        "timestamp": time.time()
                    })
            return stories

        return []

    except Exception as e:
        logger.error(f"Error fetching data from {source['url']}: {e}")
        return []

def fetch_public_dataset(url):
    """Fetch public dataset as fallback"""
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Get CSV data and convert to list of dicts
        lines = response.text.strip().split('\n')
        headers = lines[0].split(',')

        records = []
        for line in lines[1:]:
            values = line.split(',')
            if len(values) == len(headers):
                record = {
                    "source": "public_dataset",
                    "dataset_url": url,
                    "content": dict(zip(headers, values)),
                    "timestamp": time.time()
                }
                records.append(record)

        return records

    except Exception as e:
        logger.error(f"Error fetching public dataset {url}: {e}")
        return []

def produce_messages(batch_size=50, interval=60):
    """Continuously produce messages to Kafka topic"""
    while True:
        all_data = []

        # Try API sources first
        for source in DATA_SOURCES:
            data = fetch_data_from_source(source)
            all_data.extend(data)

        # If we didn't get enough data, fetch from public datasets
        if len(all_data) < 10:
            logger.info("Not enough data from API sources, fetching from public datasets")
            for dataset_url in PUBLIC_DATASETS:
                data = fetch_public_dataset(dataset_url)
                all_data.extend(data)

        if all_data:
            batch_count = min(batch_size, len(all_data))
            batch = random.sample(all_data, batch_count) if len(all_data) > batch_count else all_data

            for message in batch:
                # Add some metadata
                message["producer_timestamp"] = time.time()

                # Send to Kafka
                producer.send(TOPIC_NAME, message)
                logger.info(f"Sent message: {message.get('title', '')[:30]}...")

            producer.flush()
            logger.info(f"Sent batch of {len(batch)} messages to topic {TOPIC_NAME}")
        else:
            logger.warning("No data retrieved from any source")

        # Wait before next batch
        logger.info(f"Waiting {interval} seconds before next batch")
        time.sleep(interval)

if __name__ == "__main__":
    try:
        logger.info(f"Starting kafka producer, sending to topic {TOPIC_NAME}")
        produce_messages()
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        producer.close()
        logger.info("Producer closed")