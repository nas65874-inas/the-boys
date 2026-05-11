import json
import os
import time
from datetime import datetime

import feedparser


INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "BBC": "https://feeds.bbci.co.uk/news/rss.xml",
    "Reuters": "https://feeds.reuters.com/reuters/topNews",
    "NPR": "https://feeds.npr.org/1001/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
}


def now_text():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def fallback_rows():
    return [
        {
            "source": "Tech Daily",
            "title": "AI chip demand rises as cloud companies expand",
            "url": "https://example.com/tech-ai",
            "ts": now_text(),
        },
        {
            "source": "Vought News",
            "title": "Homelander took the V1",
            "url": "https://example.com/vought-v1",
            "ts": now_text(),
        },
        {
            "source": "Sports Now",
            "title": "Barcelona wins the Champions League final",
            "url": "https://example.com/barcelona-final",
            "ts": now_text(),
        },
        {
            "source": "Finance Post",
            "title": "Oil and shipping costs pressure global markets",
            "url": "https://example.com/oil-markets",
            "ts": now_text(),
        },
    ]


def pull_once(tick):
    rows = []

    for source, url in FEEDS.items():
        try:
            feed = feedparser.parse(url)

            for entry in feed.entries[:8]:
                rows.append(
                    {
                        "source": source,
                        "title": entry.get("title", ""),
                        "url": entry.get("link", ""),
                        "ts": now_text(),
                    }
                )
        except Exception as error:
            print(f"Feed failed for {source}: {error}")

    if not rows:
        rows = fallback_rows()

    path = os.path.join(INCOMING, f"batch_{tick}.json")

    with open(path, "w", encoding="utf-8") as file:
        for row in rows:
            file.write(json.dumps(row) + "\n")

    print(f"Wrote {len(rows)} records to {path}")


if __name__ == "__main__":
    tick = 0

    while True:
        pull_once(tick)
        tick += 1
        time.sleep(60)
