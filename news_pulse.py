import re
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, explode, lower, split, window


STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "is",
    "new",
    "of",
    "on",
    "the",
    "to",
    "with",
}
def make_mock_news():
    # This replaces a real API or RSS feed for the challenge.
    base_time = datetime.now().replace(second=0, microsecond=0)

    return [
        {
            "source": "Tech Daily",
            "headline": "AI chip demand rises as cloud companies expand",
            "published_at": base_time,
        },
        {
            "source": "Vought News",
            "headline": "Homelander took the V1",
            "published_at": base_time + timedelta(seconds=20),
        },
        {
            "source": "Tech Daily",
            "headline": "Cloud security startup raises fresh funding",
            "published_at": base_time + timedelta(seconds=50),
        },
        {
            "source": "Sports Now",
            "headline": "Barcelona wins the Champions League final",
            "published_at": base_time + timedelta(minutes=1, seconds=10),
        },
        {
            "source": "Vought News",
            "headline": "Energy ministers discuss oil supply targets",
            "published_at": base_time + timedelta(minutes=1, seconds=30),
        },
        {
            "source": "Finance Post",
            "headline": "Investors move toward cloud and AI stocks",
            "published_at": base_time + timedelta(minutes=2),
        },
        {
            "source": "Health Desk",
            "headline": "Hospitals test AI tools for faster patient notes",
            "published_at": base_time + timedelta(minutes=2, seconds=20),
        },
        {
            "source": "Finance Post",
            "headline": "Oil and shipping costs pressure global markets",
            "published_at": base_time + timedelta(minutes=3),
        },
        {
            "source": "Tech Daily",
            "headline": "Security teams prepare for cloud data risks",
            "published_at": base_time + timedelta(minutes=3, seconds=20),
        },
    ]
  
