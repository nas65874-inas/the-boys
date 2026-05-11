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

def clean_word(word):
    # Keep only letters and numbers so "AI," and "AI" count as the same word.
    return re.sub(r"[^a-z0-9]", "", word.lower())


def add_clean_headlines(news_rows):
    cleaned_rows = []

    for row in news_rows:
        words = row["headline"].split()
        useful_words = []

        for word in words:
            word = clean_word(word)
            if word and word not in STOP_WORDS:
                useful_words.append(word)

        cleaned_rows.append(
            {
                "source": row["source"],
                "headline": row["headline"],
                "published_at": row["published_at"],
                "clean_headline": " ".join(useful_words),
            }
        )

    return cleaned_rows

def show_batch_results(batch_number, batch_df, all_df):
    print(f"\n--- Batch {batch_number} incoming headlines ---")
    batch_df.select("published_at", "source", "headline").show(truncate=False)

    print("Headlines by source so far")
    source_counts = all_df.groupBy("source").agg(count("*").alias("headline_count"))
    source_counts = source_counts.orderBy(desc("headline_count"), "source")
    source_counts.show(truncate=False)

    print("Trending keywords so far")
    words = all_df.select(explode(split(lower(col("clean_headline")), " ")).alias("word"))
    words = words.where(col("word") != "")
    word_counts = words.groupBy("word").agg(count("*").alias("mentions"))
    word_counts = word_counts.orderBy(desc("mentions"), "word")
    word_counts.show(10, truncate=False)

    print("Two-minute window counts")
    window_counts = all_df.groupBy(
        window(col("published_at"), "2 minutes"), col("source")
    ).agg(count("*").alias("headlines"))
    window_counts.orderBy("window", "source").show(truncate=False)

def run_pipeline():
    spark = SparkSession.builder.appName("NewsPulse").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    news = make_mock_news()
    news = add_clean_headlines(news)

    # Each list is treated like a small batch arriving later.
    batches = [
        news[0:3],
        news[3:6],
        news[6:9],
    ]

    all_news_df = None

    for batch_number, batch in enumerate(batches, 1):
        batch_df = spark.createDataFrame(batch)

        if all_news_df is None:
            all_news_df = batch_df
        else:
            all_news_df = all_news_df.unionByName(batch_df)

        show_batch_results(batch_number, batch_df, all_news_df)

        # Small pause to make the script feel like micro-batches are arriving.
        time.sleep(1)

    output_path = "output/news_pulse_source_counts"
    final_counts = all_news_df.groupBy("source").agg(count("*").alias("headline_count"))
    final_counts = final_counts.orderBy(desc("headline_count"), "source")

    final_counts.coalesce(1).write.mode("overwrite").option("header", True).csv(
        output_path
    )

    print(f"\nSaved final source counts to {output_path}")
    spark.stop()


if __name__ == "__main__":
    run_pipeline()
