import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


STOP_WORDS = [
    "a", "an", "and", "are", "as", "at", "by", "for", "from",
    "in", "is", "new", "of", "on", "the", "to", "with",
]


def build_streaming_queries(spark):
    os.makedirs("data/incoming", exist_ok=True)

    schema = StructType(
        [
            StructField("source", StringType(), True),
            StructField("title", StringType(), True),
            StructField("url", StringType(), True),
            StructField("ts", TimestampType(), True),
        ]
    )

    stream = spark.readStream.schema(schema).json("data/incoming")

    by_source = stream.groupBy("source").count()

    q_source = (
        by_source.writeStream.outputMode("complete")
        .format("memory")
        .queryName("by_source")
        .start()
    )

    by_window = (
        stream.withWatermark("ts", "2 hours")
        .groupBy(F.window("ts", "1 hour"))
        .count()
    )

    q_window = (
        by_window.writeStream.outputMode("complete")
        .format("memory")
        .queryName("by_window")
        .start()
    )

    words = stream.select(
        F.explode(
            F.split(
                F.regexp_replace(F.lower(F.col("title")), "[^a-z0-9]+", " "),
                " ",
            )
        ).alias("word")
    )

    words = words.where(~F.col("word").isin(STOP_WORDS))
    words = words.where(F.length("word") > 2)

    top_words = words.groupBy("word").count()

    q_words = (
        top_words.writeStream.outputMode("complete")
        .format("memory")
        .queryName("top_words")
        .start()
    )

    return [q_source, q_window, q_words]


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("NewsPulseStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    build_streaming_queries(spark)

    print("Streaming job is running. Keep this terminal open.")
    spark.streams.awaitAnyTermination()
