import time

import pandas as pd
import streamlit as st
from pyspark.sql import SparkSession

from llm_summary import make_summary
from streaming_job import build_streaming_queries


st.set_page_config(page_title="News Pulse", layout="wide")


@st.cache_resource
def start_spark_streams():
    spark = (
        SparkSession.builder.appName("NewsPulseDashboard")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    if not spark.streams.active:
        build_streaming_queries(spark)

    return spark


def table_or_empty(spark, query):
    try:
        return spark.sql(query).toPandas()
    except Exception:
        return pd.DataFrame()


spark = start_spark_streams()

st.title("News Pulse - Live")

by_source = table_or_empty(
    spark,
    "select source, count from by_source order by count desc",
)

by_window = table_or_empty(
    spark,
    "select window.start as hour, count from by_window order by hour",
)

top_words = table_or_empty(
    spark,
    "select word, count from top_words order by count desc limit 10",
)

left, right = st.columns(2)

with left:
    st.subheader("Source Mix")

    if by_source.empty:
        st.info("Waiting for incoming RSS files...")
    else:
        st.bar_chart(by_source.set_index("source"))

with right:
    st.subheader("Windowed Volume")

    if by_window.empty:
        st.info("Waiting for windowed data...")
    else:
        st.line_chart(by_window.set_index("hour"))

st.subheader("Top Keywords")

if top_words.empty:
    st.info("Waiting for keyword data...")
    keywords = []
else:
    st.dataframe(top_words, use_container_width=True)
    keywords = top_words["word"].astype(str).tolist()

st.subheader("LLM Summary")
st.write(make_summary(keywords))

st.caption("Dashboard refreshes every 10 seconds.")

time.sleep(10)
st.rerun()
