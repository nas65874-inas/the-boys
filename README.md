# News Pulse

News Pulse is a PySpark and Streamlit project for monitoring live news headlines from RSS feeds. The pipeline pulls headlines, writes them as JSON-lines files, processes them with Spark Structured Streaming, and displays the results in a dashboard.

## Files

- `ingester.py`: pulls headlines from 4 RSS feeds and writes JSONL files into `data/incoming/`
- `streaming_job.py`: defines the Spark Structured Streaming queries using `readStream` and `writeStream`
- `app.py`: Streamlit dashboard with charts and keyword summary
- `llm_summary.py`: creates a short summary from the top keywords, with a fallback if no API key is available
- `reflection.md`: short scaling reflection





#OUTPUT
```csv
source,headline_count
Tech Daily,3
Finance Post,2
Vought News,2
Health Desk,1
Sports Now,1
```
