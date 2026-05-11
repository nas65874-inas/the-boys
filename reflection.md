# Reflection

The first part that would break at 1000x input is the keyword aggregation, because every headline is split into many words and Spark has to keep growing state for repeated word counts. The RSS ingester is also simple, but the bigger Spark pressure would come from stateful aggregations and memory-sink tables. To fix this, I would use event-time watermarks, checkpointing, and a stronger output sink such as Parquet, Delta, or Kafka instead of keeping all results only in memory.
