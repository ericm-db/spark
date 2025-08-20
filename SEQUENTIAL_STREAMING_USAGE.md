# Sequential Streaming Source - Usage Guide

## Overview

The Sequential Streaming Source enables processing multiple streaming sources in a defined order, with automatic transition when bounded sources complete. This is particularly useful for processing historical data followed by live data streams.

## Basic Usage

### Simple Example: Historical Delta → Live Kafka

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("SequentialStreamingExample")
  .master("local[*]")
  .getOrCreate()

val df = spark.readStream
  .format("delta")
    .name("historical-delta")
    .option("path", "/path/to/historical/events")
    .option("readChangeFeed", "true")
    .option("startingVersion", "0")
    .option("endOffsets", "latest") // Process all historical data first
  .format("kafka")
    .name("live-kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .option("startingOffsets", "latest") // Start from current for live data
  .orderBy("historical-delta", "live-kafka") // Define execution sequence
  .load()

// Apply transformations
val processedData = df
  .dropDuplicatesWithinWatermark(Seq("eventId"), expr("timestamp"))
  .groupBy($"userId")
  .agg(sum($"amount").as("total"))

// Start the streaming query
val query = processedData.writeStream
  .outputMode("update")
  .format("delta")
  .option("path", "/path/to/output")
  .option("checkpointLocation", "/checkpoints/hybrid-query")
  .start()

query.awaitTermination()
```

### Multiple File Sources Example

```scala
val df = spark.readStream
  .format("parquet")
    .name("historical-parquet")
    .option("path", "/data/historical/parquet/")
    .option("maxFilesPerTrigger", "10")
  .format("json")
    .name("recent-json")
    .option("path", "/data/recent/json/")
    .option("maxFilesPerTrigger", "5")
  .format("csv")
    .name("live-csv")
    .option("path", "/data/live/csv/")
    .option("maxFilesPerTrigger", "1")
  .orderBy("historical-parquet", "recent-json", "live-csv")
  .load()
```

## Advanced Configuration

### Source-Specific Options

Each source maintains its own configuration independently:

```scala
val df = spark.readStream
  .format("delta")
    .name("batch-1")
    .option("path", "/batch1")
    .option("readChangeFeed", "true")
    .option("startingVersion", "0")
    .option("endingVersion", "100") // Process versions 0-100
  .format("delta")
    .name("batch-2")
    .option("path", "/batch2")
    .option("readChangeFeed", "true")
    .option("startingVersion", "101")
    .option("endingVersion", "200") // Process versions 101-200
  .format("kafka")
    .name("live-stream")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "topic")
    .option("startingOffsets", "latest")
  .orderBy("batch-1", "batch-2", "live-stream")
  .load()
```

### Error Handling

```scala
import org.apache.spark.sql.streaming.StreamingQueryException

val query = df.writeStream
  .outputMode("append")
  .format("console")
  .option("checkpointLocation", "/checkpoints/error-handling")
  .start()

try {
  query.awaitTermination()
} catch {
  case e: StreamingQueryException =>
    println(s"Query failed: ${e.getMessage}")
    // Handle source transition errors, missing dependencies, etc.
}
```

## Source Completion Behavior

### Bounded Sources (Automatic Completion)
- **Delta with endOffsets**: Completes when all specified data is processed
- **File sources with finite data**: Complete when no new files arrive
- **Rate sources with maxRecords**: Complete when record limit reached

### Unbounded Sources (Never Complete)
- **Kafka**: Continues indefinitely
- **Socket sources**: Run until manually stopped
- **Rate sources**: Generate data continuously

### Completion Detection Logic

The sequential source uses the following logic to detect completion:

1. **Offset Stability**: If a source returns the same offset 3+ times consecutively
2. **Configuration Hints**: Sources with `endOffsets` are considered bounded
3. **Error Conditions**: Connection failures may trigger transitions (configurable)

## Monitoring and Debugging

### Query Progress

```scala
val query = df.writeStream.start()

// Monitor progress
query.recentProgress.foreach { progress =>
  println(s"Current source: ${progress.sources(0).description}")
  println(s"Input rows: ${progress.inputRowsPerSecond}")
}
```

### Logging Configuration

Add to `log4j.properties`:
```properties
log4j.logger.org.apache.spark.sql.execution.streaming.sources.SequentialMicroBatchStream=DEBUG
log4j.logger.org.apache.spark.sql.execution.streaming.sources.SequentialSourceProvider=DEBUG
```

### Checkpoint Structure

```
/checkpoints/hybrid-query/
├── sources/
│   ├── 0/  # Sequential source metadata
│   └── offsets/
├── commits/
└── metadata
```

## Best Practices

### 1. Schema Consistency
Ensure all sources produce compatible schemas:

```scala
// Define a common schema
val schema = StructType(Seq(
  StructField("timestamp", TimestampType, false),
  StructField("userId", StringType, false),
  StructField("amount", DoubleType, false)
))

val df = spark.readStream
  .format("delta")
    .name("source1")
    .schema(schema) // Apply consistent schema
    .option("path", "/path1")
  .format("kafka")
    .name("source2")
    .schema(schema) // Same schema
    .option("kafka.bootstrap.servers", "localhost:9092")
  .orderBy("source1", "source2")
  .load()
```

### 2. Watermark Management
Handle late data consistently across sources:

```scala
val df = spark.readStream
  // ... source configuration
  .load()
  .withWatermark("timestamp", "10 minutes")
  .dropDuplicatesWithinWatermark(Seq("id"), col("timestamp"))
```

### 3. Resource Management
Configure appropriate parallelism for each source:

```scala
val df = spark.readStream
  .format("delta")
    .name("historical")
    .option("maxFilesPerTrigger", "50") // Higher throughput for historical
  .format("kafka")
    .name("live")
    .option("maxOffsetsPerTrigger", "1000") // Rate limit live data
  .orderBy("historical", "live")
  .load()
```

## Limitations and Considerations

### Current Limitations
1. **Spark Connect**: Not yet supported (throws informative error)
2. **Dynamic Schema Evolution**: Schema must be consistent across sources
3. **Complex Joins**: Cross-source joins require careful planning

### Performance Considerations
- **Checkpoint Overhead**: Each source transition updates checkpoint metadata
- **Memory Usage**: Large offset maps for sources with many partitions
- **Latency**: Brief pause during source transitions

### Troubleshooting

#### Common Issues

**Issue**: "No sources have been configured"
```scala
// ❌ Wrong: calling orderBy without configuring sources
spark.readStream.orderBy("source1")

// ✅ Correct: configure sources first
spark.readStream
  .format("rate").name("source1").option("rowsPerSecond", "10")
  .orderBy("source1")
```

**Issue**: "Sources not configured: missing-source"
```scala
// ❌ Wrong: referencing unconfigured source
spark.readStream
  .format("rate").name("source1").option("rowsPerSecond", "10")
  .orderBy("source1", "missing-source")

// ✅ Correct: all referenced sources must be configured
spark.readStream
  .format("rate").name("source1").option("rowsPerSecond", "10")
  .format("rate").name("source2").option("rowsPerSecond", "5")
  .orderBy("source1", "source2")
```

**Issue**: "Delta Lake support not available"
- Ensure Delta Lake dependencies are included in classpath
- Use `--packages io.delta:delta-core_2.13:2.4.0` when submitting jobs

## Integration Examples

### With Structured Streaming Aggregations

```scala
val aggregatedStream = spark.readStream
  .format("delta")
    .name("historical")
    .option("path", "/historical")
    .option("endOffsets", "latest")
  .format("kafka")
    .name("live")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
  .orderBy("historical", "live")
  .load()
  .withWatermark("timestamp", "1 hour")
  .groupBy(
    window(col("timestamp"), "1 hour"),
    col("category")
  )
  .agg(
    count("*").as("event_count"),
    sum("amount").as("total_amount")
  )
```

### With Delta Lake Output

```scala
val query = processedData.writeStream
  .outputMode("append")
  .format("delta")
  .option("path", "/output/delta-table")
  .option("checkpointLocation", "/checkpoints/delta-output")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
```

This Sequential Streaming Source provides a powerful pattern for hybrid batch-streaming workloads, enabling seamless transitions from historical data processing to real-time stream processing.