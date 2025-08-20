/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.sources

import java.io.{BufferedWriter, InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import scala.jdk.CollectionConverters._

import org.apache.commons.io.IOUtils

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset => StreamingOffset, ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.execution.streaming.{LongOffset, SerializedOffset, Source, _}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A MicroBatchStream that processes multiple sources sequentially. Each source is processed
 * to completion before moving to the next source in the sequence.
 *
 * State management:
 * - Tracks which source is currently active
 * - Maintains individual source offsets within a composite offset
 * - Handles transitions between sources when one completes
 */
class SequentialMicroBatchStream(
    options: CaseInsensitiveStringMap,
    checkpointLocation: String)
  extends MicroBatchStream with Logging {

  import SequentialMicroBatchStream._
  import SequentialSourceProvider._

  private val sourceOrder: Array[String] = options.get(SOURCE_ORDER_KEY).split(",").map(_.trim)
  private val sourceConfigurations: Map[String, CaseInsensitiveStringMap] =
    sourceOrder.map { sourceName =>
      val prefix = s"$sourceName."
      val sourceOptions = options.asCaseSensitiveMap().asScala.filter(_._1.startsWith(prefix))
        .map { case (key, value) => (key.substring(prefix.length), value) }
      sourceName -> new CaseInsensitiveStringMap(sourceOptions.asJava)
    }.toMap

  private val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    .getOrElse(throw new IllegalStateException("No active Spark session found"))

  // Establish a unified schema for all sources in the sequence
  // This ensures schema compatibility across different source types
  private lazy val unifiedSchema: org.apache.spark.sql.types.StructType = {
    import org.apache.spark.sql.types._
    // Use rate source compatible schema as the unified schema
    // All sources will be projected to this schema
    StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("value", LongType, nullable = false)
    ))
  }

  private val metadataLog = new HDFSMetadataLog[SequentialOffset](session, checkpointLocation) {
    override def serialize(metadata: SequentialOffset, out: OutputStream): Unit = {
      val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
      writer.write("v" + VERSION + "\n")
      writer.write(metadata.json)
      writer.flush()
    }

    override def deserialize(in: InputStream): SequentialOffset = {
      val content = IOUtils.toString(in, StandardCharsets.UTF_8)
      assert(content.nonEmpty)
      if (content.startsWith("v")) {
        val indexOfNewLine = content.indexOf("\n")
        if (indexOfNewLine > 0) {
          validateVersion(content.substring(0, indexOfNewLine), VERSION)
          SequentialOffset.fromJson(content.substring(indexOfNewLine + 1))
        } else {
          throw new IllegalStateException("Log file was malformed: failed to detect version line")
        }
      } else {
        throw new IllegalStateException("Log file was malformed: failed to detect version line")
      }
    }
  }

  @volatile private var currentSourceStreams: Map[String, MicroBatchStream] = Map.empty
  @volatile private var lastKnownOffset: SequentialOffset = _
  @volatile private var previousOffsets: Map[String,
    org.apache.spark.sql.execution.streaming.Offset] = Map.empty
  @volatile private var offsetStableCount: Map[String, Int] = Map.empty

  override def initialOffset(): StreamingOffset = {
    lastKnownOffset = metadataLog.get(0).getOrElse {
      val initialOffset = SequentialOffset(
        currentSourceIndex = 0,
        currentSourceName = sourceOrder.head,
        sourceOffsets = Map.empty,
        isCompleted = false
      )
      metadataLog.add(0, initialOffset)
      logInfo(log"Sequential stream initialized with first source:" +
        log" ${MDC(NAME, sourceOrder.head)}")
      initialOffset
    }
    lastKnownOffset
  }

  override def latestOffset(): StreamingOffset = {
    if (lastKnownOffset == null) {
      lastKnownOffset = initialOffset().asInstanceOf[SequentialOffset]
    }

    if (lastKnownOffset.isCompleted) {
      return lastKnownOffset
    }

    val currentSource = getCurrentSource()
    val currentSourceLatest = currentSource.latestOffset()

    // Convert V2 offset to V1 offset for internal storage
    val v1CurrentOffset = SerializedOffset(currentSourceLatest.json)

    val updatedOffset = lastKnownOffset.copy(
      sourceOffsets = lastKnownOffset.sourceOffsets +
        (lastKnownOffset.currentSourceName -> v1CurrentOffset)
    )

    // Check if current source has completed and we need to transition
    val shouldTransition = checkSourceCompletion(v1CurrentOffset)

    if (shouldTransition && lastKnownOffset.currentSourceIndex < sourceOrder.length - 1) {
      // Transition to next source
      val nextSourceIndex = lastKnownOffset.currentSourceIndex + 1
      val nextSourceName = sourceOrder(nextSourceIndex)

      logInfo(
        log"Transitioning from " +
        log"${MDC(STREAMING_DATA_SOURCE_NAME, lastKnownOffset.currentSourceName)} " +
        log"to ${MDC(STREAMING_DATA_SOURCE_NAME, nextSourceName)}")

      val transitionOffset = SequentialOffset(
        currentSourceIndex = nextSourceIndex,
        currentSourceName = nextSourceName,
        sourceOffsets = lastKnownOffset.sourceOffsets,
        isCompleted = false
      )
      lastKnownOffset = transitionOffset
      return transitionOffset
    } else if (shouldTransition) {
      // All sources completed
      val completedOffset = lastKnownOffset.copy(isCompleted = true)
      lastKnownOffset = completedOffset
      return completedOffset
    }

    lastKnownOffset = updatedOffset
    updatedOffset
  }

  override def deserializeOffset(json: String): StreamingOffset = {
    SequentialOffset.fromJson(json)
  }

  override def planInputPartitions(start: StreamingOffset,
      end: StreamingOffset): Array[InputPartition] = {
    val startSeq = start.asInstanceOf[SequentialOffset]
    val endSeq = end.asInstanceOf[SequentialOffset]

    if (startSeq.currentSourceName != endSeq.currentSourceName) {
      logInfo(log"Source transition detected: " +
        log"${MDC(NAME, s"${startSeq.currentSourceName} -> ${endSeq.currentSourceName}")}")
      // Handle source transition
      Array.empty // No data during transition
    } else {
      // Process current source
      val currentSource = getCurrentSource()
      val sourceStart = startSeq.sourceOffsets.get(startSeq.currentSourceName)
      val sourceEnd = endSeq.sourceOffsets.get(endSeq.currentSourceName)

      if (sourceStart.isDefined && sourceEnd.isDefined) {
        // Convert serialized offsets back to the source's native offset type
        val nativeStartOffset = deserializeToNativeOffset(sourceStart.get,
          startSeq.currentSourceName)
        val nativeEndOffset = deserializeToNativeOffset(sourceEnd.get,
          endSeq.currentSourceName)
        currentSource.planInputPartitions(nativeStartOffset, nativeEndOffset)
      } else {
        Array.empty
      }
    }
  }

  private def deserializeToNativeOffset(
      offset: org.apache.spark.sql.execution.streaming.Offset,
      sourceName: String): StreamingOffset = {
    val sourceConfig = sourceConfigurations(sourceName)
    val format = sourceConfig.get("format")
    // scalastyle:off caselocale
    format.toLowerCase match {
    // scalastyle:on caselocale
      case "rate" =>
        // Rate source uses LongOffset
        LongOffset(offset.json.toLong)
      case _ =>
        // For other sources, use SerializedOffset
        SerializedOffset(offset.json)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    getCurrentSource().createReaderFactory()
  }

  override def commit(end: StreamingOffset): Unit = {
    val endSeq = end.asInstanceOf[SequentialOffset]
    getCurrentSource().commit(endSeq.sourceOffsets.getOrElse(endSeq.currentSourceName,
      getCurrentSource().initialOffset()))
  }

  override def stop(): Unit = {
    currentSourceStreams.values.foreach(_.stop())
    currentSourceStreams = Map.empty
  }

  private def checkSourceCompletion(
      currentOffset: org.apache.spark.sql.execution.streaming.Offset): Boolean = {
    val sourceName = lastKnownOffset.currentSourceName
    val previousOffset = previousOffsets.get(sourceName)

    val isStable = previousOffset match {
      case Some(prev) if prev.json == currentOffset.json =>
        // Offset hasn't changed, increment stable count
        val newCount = offsetStableCount.getOrElse(sourceName, 0) + 1
        offsetStableCount = offsetStableCount + (sourceName -> newCount)
        newCount >= 3 // Consider complete after 3 stable readings
      case _ =>
        // Offset changed, reset stable count
        offsetStableCount = offsetStableCount + (sourceName -> 0)
        false
    }

    // Update previous offset for next comparison
    previousOffsets = previousOffsets + (sourceName -> currentOffset)

    // Check source-specific completion conditions
    val sourceConfig = sourceConfigurations(sourceName)
    val hasEndOffsets = sourceConfig.containsKey("endOffsets")

    // For bounded sources with endOffsets, completion is when offset is stable
    // For unbounded sources, they never complete (unless explicitly configured)
    if (hasEndOffsets) {
      isStable
    } else {
      // Unbounded sources (like Kafka) don't complete naturally
      false
    }
  }

  private def getCurrentSource(): MicroBatchStream = {
    val sourceName = lastKnownOffset.currentSourceName
    currentSourceStreams.getOrElse(sourceName, {
      logInfo(log"Creating source stream for ${MDC(STREAMING_DATA_SOURCE_NAME, sourceName)}")
      val newStream = createSourceStream(sourceName)
      currentSourceStreams = currentSourceStreams + (sourceName -> newStream)
      logInfo(log"Successfully created source stream for " +
        log"${MDC(STREAMING_DATA_SOURCE_NAME, sourceName)}")
      newStream
    })
  }

  private def createSourceStream(sourceName: String): MicroBatchStream = {
    val sourceConfig = sourceConfigurations(sourceName)
    val format = sourceConfig.get("format")
    require(format != null, s"Format not specified for source $sourceName")

    logInfo(log"Creating source stream for ${MDC(STREAMING_DATA_SOURCE_NAME, sourceName)} " +
      log"with format ${MDC(FILE_FORMAT, format)}")

    // scalastyle:off caselocale
    format.toLowerCase match {
    // scalastyle:on caselocale
      case "kafka" =>
        createKafkaStream(sourceConfig, sourceName)
      case "delta" =>
        createDeltaStream(sourceConfig, sourceName)
      case "parquet" | "json" | "csv" | "text" =>
        createFileStream(sourceConfig, sourceName, format)
      case "rate" =>
        createRateStream(sourceConfig, sourceName)
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported format '$format' for source '$sourceName'. " +
          s"Supported formats: kafka, delta, parquet, json, csv, text, rate")
    }
  }

  private def createRateStream(
      config: CaseInsensitiveStringMap,
      sourceName: String): MicroBatchStream = {
    try {
      // Use reflection to create Rate stream to avoid direct dependency
      // scalastyle:off classforname
      val rateProviderClass = Class.forName(
        "org.apache.spark.sql.execution.streaming.sources.RateStreamProvider")
      // scalastyle:on classforname
      val rateProvider = rateProviderClass.getDeclaredConstructor().newInstance()

      // Get the table from the provider
      val getTableMethod = rateProviderClass.getMethod("getTable",
        classOf[CaseInsensitiveStringMap])
      val rateTable = getTableMethod.invoke(rateProvider, config)

      // Create scan builder and micro batch stream
      val newScanBuilderMethod = rateTable.getClass.getMethod("newScanBuilder",
        classOf[CaseInsensitiveStringMap])
      val scanBuilder = newScanBuilderMethod.invoke(rateTable, config)

      val buildMethod = scanBuilder.getClass.getMethod("build")
      val scan = buildMethod.invoke(scanBuilder)

      val toMicroBatchStreamMethod = scan.getClass.getMethod("toMicroBatchStream",
        classOf[String])
      val checkpointPath = s"$checkpointLocation/$sourceName"
      toMicroBatchStreamMethod.invoke(scan, checkpointPath).asInstanceOf[MicroBatchStream]

    } catch {
      case _: ClassNotFoundException =>
        throw new UnsupportedOperationException(
          s"Rate source not available. This should not happen as rate source is built-in.")
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create rate stream for $sourceName", ex)
    }
  }

  private def createKafkaStream(
      config: CaseInsensitiveStringMap,
      sourceName: String): MicroBatchStream = {
    try {
      // Use reflection to create Kafka stream to avoid direct dependency
      // scalastyle:off classforname
      val kafkaProviderClass = Class.forName("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      // scalastyle:on classforname
      val kafkaProvider = kafkaProviderClass.getDeclaredConstructor().newInstance()

      // Get the table from the provider
      val getTableMethod = kafkaProviderClass.getMethod("getTable",
        classOf[CaseInsensitiveStringMap])
      val kafkaTable = getTableMethod.invoke(kafkaProvider, config)

      // Create scan builder and micro batch stream
      val newScanBuilderMethod = kafkaTable.getClass.getMethod("newScanBuilder",
        classOf[CaseInsensitiveStringMap])
      val scanBuilder = newScanBuilderMethod.invoke(kafkaTable, config)

      val buildMethod = scanBuilder.getClass.getMethod("build")
      val scan = buildMethod.invoke(scanBuilder)

      val toMicroBatchStreamMethod = scan.getClass.getMethod("toMicroBatchStream",
        classOf[String])
      val checkpointPath = s"$checkpointLocation/$sourceName"
      toMicroBatchStreamMethod.invoke(scan, checkpointPath).asInstanceOf[MicroBatchStream]

    } catch {
      case _: ClassNotFoundException =>
        throw new UnsupportedOperationException(
          s"Kafka support not available. Please include spark-sql-kafka connector.")
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create Kafka stream for $sourceName", ex)
    }
  }

  private def createDeltaStream(
      config: CaseInsensitiveStringMap,
      sourceName: String): MicroBatchStream = {
    try {
      // Use reflection to create Delta stream to avoid direct dependency
      // scalastyle:off classforname
      val deltaProviderClass = Class.forName("io.delta.sql.DeltaDataSource")
      // scalastyle:on classforname
      val deltaProvider = deltaProviderClass.getDeclaredConstructor().newInstance()

      val getTableMethod = deltaProviderClass.getMethod("getTable",
        classOf[CaseInsensitiveStringMap])
      val deltaTable = getTableMethod.invoke(deltaProvider, config)

      val newScanBuilderMethod = deltaTable.getClass.getMethod("newScanBuilder",
        classOf[CaseInsensitiveStringMap])
      val scanBuilder = newScanBuilderMethod.invoke(deltaTable, config)

      val buildMethod = scanBuilder.getClass.getMethod("build")
      val scan = buildMethod.invoke(scanBuilder)

      val toMicroBatchStreamMethod = scan.getClass.getMethod("toMicroBatchStream",
        classOf[String])
      val checkpointPath = s"$checkpointLocation/$sourceName"
      toMicroBatchStreamMethod.invoke(scan, checkpointPath).asInstanceOf[MicroBatchStream]

    } catch {
      case _: ClassNotFoundException =>
        throw new UnsupportedOperationException(
          s"Delta Lake support not available. Please include delta-core dependency.")
      case ex: Exception =>
        throw new RuntimeException(s"Failed to create Delta stream for $sourceName", ex)
    }
  }

  private def createFileStream(
      config: CaseInsensitiveStringMap,
      sourceName: String,
      format: String): MicroBatchStream = {
    try {
      val path = config.get("path")
      require(path != null, s"Path must be specified for file source $sourceName")

      logInfo(
        log"Creating file stream for ${MDC(STREAMING_DATA_SOURCE_NAME, sourceName)} " +
        log"with format ${MDC(FILE_FORMAT, format)}")

      // Use the unified schema for all file sources to ensure compatibility
      // with other sources in the sequence
      logInfo(log"Using unified schema for file source: ${MDC(NAME, unifiedSchema.toString)}")

      // Use the same approach as Spark's built-in DataSource.createSource()
      // Create a DataSource and then create a Source from it
      val optionsMap = config.asCaseSensitiveMap().asScala.toMap
      val dataSource = org.apache.spark.sql.execution.datasources.DataSource(
        sparkSession = session,
        className = format,
        userSpecifiedSchema = Some(unifiedSchema), // Use unified schema
        options = optionsMap
      )

      // Create the V1 Source using DataSource.createSource()
      val metadataPath = s"$checkpointLocation/$sourceName"
      val v1Source = dataSource.createSource(metadataPath)

      // Wrap the V1 Source in a MicroBatchStream adapter with unified schema
      new SourceToMicroBatchAdapter(v1Source, Some(unifiedSchema))

    } catch {
      case ex: Exception =>
        throw new RuntimeException(
          s"Failed to create file stream for $sourceName: ${ex.getMessage}", ex)
    }
  }

  override def toString: String = {
    s"SequentialMicroBatchStream[sources=${sourceOrder.mkString(", ")}, " +
      s"current=${if (lastKnownOffset != null) lastKnownOffset.currentSourceName else "none"}]"
  }

  /**
   * Returns the unified schema that all sources in the sequence conform to.
   * This ensures type compatibility across different source types.
   */
  def schema: org.apache.spark.sql.types.StructType = unifiedSchema
}

case class SequentialOffset(
    currentSourceIndex: Int,
    currentSourceName: String,
    sourceOffsets: Map[String, org.apache.spark.sql.execution.streaming.Offset],
    isCompleted: Boolean) extends StreamingOffset {

  override def json: String = {
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    val offsetsJson = sourceOffsets.map { case (name, offset) =>
      name -> offset.json
    }

    val jsonObj = ("currentSourceIndex" -> currentSourceIndex) ~
      ("currentSourceName" -> currentSourceName) ~
      ("sourceOffsets" -> offsetsJson) ~
      ("isCompleted" -> isCompleted)

    compact(render(jsonObj))
  }
}

object SequentialOffset {
  def fromJson(json: String): SequentialOffset = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._

    implicit val formats: Formats = DefaultFormats
    val jsonObj = parse(json)

    val currentSourceIndex = (jsonObj \ "currentSourceIndex").extract[Int]
    val currentSourceName = (jsonObj \ "currentSourceName").extract[String]
    val isCompleted = (jsonObj \ "isCompleted").extract[Boolean]

    // Parse source offsets - this needs to be enhanced to handle different offset types
    val sourceOffsetsJson = (jsonObj \ "sourceOffsets").extract[Map[String, String]]
    val sourceOffsets = sourceOffsetsJson.map { case (name, offsetJson) =>
      // TODO: This should dynamically deserialize based on source type
      name -> SerializedOffset(offsetJson)
    }

    SequentialOffset(currentSourceIndex, currentSourceName, sourceOffsets, isCompleted)
  }
}


object SequentialMicroBatchStream {
  val VERSION = 1
}

/**
 * Adapter that wraps a V1 Source and exposes it as a V2 MicroBatchStream.
 * This allows us to use Spark's built-in file streaming sources (FileStreamSource)
 * within the V2 MicroBatchStream interface.
 */
private class SourceToMicroBatchAdapter(
    source: Source,
    schemaOverride: Option[org.apache.spark.sql.types.StructType] = None)
  extends MicroBatchStream with SupportsAdmissionControl {
  /**
   * Returns the schema for this source. Uses the override schema if provided,
   * otherwise falls back to the source's natural schema.
   */
  def schema: org.apache.spark.sql.types.StructType = {
    schemaOverride.getOrElse(source.schema)
  }
  override def initialOffset(): StreamingOffset = {
    val v1Offset = source.getOffset.orNull
    if (v1Offset != null) SerializedOffset(v1Offset.json) else null
  }

  override def latestOffset(): StreamingOffset = {
    // For sources that support admission control, this should delegate to the newer method
    latestOffset(null, getDefaultReadLimit())
  }

  // Implement the admission control interface method
  override def latestOffset(startOffset: StreamingOffset, readLimit: ReadLimit): StreamingOffset = {
    // If the V1 source supports admission control, use it
    source match {
      case admissionControlSource: SupportsAdmissionControl =>
        val v1StartOffset = if (startOffset != null) SerializedOffset(startOffset.json) else null
        val result = admissionControlSource.latestOffset(v1StartOffset, readLimit)
        if (result != null) SerializedOffset(result.json) else null
      case _ =>
        // Fallback to regular getOffset for sources that don't support admission control
        val v1Offset = source.getOffset.orNull
        if (v1Offset != null) SerializedOffset(v1Offset.json) else null
    }
  }

  override def deserializeOffset(json: String): StreamingOffset = SerializedOffset(json)

  override def planInputPartitions(start: StreamingOffset,
      end: StreamingOffset): Array[InputPartition] = {
    // Convert V2 offsets back to V1 offsets for getBatch
    val v1Start = if (start != null) SerializedOffset(start.json) else null
    val v1End = if (end != null) SerializedOffset(end.json) else null
    val batch = source.getBatch(Option(v1Start), v1End)
    // Convert DataFrame to RDD[InternalRow] then to InputPartitions
    val internalRDD = batch.queryExecution.toRdd
    internalRDD.partitions.indices.map { i =>
      new RDDInputPartition(internalRDD, i)
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new RDDPartitionReaderFactory()
  }

  override def commit(end: StreamingOffset): Unit = {
    // Convert V2 offset back to V1 offset for commit
    val v1End = if (end != null) SerializedOffset(end.json) else null
    source.commit(v1End)
  }

  override def stop(): Unit = source.stop()
}

/**
 * InputPartition implementation that wraps an RDD partition
 */
private class RDDInputPartition(rdd: RDD[InternalRow], partitionIndex: Int)
  extends InputPartition {

  def getRDD: RDD[InternalRow] = rdd
  def getPartitionIndex: Int = partitionIndex
}

/**
 * PartitionReaderFactory that creates readers for RDD partitions
 */
private class RDDPartitionReaderFactory extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val rddPartition = partition.asInstanceOf[RDDInputPartition]
    new RDDPartitionReader(rddPartition.getRDD, rddPartition.getPartitionIndex)
  }
}

/**
 * PartitionReader that reads from a specific RDD partition
 */
private class RDDPartitionReader(rdd: RDD[InternalRow], partitionIndex: Int)
  extends PartitionReader[InternalRow] {

  // Note: RDD partition computation should happen during task execution,
  // not during object creation
  private var iterator: Iterator[InternalRow] = _

  override def next(): Boolean = {
    if (iterator == null) {
      iterator = rdd.compute(rdd.partitions(partitionIndex), org.apache.spark.TaskContext.get())
    }
    iterator.hasNext
  }

  override def get(): InternalRow = iterator.next()

  override def close(): Unit = {}
}

