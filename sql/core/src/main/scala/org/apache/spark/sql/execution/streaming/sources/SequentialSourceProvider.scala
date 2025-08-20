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

import java.{util => ju}

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * The provider class for Sequential Streaming Sources. This allows chaining multiple
 * streaming sources in sequence, processing one completely before moving to the next.
 * Example usage:
 * val df = spark.readStream
 *   .format("delta")
 *     .name("historical-delta")
 *     .option("path", "/path/to/historical/events")
 *     .option("readChangeFeed", "true")
 *     .option("startingVersion", "0")
 *     .option("endOffsets", "latest")
 *   .format("kafka")
 *     .name("live-kafka")
 *     .option("kafka.bootstrap.servers", "cluster:9092")
 *     .option("subscribe", "events")
 *     .option("startingOffsets", "latest")
 *   .orderBy("historical-delta", "live-kafka")
 *   .load()
 */
class SequentialSourceProvider extends DataSourceRegister
    with StreamSourceProvider
    with SimpleTableProvider
    with Logging {

  import SequentialSourceProvider._

  override def shortName(): String = "sequential"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    val caseInsensitiveParameters = CaseInsensitiveMap(parameters)
    validateStreamOptions(caseInsensitiveParameters)

    // The schema will be determined by the first source in the sequence
    val firstSourceConfig = extractFirstSourceConfig(caseInsensitiveParameters)
    // TODO(human): Implement schema inference from the first configured source
    (shortName(), schema.getOrElse(StructType.fromDDL("value string")))
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    throw new UnsupportedOperationException("Sequential source only supports MicroBatchStream API")
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new SequentialStreamingTable(options)
  }

  private def validateStreamOptions(parameters: CaseInsensitiveMap[String]): Unit = {
    val sourceOrder = parameters.get(SOURCE_ORDER_KEY)
    require(sourceOrder.isDefined, s"$SOURCE_ORDER_KEY must be specified")

    val sourceNames = sourceOrder.get.split(",").map(_.trim)
    require(sourceNames.nonEmpty, "At least one source must be specified in order")

    // Validate that all named sources have configurations
    sourceNames.foreach { sourceName =>
      val sourceConfig = extractSourceConfig(parameters, sourceName)
      require(sourceConfig.nonEmpty, s"Configuration for source '$sourceName' not found")
    }
  }

  private def extractFirstSourceConfig(
      parameters: CaseInsensitiveMap[String]): CaseInsensitiveMap[String] = {
    val sourceOrder = parameters(SOURCE_ORDER_KEY).split(",").map(_.trim)
    extractSourceConfig(parameters, sourceOrder.head)
  }

  private def extractSourceConfig(
      parameters: CaseInsensitiveMap[String],
      sourceName: String): CaseInsensitiveMap[String] = {
    val prefix = s"$sourceName."
    CaseInsensitiveMap(parameters.filter(_._1.startsWith(prefix)).map {
      case (key, value) => (key.substring(prefix.length), value)
    })
  }
}

class SequentialStreamingTable(options: CaseInsensitiveStringMap)
    extends Table with SupportsRead with Logging {

  override def name(): String = "SequentialStreamingTable"

  override def schema(): StructType = {
    // Return the unified schema used by all sequential sources
    import org.apache.spark.sql.types._
    StructType(Seq(
      StructField("timestamp", TimestampType, nullable = false),
      StructField("value", LongType, nullable = false)
    ))
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new SequentialScanBuilder(this.options, options)
  }
}

class SequentialScanBuilder(
    tableOptions: CaseInsensitiveStringMap,
    scanOptions: CaseInsensitiveStringMap) extends ScanBuilder {

  override def build(): Scan = new SequentialScan(tableOptions, scanOptions)
}

class SequentialScan(
    tableOptions: CaseInsensitiveStringMap,
    scanOptions: CaseInsensitiveStringMap) extends Scan {

  override def readSchema(): StructType = {
    // Schema should be determined from the first source
    StructType.fromDDL("value string") // placeholder
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    new SequentialMicroBatchStream(tableOptions, checkpointLocation)
  }
}

object SequentialSourceProvider {
  val SOURCE_ORDER_KEY = "sequential.order"
  val SOURCE_NAME_PREFIX = "source."
}
