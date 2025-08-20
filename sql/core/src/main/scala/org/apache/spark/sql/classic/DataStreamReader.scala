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

package org.apache.spark.sql.classic

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedDataSource
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.json.JsonUtils.checkJsonSchema
import org.apache.spark.sql.execution.datasources.xml.XmlUtils.checkXmlSchema
import org.apache.spark.sql.execution.streaming.OffsetSeqV2
import org.apache.spark.sql.streaming
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamReader private[sql](sparkSession: SparkSession)
  extends streaming.DataStreamReader {
  /** @inheritdoc */
  def format(source: String): this.type = {
    // If we have a current source being configured, save it and start a new one
    if (currentSourceName.isDefined) {
      saveCurrentSourceConfig()
    }

    this.source = source
    this.currentSourceName = None // Will be set when name() is called
    this
  }

  /** @inheritdoc */
  override def name(sourceName: String): this.type = {
    OffsetSeqV2.validateSourceName(sourceName)
    this.userSpecifiedName = Some(sourceName)
    this.currentSourceName = Some(sourceName)

    // Initialize config for this source with the format
    val config = Map("format" -> source)
    multiSourceConfigs = multiSourceConfigs + (sourceName -> config)
    this
  }

  /** @inheritdoc */
  def schema(schema: StructType): this.type = {
    if (schema != null) {
      val replaced = CharVarcharUtils.failIfHasCharVarchar(schema).asInstanceOf[StructType]
      this.userSpecifiedSchema = Option(replaced)
    }
    this
  }

  /** @inheritdoc */
  def option(key: String, value: String): this.type = {
    // If we're building multi-source config, add to current source
    currentSourceName match {
      case Some(sourceName) =>
        val currentConfig = multiSourceConfigs.getOrElse(sourceName, Map.empty)
        val updatedConfig = currentConfig + (key -> value)
        multiSourceConfigs = multiSourceConfigs + (sourceName -> updatedConfig)
      case None =>
        // Fall back to single-source behavior
        this.extraOptions += (key -> value)
    }
    this
  }

  /** @inheritdoc */
  def options(options: scala.collection.Map[String, String]): this.type = {
    // If we're building multi-source config, add to current source
    currentSourceName match {
      case Some(sourceName) =>
        val currentConfig = multiSourceConfigs.getOrElse(sourceName, Map.empty)
        val updatedConfig = currentConfig ++ options
        multiSourceConfigs = multiSourceConfigs + (sourceName -> updatedConfig)
      case None =>
        // Fall back to single-source behavior
        this.extraOptions ++= options
    }
    this
  }

  /** @inheritdoc */
  def load(): DataFrame = loadInternal(None)

  private def loadInternal(path: Option[String]): DataFrame = {
    val unresolved = UnresolvedDataSource(
      source,
      userSpecifiedSchema,
      extraOptions,
      isStreaming = true,
      path.toSeq,
      userSpecifiedName
    )
    Dataset.ofRows(sparkSession, unresolved)
  }

  /** @inheritdoc */
  def load(path: String): DataFrame = {
    if (!sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw QueryCompilationErrors.setPathOptionAndCallWithPathParameterError("load")
    }
    loadInternal(Some(path))
  }

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    require(tableName != null, "The table name can't be null")
    val identifier = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    Dataset.ofRows(
      sparkSession,
      UnresolvedRelation(
        identifier,
        new CaseInsensitiveStringMap(extraOptions.toMap.asJava),
        isStreaming = true))
  }

  override protected def assertNoSpecifiedSchema(operation: String): Unit = {
    if (userSpecifiedSchema.nonEmpty) {
      throw QueryCompilationErrors.userSpecifiedSchemaUnsupportedError(operation)
    }
  }

  override protected def validateJsonSchema(): Unit = userSpecifiedSchema.foreach(checkJsonSchema)

  override protected def validateXmlSchema(): Unit = userSpecifiedSchema.foreach(checkXmlSchema)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant overrides.
  ///////////////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def schema(schemaString: String): this.type = super.schema(schemaString)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = super.options(options)

  /** @inheritdoc */
  override def json(path: String): DataFrame = super.json(path)

  /** @inheritdoc */
  override def csv(path: String): DataFrame = super.csv(path)

  /** @inheritdoc */
  override def xml(path: String): DataFrame = super.xml(path)

  /** @inheritdoc */
  override def orc(path: String): DataFrame = super.orc(path)

  /** @inheritdoc */
  override def parquet(path: String): DataFrame = super.parquet(path)

  /** @inheritdoc */
  override def text(path: String): DataFrame = super.text(path)

  /** @inheritdoc */
  override def textFile(path: String): Dataset[String] = super.textFile(path)

  def orderBy(sourceNames: String*): DataStreamReader = {
    require(sourceNames.nonEmpty, "At least one source name must be specified")
    require(multiSourceConfigs.nonEmpty, "No sources have been configured. " +
      "Use .format().name().option() to configure sources before calling orderBy()")

    // Validate that all specified source names have been configured
    val configuredNames = multiSourceConfigs.keys.toSet
    val missingNames = sourceNames.toSet -- configuredNames
    require(missingNames.isEmpty,
      s"Sources not configured: ${missingNames.mkString(", ")}. " +
      s"Available sources: ${configuredNames.mkString(", ")}")

    // Create a new reader configured for sequential processing
    val newReader = new DataStreamReader(sparkSession)
    newReader.source = "sequential"
    newReader.extraOptions = CaseInsensitiveMap(Map(
      "sequential.order" -> sourceNames.mkString(",")
    ))

    // Add all source configurations with proper prefixing
    multiSourceConfigs.foreach { case (sourceName, config) =>
      config.foreach { case (key, value) =>
        newReader.extraOptions += (s"$sourceName.$key" -> value)
      }
    }

    newReader
  }

  private def saveCurrentSourceConfig(): Unit = {
    // This method is called when starting a new source configuration
    // Currently, the configuration is saved immediately in the name() method,
    // so this is a placeholder for any future cleanup logic
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = sparkSession.sessionState.conf.defaultDataSourceName

  private var userSpecifiedSchema: Option[StructType] = None

  private var userSpecifiedName: Option[String] = None

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  // Support for multi-source configuration
  private var multiSourceConfigs: Map[String, Map[String, String]] = Map.empty
  private var currentSourceName: Option[String] = None
}
