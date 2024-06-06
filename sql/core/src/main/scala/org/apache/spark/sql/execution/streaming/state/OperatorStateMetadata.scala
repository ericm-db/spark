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

package org.apache.spark.sql.execution.streaming.state

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.Serialization

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, MetadataVersionUtil}
import org.apache.spark.util.AccumulatorV2

/**
 * Metadata for a state store instance.
 */
trait StateStoreMetadata {
  def storeName: String
  def numColsPrefixKey: Int
  def numPartitions: Int
}

case class StateStoreMetadataV1(storeName: String, numColsPrefixKey: Int, numPartitions: Int)
  extends StateStoreMetadata

/**
 * Information about a stateful operator.
 */
trait OperatorInfo {
  def operatorId: Long
  def operatorName: String
}

case class OperatorInfoV1(operatorId: Long, operatorName: String) extends OperatorInfo

trait OperatorStateMetadata {
  def version: Int

  def operatorInfo: OperatorInfo

  def stateStoreInfo: Array[StateStoreMetadataV1]
}

object OperatorStateMetadata {
  def metadataFilePath(stateCheckpointPath: Path): Path =
    new Path(new Path(stateCheckpointPath, "_metadata"), "metadata")
}

case class OperatorStateMetadataV1(
    operatorInfo: OperatorInfoV1,
    stateStoreInfo: Array[StateStoreMetadataV1]) extends OperatorStateMetadata {
  override def version: Int = 1
}

/**
 * Accumulator to store arbitrary Operator properties.
 * This accumulator is used to store the properties of an operator that are not
 * available on the driver at the time of planning, and will only be known from
 * the executor side.
 */
class OperatorProperties(initValue: Map[String, JValue] = Map.empty)
  extends AccumulatorV2[Map[String, JValue], Map[String, JValue]] {

  private var _value: Map[String, JValue] = initValue

  override def isZero: Boolean = _value.isEmpty

  override def copy(): AccumulatorV2[Map[String, JValue], Map[String, JValue]] = {
    val newAcc = new OperatorProperties
    newAcc._value = _value
    newAcc
  }

  override def reset(): Unit = _value = Map.empty[String, JValue]

  override def add(v: Map[String, JValue]): Unit = _value ++= v

  override def merge(other: AccumulatorV2[Map[String, JValue], Map[String, JValue]]): Unit = {
    _value ++= other.value
  }

  override def value: Map[String, JValue] = _value
}

object OperatorProperties {
  def create(
      sc: SparkContext,
      name: String,
      initValue: Map[String, JValue] = Map.empty): OperatorProperties = {
    val acc = new OperatorProperties(initValue)
    acc.register(sc, name = Some(name))
    acc
  }
}

// operatorProperties is an arbitrary JSON formatted string that contains
// any properties that we would want to store for a particular operator.
case class OperatorStateMetadataV2(
    operatorInfo: OperatorInfoV1,
    stateStoreInfo: Array[StateStoreMetadataV1],
    operatorPropertiesJson: String) extends OperatorStateMetadata {
  override def version: Int = 2
}

object OperatorStateMetadataV1 {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  @scala.annotation.nowarn
  private implicit val manifest = Manifest
    .classType[OperatorStateMetadataV1](implicitly[ClassTag[OperatorStateMetadataV1]].runtimeClass)

  def deserialize(in: BufferedReader): OperatorStateMetadata = {
    Serialization.read[OperatorStateMetadataV1](in)
  }

  def serialize(
      out: FSDataOutputStream,
      operatorStateMetadata: OperatorStateMetadata): Unit = {
    Serialization.write(operatorStateMetadata.asInstanceOf[OperatorStateMetadataV1], out)
  }
}

object OperatorStateMetadataV2 {
  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  @scala.annotation.nowarn
  private implicit val manifest = Manifest
    .classType[OperatorStateMetadataV2](implicitly[ClassTag[OperatorStateMetadataV2]].runtimeClass)

  def deserialize(in: BufferedReader): OperatorStateMetadata = {
    Serialization.read[OperatorStateMetadataV2](in)
  }

  def serialize(
      out: FSDataOutputStream,
      operatorStateMetadata: OperatorStateMetadata): Unit = {
    Serialization.write(operatorStateMetadata.asInstanceOf[OperatorStateMetadataV2], out)
  }
}

/**
 * Write OperatorStateMetadata into the state checkpoint directory.
 */
class OperatorStateMetadataWriter(stateCheckpointPath: Path, hadoopConf: Configuration)
  extends Logging {

  private val metadataFilePath = OperatorStateMetadata.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  def write(operatorMetadata: OperatorStateMetadata): Unit = {
    if (fm.exists(metadataFilePath)) return

    fm.mkdirs(metadataFilePath.getParent)
    val outputStream = fm.createAtomic(metadataFilePath, overwriteIfPossible = false)
    try {
      outputStream.write(s"v${operatorMetadata.version}\n".getBytes(StandardCharsets.UTF_8))
      operatorMetadata.version match {
        case 1 =>
          OperatorStateMetadataV1.serialize(outputStream, operatorMetadata)
        case 2 =>
          OperatorStateMetadataV2.serialize(outputStream, operatorMetadata)
      }
      outputStream.close()
    } catch {
      case e: Throwable =>
        logError(s"Fail to write state metadata file to $metadataFilePath", e)
        outputStream.cancel()
        throw e
    }
  }
}

/**
 * Read OperatorStateMetadata from the state checkpoint directory.
 */
class OperatorStateMetadataReader(stateCheckpointPath: Path, hadoopConf: Configuration) {

  private val metadataFilePath = OperatorStateMetadata.metadataFilePath(stateCheckpointPath)

  private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)

  def read(): OperatorStateMetadata = {
    val inputStream = fm.open(metadataFilePath)
    val inputReader =
      new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    try {
      val versionStr = inputReader.readLine()
      val version = MetadataVersionUtil.validateVersion(versionStr, 2)
      assert(version == 1 || version == 2)
      version match {
        case 1 => OperatorStateMetadataV1.deserialize(inputReader)
        case 2 => OperatorStateMetadataV2.deserialize(inputReader)
      }
    } finally {
      inputStream.close()
    }
  }
}
