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

import java.io.{OutputStream, StringReader}

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.json4s.{DefaultFormats, JsonAST}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, MetadataVersionUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

sealed trait ColumnFamilySchema extends Serializable {
  def jsonValue: JsonAST.JObject

  def json: String
}

case class ColumnFamilySchemaV1(
    val columnFamilyName: String,
    val keySchema: StructType,
    val valueSchema: StructType,
    val keyStateEncoderSpec: KeyStateEncoderSpec,
    val multipleValuesPerKey: Boolean) extends ColumnFamilySchema {
    def jsonValue: JsonAST.JObject = {
        ("columnFamilyName" -> JString(columnFamilyName)) ~
        ("keySchema" -> keySchema.json) ~
        ("valueSchema" -> valueSchema.json) ~
        ("keyStateEncoderSpec" -> keyStateEncoderSpec.jsonValue) ~
        ("multipleValuesPerKey" -> JBool(multipleValuesPerKey))
    }

    def json: String = {
      compact(render(jsonValue))
    }
}

object ColumnFamilySchemaV1 {
  def fromJson(json: List[Map[String, Any]]): List[ColumnFamilySchema] = {
    assert(json.isInstanceOf[List[_]])

    json.map { colFamilyMap =>
      new ColumnFamilySchemaV1(
        colFamilyMap("columnFamilyName").asInstanceOf[String],
        StructType.fromString(colFamilyMap("keySchema").asInstanceOf[String]),
        StructType.fromString(colFamilyMap("valueSchema").asInstanceOf[String]),
        KeyStateEncoderSpec.fromJson(colFamilyMap("keyStateEncoderSpec")
          .asInstanceOf[Map[String, Any]]),
        colFamilyMap("multipleValuesPerKey").asInstanceOf[Boolean]
      )
    }
  }

  def fromJValue(jValue: JValue): List[ColumnFamilySchema] = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val deserializedList: List[Any] = jValue.extract[List[Any]]
    assert(deserializedList.isInstanceOf[List[_]],
      s"Expected List but got ${deserializedList.getClass}")
    val columnFamilyMetadatas = deserializedList.asInstanceOf[List[Map[String, Any]]]
    // Extract each JValue to StateVariableInfo
    ColumnFamilySchemaV1.fromJson(columnFamilyMetadatas)
  }
}

/**
 * Helper classes for reading/writing state schema.
 */
object SchemaHelper {

  sealed trait SchemaReader {
    def read(inputStream: FSDataInputStream): (StructType, StructType)
  }

  object SchemaReader {
    def createSchemaReader(versionStr: String): SchemaReader = {
      val version = MetadataVersionUtil.validateVersion(versionStr,
        StateSchemaCompatibilityChecker.VERSION)
      version match {
        case 1 => new SchemaV1Reader
        case 2 => new SchemaV2Reader
      }
    }
  }

  class SchemaV1Reader extends SchemaReader {
    def read(inputStream: FSDataInputStream): (StructType, StructType) = {
      val keySchemaStr = inputStream.readUTF()
      val valueSchemaStr = inputStream.readUTF()
      (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
    }
  }

  class SchemaV2Reader extends SchemaReader {
    def read(inputStream: FSDataInputStream): (StructType, StructType) = {
      val buf = new StringBuilder
      val numKeyChunks = inputStream.readInt()
      (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val keySchemaStr = buf.toString()

      buf.clear()
      val numValueChunks = inputStream.readInt()
      (0 until numValueChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val valueSchemaStr = buf.toString()
      (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
    }
  }

  class SchemaV3Reader(
      stateCheckpointPath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration) {

    private val schemaFilePath = SchemaV3Writer.getSchemaFilePath(stateCheckpointPath)

    private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)
    def read: List[ColumnFamilySchema] = {
      if (!fm.exists(schemaFilePath)) {
          return List.empty
      }
      val buf = new StringBuilder
      val inputStream = fm.open(schemaFilePath)
      val numKeyChunks = inputStream.readInt()
      (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val json = buf.toString()
      val parsedJson = JsonMethods.parse(json)

      implicit val formats = DefaultFormats
      val deserializedList: List[Any] = parsedJson.extract[List[Any]]
      assert(deserializedList.isInstanceOf[List[_]],
        s"Expected List but got ${deserializedList.getClass}")
      val columnFamilyMetadatas = deserializedList.asInstanceOf[List[Map[String, Any]]]
      // Extract each JValue to StateVariableInfo
      ColumnFamilySchemaV1.fromJson(columnFamilyMetadatas)
    }
  }

  trait SchemaWriter {
    val version: Int

    final def write(
        keySchema: StructType,
        valueSchema: StructType,
        outputStream: FSDataOutputStream): Unit = {
      writeVersion(outputStream)
      writeSchema(keySchema, valueSchema, outputStream)
    }

    private def writeVersion(outputStream: FSDataOutputStream): Unit = {
      outputStream.writeUTF(s"v${version}")
    }

    protected def writeSchema(
        keySchema: StructType,
        valueSchema: StructType,
        outputStream: FSDataOutputStream): Unit
  }

  object SchemaWriter {
    def createSchemaWriter(version: Int): SchemaWriter = {
      version match {
        case 1 if Utils.isTesting => new SchemaV1Writer
        case 2 => new SchemaV2Writer
      }
    }
  }

  class SchemaV1Writer extends SchemaWriter {
    val version: Int = 1

    def writeSchema(
        keySchema: StructType,
        valueSchema: StructType,
        outputStream: FSDataOutputStream): Unit = {
      outputStream.writeUTF(keySchema.json)
      outputStream.writeUTF(valueSchema.json)
    }
  }

  class SchemaV2Writer extends SchemaWriter {
    val version: Int = 2

    // 2^16 - 1 bytes
    final val MAX_UTF_CHUNK_SIZE = 65535

    def writeSchema(
        keySchema: StructType,
        valueSchema: StructType,
        outputStream: FSDataOutputStream): Unit = {
      val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)

      // DataOutputStream.writeUTF can't write a string at once
      // if the size exceeds 65535 (2^16 - 1) bytes.
      // So a key as well as a value consist of multiple chunks in schema version 2.
      val keySchemaJson = keySchema.json
      val numKeyChunks = (keySchemaJson.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val keyStringReader = new StringReader(keySchemaJson)
      outputStream.writeInt(numKeyChunks)
      (0 until numKeyChunks).foreach { _ =>
        val numRead = keyStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }

      val valueSchemaJson = valueSchema.json
      val numValueChunks = (valueSchemaJson.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val valueStringReader = new StringReader(valueSchemaJson)
      outputStream.writeInt(numValueChunks)
      (0 until numValueChunks).foreach { _ =>
        val numRead = valueStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }
    }
  }

  object SchemaV3Writer {
    def getSchemaFilePath(stateCheckpointPath: Path): Path = {
      new Path(new Path(stateCheckpointPath, "_metadata"), "schema")
    }

    def serialize(out: OutputStream, schema: List[ColumnFamilySchema]): Unit = {
      val json = schema.map(_.json)
      out.write(compact(render(json)).getBytes("UTF-8"))
    }
  }
  /**
   * Schema writer for schema version 3. Because this writer writes out ColFamilyMetadatas
   * instead of key and value schemas, it is not compatible with the SchemaWriter interface.
   */
  class SchemaV3Writer(
      stateCheckpointPath: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration) {
    val version: Int = 3

    private lazy val fm = CheckpointFileManager.create(stateCheckpointPath, hadoopConf)
    private val schemaFilePath = SchemaV3Writer.getSchemaFilePath(stateCheckpointPath)

    // 2^16 - 1 bytes
    final val MAX_UTF_CHUNK_SIZE = 65535
    def writeSchema(metadatasJson: String): Unit = {
      val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)

      if (fm.exists(schemaFilePath)) return

      fm.mkdirs(schemaFilePath.getParent)
      val outputStream = fm.createAtomic(schemaFilePath, overwriteIfPossible = false)
      // DataOutputStream.writeUTF can't write a string at once
      // if the size exceeds 65535 (2^16 - 1) bytes.
      // Each metadata consists of multiple chunks in schema version 3.
      try {
        val numMetadataChunks = (metadatasJson.length - 1) / MAX_UTF_CHUNK_SIZE + 1
        val metadataStringReader = new StringReader(metadatasJson)
        outputStream.writeInt(numMetadataChunks)
        (0 until numMetadataChunks).foreach { _ =>
          val numRead = metadataStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
          outputStream.writeUTF(new String(buf, 0, numRead))
        }
        outputStream.close()
      } catch {
        case e: Throwable =>
          outputStream.cancel()
          throw e
      }
    }
  }
}
