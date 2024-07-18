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

import java.io.StringReader

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.sql.execution.streaming.MetadataVersionUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Helper classes for reading/writing state schema.
 */
case class StateSchemaFormatV3(
    stateStoreColFamilySchema: List[StateStoreColFamilySchema]
)

object SchemaHelper {

  sealed trait SchemaReader {
    def version: Int

    def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema]
  }

  object SchemaReader {
    def createSchemaReader(versionStr: String): SchemaReader = {
      val version = MetadataVersionUtil.validateVersion(versionStr,
        2)
      version match {
        case 1 => new SchemaV1Reader
        case 2 => new SchemaV2Reader
        case 3 => new SchemaV3Reader
      }
    }
  }

  class SchemaV1Reader extends SchemaReader {
    override def version: Int = 1

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      val keySchemaStr = inputStream.readUTF()
      val valueSchemaStr = inputStream.readUTF()
      List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
        StructType.fromString(keySchemaStr),
        StructType.fromString(valueSchemaStr)))
    }
  }

  class SchemaV2Reader extends SchemaReader {
    override def version: Int = 2

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      val buf = new StringBuilder
      val numKeyChunks = inputStream.readInt()
      (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val keySchemaStr = buf.toString()

      buf.clear()
      val numValueChunks = inputStream.readInt()
      (0 until numValueChunks).foreach(_ => buf.append(inputStream.readUTF()))
      val valueSchemaStr = buf.toString()
      List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
        StructType.fromString(keySchemaStr),
        StructType.fromString(valueSchemaStr)))
    }
  }

  class SchemaV3Reader extends SchemaReader {
    private implicit val formats: Formats = Serialization.formats(NoTypeHints)

    override def version: Int = 3

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      val stateSchema = Serialization.read[StateSchemaFormatV3](inputStream)
      stateSchema.stateStoreColFamilySchema
    }
  }

  trait SchemaWriter {
    def version: Int

    final def write(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      writeVersion(outputStream)
      writeSchema(stateStoreColFamilySchema, outputStream)
    }

    private def writeVersion(outputStream: FSDataOutputStream): Unit = {
      outputStream.writeUTF(s"v${version}")
    }

    protected def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit
  }

  object SchemaWriter {
    def createSchemaWriter(version: Int): SchemaWriter = {
      version match {
        case 1 if Utils.isTesting => new SchemaV1Writer
        case 2 => new SchemaV2Writer
        case 3 => new SchemaV3Writer
      }
    }
  }

  class SchemaV1Writer extends SchemaWriter {
    override def version: Int = 1

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      assert(stateStoreColFamilySchema.length == 1)
      val stateSchema = stateStoreColFamilySchema.head
      outputStream.writeUTF(stateSchema.keySchema.json)
      outputStream.writeUTF(stateSchema.valueSchema.json)
    }
  }

  class SchemaV2Writer extends SchemaWriter {
    override def version: Int = 2

    // 2^16 - 1 bytes
    final val MAX_UTF_CHUNK_SIZE = 65535

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      assert(stateStoreColFamilySchema.length == 1)
      val stateSchema = stateStoreColFamilySchema.head
      val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)

      // DataOutputStream.writeUTF can't write a string at once
      // if the size exceeds 65535 (2^16 - 1) bytes.
      // So a key as well as a value consist of multiple chunks in schema version 2.
      val keySchemaJson = stateSchema.keySchema.json
      val numKeyChunks = (keySchemaJson.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val keyStringReader = new StringReader(keySchemaJson)
      outputStream.writeInt(numKeyChunks)
      (0 until numKeyChunks).foreach { _ =>
        val numRead = keyStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }

      val valueSchemaJson = stateSchema.valueSchema.json
      val numValueChunks = (valueSchemaJson.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val valueStringReader = new StringReader(valueSchemaJson)
      outputStream.writeInt(numValueChunks)
      (0 until numValueChunks).foreach { _ =>
        val numRead = valueStringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }
    }
  }

  class SchemaV3Writer extends SchemaWriter {
    override def version: Int = 3

    private implicit val formats: Formats = Serialization.formats(NoTypeHints)

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      val stateSchema = StateSchemaFormatV3(stateStoreColFamilySchema)
      Serialization.write(stateSchema, outputStream)
    }
  }

}
