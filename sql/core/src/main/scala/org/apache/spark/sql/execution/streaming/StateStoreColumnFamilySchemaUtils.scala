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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{AvroEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStoreColFamilySchema}
import org.apache.spark.sql.types.{NullType, StructField, StructType}

object StateStoreColumnFamilySchemaUtils {

  def apply(initializeAvroSerde: Boolean): StateStoreColumnFamilySchemaUtils =
    new StateStoreColumnFamilySchemaUtils(initializeAvroSerde)
}

/**
 *
 * @param initializeAvroSerde Whether or not to create the Avro serializers and deserializers
 *                            for this state type. This class is used to create the
 *                            StateStoreColumnFamilySchema for each state variable from the driver
 */
class StateStoreColumnFamilySchemaUtils(initializeAvroSerde: Boolean) {

  /**
   * If initializeAvroSerde is true, this method will create an Avro Serializer and Deserializer
   * for a particular key and value schema.
   */
  private def getAvroSerde(
      keySchema: StructType,
      valSchema: StructType,
      userKeySchema: Option[StructType] = None
  ): Option[AvroEncoderSpec] = {
    if (initializeAvroSerde) {
      val avroType = SchemaConverters.toAvroType(valSchema)
      val avroOptions = AvroOptions(Map.empty)
      val keyAvroType = SchemaConverters.toAvroType(keySchema)
      val keySer = new AvroSerializer(keySchema, keyAvroType, nullable = false)
      val keyDe = new AvroDeserializer(keyAvroType, keySchema,
        avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
        avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
      val valueSerializer = new AvroSerializer(valSchema, avroType, nullable = false)
      val valueDeserializer = new AvroDeserializer(avroType, valSchema,
        avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
        avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
      val (userKeySerializer, userKeyDeserializer) = if (userKeySchema.isDefined) {
        val userKeyAvroType = SchemaConverters.toAvroType(userKeySchema.get)
        val ukSer = new AvroSerializer(userKeySchema.get, userKeyAvroType, nullable = false)
        val ukDe = new AvroDeserializer(userKeyAvroType, userKeySchema.get,
          avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
          avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
        (Some(ukSer), Some(ukDe))
      } else {
        (None, None)
      }
      Some(AvroEncoderSpec(
        keySer, keyDe, valueSerializer, valueDeserializer, userKeySerializer, userKeyDeserializer))
    } else {
      None
    }
  }

  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): StateStoreColFamilySchema = {
   val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
   StateStoreColFamilySchema(
      stateName,
      keyEncoder.schema,
      valSchema,
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)),
      avroEnc = getAvroSerde(keyEncoder.schema, valSchema))
  }

  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): StateStoreColFamilySchema = {
  val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
  StateStoreColFamilySchema(
      stateName,
      keyEncoder.schema,
      valSchema,
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)),
      avroEnc = getAvroSerde(keyEncoder.schema, valSchema))
  }

  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): StateStoreColFamilySchema = {
    val compositeKeySchema = getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
    val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
    StateStoreColFamilySchema(
      stateName,
      compositeKeySchema,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
      Some(userKeyEnc.schema),
      avroEnc = getAvroSerde(
        StructType(compositeKeySchema.take(1)),
        valSchema,
        Some(StructType(compositeKeySchema.drop(1)))
      )
    )
  }

  def getTtlStateSchema(
      stateName: String,
      keyEncoder: ExpressionEncoder[Any]): StateStoreColFamilySchema = {
    val ttlKeySchema = getSingleKeyTTLAvroRowSchema(keyEncoder.schema)
    val ttlValSchema = StructType(
      Array(StructField("__dummy__", NullType)))
    StateStoreColFamilySchema(
      stateName,
      ttlKeySchema,
      ttlValSchema,
      Some(RangeKeyScanStateEncoderSpec(ttlKeySchema, Seq(0))),
      avroEnc = getAvroSerde(
        StructType(ttlKeySchema.take(1)),
        ttlValSchema,
        Some(StructType(ttlKeySchema.drop(1)))
      )
    )
  }

  def getTimerStateSchema(
      stateName: String,
      keySchema: StructType,
      valSchema: StructType): StateStoreColFamilySchema = {
    StateStoreColFamilySchema(
      stateName,
      keySchema,
      valSchema,
      Some(PrefixKeyScanStateEncoderSpec(keySchema, 1)))
  }
}
