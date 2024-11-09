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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{AvroEncoder, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStoreColFamilySchema}
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StructField, StructType}

object StateStoreColumnFamilySchemaUtils extends Serializable {

  def apply(initializeAvroSerde: Boolean): StateStoreColumnFamilySchemaUtils =
    new StateStoreColumnFamilySchemaUtils(initializeAvroSerde)

  /**
   * Avro uses zig-zag encoding for some fixed-length types, like Longs and Ints. For range scans
   * we want to use big-endian encoding, so we need to convert the source schema to replace these
   * types with BinaryType.
   *
   * @param schema The schema to convert
   * @param ordinals If non-empty, only convert fields at these ordinals.
   *                 If empty, convert all fields.
   */
  def convertForRangeScan(schema: StructType, ordinals: Seq[Int] = Seq.empty): StructType = {
    val ordinalSet = ordinals.toSet

    StructType(schema.fields.zipWithIndex.flatMap { case (field, idx) =>
      if ((ordinals.isEmpty || ordinalSet.contains(idx)) && isFixedSize(field.dataType)) {
        // For each numeric field, create two fields:
        // 1. A boolean for sign (positive = true, negative = false)
        // 2. The original numeric value in big-endian format
        Seq(
          StructField(s"${field.name}_marker", BooleanType, nullable = false),
          field.copy(name = s"${field.name}_value", BinaryType)
        )
      } else {
        Seq(field)
      }
    })
  }

  private def isFixedSize(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: BooleanType | _: ShortType | _: IntegerType | _: LongType |
         _: FloatType | _: DoubleType => true
    case _ => false
  }

  def getTtlColFamilyName(stateName: String): String = {
    "$ttl_" + stateName
  }
}

/**
 *
 * @param initializeAvroSerde Whether or not to create the Avro serializers and deserializers
 *                            for this state type. This class is used to create the
 *                            StateStoreColumnFamilySchema for each state variable from the driver
 */
class StateStoreColumnFamilySchemaUtils(initializeAvroSerde: Boolean)
    extends Logging with Serializable {
  private def getAvroSerializer(schema: StructType): AvroSerializer = {
    val avroType = SchemaConverters.toAvroType(schema)
    new AvroSerializer(schema, avroType, nullable = false)
  }

  private def getAvroDeserializer(schema: StructType): AvroDeserializer = {
    val avroType = SchemaConverters.toAvroType(schema)
    val avroOptions = AvroOptions(Map.empty)
    new AvroDeserializer(avroType, schema,
      avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
      avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
  }

  /**
   * If initializeAvroSerde is true, this method will create an Avro Serializer and Deserializer
   * for a particular key and value schema.
   */
  private[sql] def getAvroSerde(
      keySchema: StructType,
      valSchema: StructType,
      suffixKeySchema: Option[StructType] = None
  ): Option[AvroEncoder] = {
    if (initializeAvroSerde) {

      val (suffixKeySer, suffixKeyDe) = if (suffixKeySchema.isDefined) {
        (Some(getAvroSerializer(suffixKeySchema.get)),
          Some(getAvroDeserializer(suffixKeySchema.get)))
      } else {
        (None, None)
      }
      Some(AvroEncoder(
        getAvroSerializer(keySchema),
        getAvroDeserializer(keySchema),
        getAvroSerializer(valSchema),
        getAvroDeserializer(valSchema),
        suffixKeySer, suffixKeyDe))
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

  // This function creates the StateStoreColFamilySchema for
  // the TTL secondary index.
  // Because we want to encode fixed-length types as binary types
  // if we are using Avro, we need to do some schema conversion to ensure
  // we can use range scan
  def getTtlStateSchema(
      stateName: String,
      keyEncoder: ExpressionEncoder[Any]): StateStoreColFamilySchema = {
    val ttlKeySchema = StateStoreColumnFamilySchemaUtils.convertForRangeScan(
      getSingleKeyTTLRowSchema(keyEncoder.schema), Seq(0))
    val ttlValSchema = StructType(
      Array(StructField("__dummy__", NullType)))
    StateStoreColFamilySchema(
      stateName,
      ttlKeySchema,
      ttlValSchema,
      Some(RangeKeyScanStateEncoderSpec(ttlKeySchema, Seq(0))),
      avroEnc = getAvroSerde(
        StructType(ttlKeySchema.take(2)),
        ttlValSchema,
        Some(StructType(ttlKeySchema.drop(2)))
      )
    )
  }

  // This function creates the StateStoreColFamilySchema for
  // the TTL secondary index.
  // Because we want to encode fixed-length types as binary types
  // if we are using Avro, we need to do some schema conversion to ensure
  // we can use range scan
  def getTtlStateSchema(
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeySchema: StructType): StateStoreColFamilySchema = {
    val ttlKeySchema = StateStoreColumnFamilySchemaUtils.convertForRangeScan(
      getCompositeKeyTTLRowSchema(keyEncoder.schema, userKeySchema), Seq(0))
    val ttlValSchema = StructType(
      Array(StructField("__dummy__", NullType)))
    StateStoreColFamilySchema(
      stateName,
      ttlKeySchema,
      ttlValSchema,
      Some(RangeKeyScanStateEncoderSpec(ttlKeySchema, Seq(0))),
      avroEnc = getAvroSerde(
        StructType(ttlKeySchema.take(2)),
        ttlValSchema,
        Some(StructType(ttlKeySchema.drop(2)))
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
      Some(PrefixKeyScanStateEncoderSpec(keySchema, 1)),
      avroEnc = getAvroSerde(
        StructType(keySchema.take(1)),
        valSchema,
        Some(StructType(keySchema.drop(1)))
      ))
  }

  // This function creates the StateStoreColFamilySchema for
  // Timers' secondary index.
  // Because we want to encode fixed-length types as binary types
  // if we are using Avro, we need to do some schema conversion to ensure
  // we can use range scan
  def getTimerStateSchemaForSecIndex(
      stateName: String,
      keySchema: StructType,
      valSchema: StructType): StateStoreColFamilySchema = {
    val avroKeySchema = StateStoreColumnFamilySchemaUtils.
      convertForRangeScan(keySchema, Seq(0))
    StateStoreColFamilySchema(
      stateName,
      keySchema,
      valSchema,
      Some(RangeKeyScanStateEncoderSpec(keySchema, Seq(0))),
      avroEnc = getAvroSerde(
        StructType(avroKeySchema.take(2)),
        valSchema,
        Some(StructType(avroKeySchema.drop(2)))
      ))
  }
}
