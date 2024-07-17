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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema.getPqKeySchema
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object TransformWithStateKeyValueRowSchema {
  /**
   * The following are the key/value row schema used in StateStore layer.
   * Key/value rows will be serialized into Binary format in `StateTypesEncoder`.
   * The "real" key/value row schema will be written into state schema metadata.
   */
  def valueRowSchemaWithTTL(valueRowSchema: StructType): StructType =
    valueRowSchema.add("ttlExpirationMs", LongType)

  /** Helper functions for passing the key/value schema to write to state schema metadata. */
  // Return value schema with additional TTL column if TTL is enabled.
  def getValueSchemaWithTTL(schema: StructType, hasTTL: Boolean): StructType = {
    val valSchema = if (hasTTL) {
      new StructType(schema.fields).add("ttlExpirationMs", LongType)
    } else schema
    new StructType()
      .add("value", valSchema)
  }

  // Given grouping key and user key schema, return the schema of the composite key.
  def getCompositeKeySchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType = {
    new StructType()
      .add("key", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))
  }

  def getPqKeySchema(
      groupingKeySchema: StructType,
      valueSchema: StructType): StructType = {
    new StructType()
      .add("priority", LongType)
      .add("key", new StructType(groupingKeySchema.fields))
      .add("value", new StructType(valueSchema.fields))
      .add("uuid", StringType)
  }

  // keyEncoder.schema will attach a default "value" as field name if
  // the object is a primitive type encoder
  private def isPrimitiveType(schema: StructType): Boolean = {
    schema.length == 1 && schema.fields.head.name == "value"
  }

  private def getPrimitiveType(schema: StructType): DataType = {
    assert(isPrimitiveType(schema))
    schema.fields.head.dataType
  }

  def realCompositeType(
      keySchema: StructType,
      userKeySchema: StructType): StructType = {
    var compositeSchema = new StructType()
    if (isPrimitiveType(keySchema)) {
      compositeSchema =
        compositeSchema.add("key", getPrimitiveType(keySchema))
    } else {
      compositeSchema = compositeSchema.add("key", keySchema)
    }

    if (isPrimitiveType(userKeySchema)) {
      compositeSchema =
        compositeSchema.add("userKey", getPrimitiveType(userKeySchema))
    } else {
      compositeSchema = compositeSchema.add("userKey", userKeySchema)
    }

    compositeSchema
  }
}

/**
 * Helper class providing APIs to encode the grouping key, and user provided values
 * to Spark [[UnsafeRow]].
 *
 * CAUTION: StateTypesEncoder class instance is *not* thread-safe.
 * This class reuses the keyProjection and valueProjection for encoding grouping
 * key and state value respectively. As UnsafeProjection is not thread safe, this
 * class is also not thread safe.
 *
 * @param keyEncoder - SQL encoder for the grouping key, key type is implicit
 * @param valEncoder - SQL encoder for value of type `S`
 * @param stateName - name of logical state partition
 * @tparam V - value type
 */
class StateTypesEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean) {

  /** Variables reused for value conversions between spark sql and object */
  private val keySerializer = keyEncoder.createSerializer()
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()
  private val valueTTLProjection =
    UnsafeProjection.create(valEncoder.schema.add("ttlExpirationMs", LongType))

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    keySerializer.apply(keyOption.get).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow with no ttl.
   */
  def encodeValue(value: V): UnsafeRow = {
    objToRowSerializer.apply(value).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow
   * with provided ttl expiration.
   */
  def encodeValue(value: V, expirationMs: Long): UnsafeRow = {
    val objRow: InternalRow = objToRowSerializer.apply(value)
    val newValArr: Array[Any] =
      objRow.toSeq(valEncoder.schema).toArray :+ expirationMs

    valueTTLProjection.apply(new GenericInternalRow(newValArr))
  }

  def decodeValue(row: UnsafeRow): V = {
    rowToObjDeserializer.apply(row)
  }

  /**
   * Decode the ttl information out of Value row. If the ttl has
   * not been set (-1L specifies no user defined value), the API will
   * return None.
   */
  def decodeTtlExpirationMs(row: UnsafeRow): Option[Long] = {
    // ensure ttl has been set
    assert(hasTtl)
    val expirationMs = row.getLong(valEncoder.schema.length)
    if (expirationMs == -1) {
      None
    } else {
      Some(expirationMs)
    }
  }

  def isExpired(row: UnsafeRow, batchTimestampMs: Long): Boolean = {
    val expirationMs = decodeTtlExpirationMs(row)
    expirationMs.exists(StateTTL.isExpired(_, batchTimestampMs))
  }
}

object StateTypesEncoder {
  def apply[V](
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[V],
      stateName: String,
      hasTtl: Boolean = false): StateTypesEncoder[V] = {
    new StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl)
  }
}

class CompositeKeyStateEncoder[K, V](
    keyEncoder: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean = false)
  extends StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl) {
  import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema._

  // keyEncoder.schema will attach a default "value" as field name if
  // the object is a primitive type encoder
  private def isPrimitiveType(schema: StructType): Boolean = {
    schema.length == 1 && schema.fields.head.name == "value"
  }

  private def getPrimitiveType(schema: StructType): DataType = {
    assert(isPrimitiveType(schema))
    schema.fields.head.dataType
  }

  private val schemaForCompositeKeyRow =
    getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
  private val compositeKeyProjection = UnsafeProjection.create(schemaForCompositeKeyRow)
  private val reusedKeyRow = new UnsafeRow(userKeyEnc.schema.fields.length)
  private val userKeyExpressionEnc = encoderFor(userKeyEnc)

  private val userKeyRowToObjDeserializer =
    userKeyExpressionEnc.resolveAndBind().createDeserializer()
  private val userKeySerializer = encoderFor(userKeyEnc).createSerializer()

  override def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val realGroupingKey =
      if (groupingKey.isInstanceOf[String]) UTF8String.fromString(groupingKey.asInstanceOf[String])
      else groupingKey
    val keyRow = new GenericInternalRow(Array[Any](realGroupingKey))

    val keyProj = UnsafeProjection.create(new StructType().add("key", keyEncoder.schema))

    keyProj.apply(InternalRow(keyRow))
  }

  def serializeUserKey(userKey: K): Array[Byte] = {
    val realKey =
      if (userKey.isInstanceOf[String]) UTF8String.fromString(userKey.asInstanceOf[String])
      else userKey
    val keyRow = new GenericInternalRow(Array[Any](realKey))

    val keyProj = UnsafeProjection.create(new StructType().add("userKey", keyEncoder.schema))

    keyProj.apply(InternalRow(keyRow)).getBytes
  }

  def encodeCompositeKey(
      groupingKeyByteArr: Array[Byte],
      userKeyByteArr: Array[Byte]): UnsafeRow = {
    compositeKeyProjection(InternalRow(groupingKeyByteArr, userKeyByteArr))
  }


  /**
   * Grouping key and user key are encoded as a row of `schemaForCompositeKeyRow` schema.
   * Grouping key will be encoded in `RocksDBStateEncoder` as the prefix column.
   */
  def encodeCompositeKey(userKey: K): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val realGroupingKey =
      if (groupingKey.isInstanceOf[String]) UTF8String.fromString(groupingKey.asInstanceOf[String])
      else groupingKey
    val realUserKey =
      if (userKey.isInstanceOf[String]) UTF8String.fromString(userKey.asInstanceOf[String])
      else userKey

    // Create the nested InternalRows for the inner structs (value)
    val keyRow = new GenericInternalRow(Array[Any](realGroupingKey))
    val userKeyRow = new GenericInternalRow(Array[Any](realUserKey))

    // Create the final InternalRow combining the nested keyRow and userKeyRow
    val compositeRow = new GenericInternalRow(Array[Any](keyRow, userKeyRow))

    val compositeKey = compositeKeyProjection(compositeRow)
    val decode = compositeKey.getString(0)
    compositeKey
  }

  def decodeUserKeyFromTTLRow(row: CompositeKeyTTLRow): K = {
    val bytes = row.userKey
    reusedKeyRow.pointTo(bytes, bytes.length)
    val userKey = userKeyRowToObjDeserializer.apply(reusedKeyRow)
    userKey
  }

  /**
   * The input row is of composite Key schema.
   * Only user key is returned though grouping key also exist in the row.
   */
  def decodeCompositeKey(row: UnsafeRow): K = {
    userKeyRowToObjDeserializer.apply(row.getStruct(1, 1))
  }
}

class PQKeyStateEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean = false)
  extends StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl) {

  private val compositeKeySchema = getPqKeySchema(keyEncoder.schema, valEncoder.schema)
  private val compositeKeyProjection = UnsafeProjection.create(compositeKeySchema)
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()

  private def decodePriority(row: UnsafeRow): Long = {
    val compositeKey = compositeKeyProjection(row)
    compositeKey.getLong(0)
  }

  def encodeCompositeKey(
      priority: Long,
      value: V,
      uuid: String): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val realGroupingKey =
      if (groupingKey.isInstanceOf[String]) UTF8String.fromString(groupingKey.asInstanceOf[String])
      else groupingKey

    val keyRow = new GenericInternalRow(Array[Any](realGroupingKey))
    val valueRow = objToRowSerializer.apply(value)
    compositeKeyProjection(
      InternalRow(priority, keyRow, valueRow, UTF8String.fromString(uuid)))
  }

  private def decodeGroupingKey(row: UnsafeRow): Array[Byte] = {
    val compositeKey = compositeKeyProjection(row)
    compositeKey.getBinary(1)
  }

  override def decodeValue(row: UnsafeRow): V = {
    val compositeKey = compositeKeyProjection(row)
    rowToObjDeserializer.apply(compositeKey.getStruct(2, 1))
  }

  def decodeUUID(row: UnsafeRow): String = {
    val compositeKey = compositeKeyProjection(row)
    compositeKey.getString(3)
  }

  def decodeCompositeKey(row: UnsafeRow): (Long, Array[Byte], V, Long) = {
    (decodePriority(row), decodeGroupingKey(row), decodeValue(row), decodeUUID(row))
  }
}

