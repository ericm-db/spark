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

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.TTLStateKeyValueRowSchema.{TTL_KEY_ROW_SCHEMA, TTL_VALUE_ROW_SCHEMA}
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types.{BinaryType, LongType, StructType}

object StateKeyValueRowSchema {
  val KEY_ROW_SCHEMA: StructType = new StructType().add("key", BinaryType)
  val VALUE_ROW_SCHEMA: StructType = new StructType().add("value", BinaryType)
    .add("ttl", LongType)

  def encodeGroupingKeyBytes(keyBytes: Array[Byte]): UnsafeRow = {
    val keyProjection = UnsafeProjection.create(KEY_ROW_SCHEMA)
    val keyRow = keyProjection(InternalRow(keyBytes))
    keyRow
  }
}

object TTLStateKeyValueRowSchema {
  val TTL_KEY_ROW_SCHEMA: StructType = new StructType()
    .add("ttl", LongType)
    .add("stateName", BinaryType)
    .add("groupingKey", BinaryType)
    .add("userKey", BinaryType)
  val TTL_VALUE_ROW_SCHEMA: StructType = new StructType().add("value", BinaryType)
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
 * @param keySerializer - serializer to serialize the grouping key of type `GK`
 *     to an [[InternalRow]]
 * @param stateName - name of logical state partition
 * @tparam GK - grouping key type
 */
class StateTypesEncoder[GK](
    keySerializer: Serializer[GK],
    stateName: String) {
  import org.apache.spark.sql.execution.streaming.StateKeyValueRowSchema._

  private val keyProjection = UnsafeProjection.create(KEY_ROW_SCHEMA)
  private val valueProjection = UnsafeProjection.create(VALUE_ROW_SCHEMA)

  private val ttlKeyRowEncoder = UnsafeProjection.create(TTL_KEY_ROW_SCHEMA)
  private val ttlValueRowEncoder = UnsafeProjection.create(TTL_VALUE_ROW_SCHEMA)

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeGroupingKey(): UnsafeRow = {
    val keyRow = keyProjection(InternalRow(getGroupingKeyBytes()))
    keyRow
  }
  def getGroupingKeyBytes(): Array[Byte] = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get.asInstanceOf[GK]
    keySerializer.apply(groupingKey).asInstanceOf[UnsafeRow].getBytes()
  }

  def getTTLRowKey(expirationTimestamp: Long): UnsafeRow = {
    val stateNameArr = SerializationUtils.serialize(stateName.asInstanceOf[Serializable])
    val groupingKeyArr = getGroupingKeyBytes()

    ttlKeyRowEncoder(
      InternalRow(expirationTimestamp, stateNameArr, groupingKeyArr, null))
  }

  def getTTLRowValue(): UnsafeRow = {
    ttlValueRowEncoder(InternalRow(null))
  }

  def encodeValue[S](value: S, expirationTimestamp: Long): UnsafeRow = {
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueRow = valueProjection(InternalRow(valueByteArr, expirationTimestamp))
    valueRow
  }

  def decodeValue[S](row: UnsafeRow): S = {
    SerializationUtils
      .deserialize(row.getBinary(0))
      .asInstanceOf[S]
  }
}

object StateTypesEncoder {
  def apply[GK](
      keySerializer: Serializer[GK],
      stateName: String): StateTypesEncoder[GK] = {
    new StateTypesEncoder[GK](keySerializer, stateName)
  }
}
