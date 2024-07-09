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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema._
import org.apache.spark.sql.execution.streaming.state.{ColumnFamilySchema, ColumnFamilySchemaV1, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec}

trait ColumnFamilySchemaUtils {
  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchema


  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchema


  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): ColumnFamilySchema
}

object ColumnFamilySchemaUtilsV1 extends ColumnFamilySchemaUtils {

  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    ColumnFamilySchemaV1(
      stateName,
      getKeySchema(keyEncoder.schema),
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA))
  }

  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    ColumnFamilySchemaV1(
      stateName,
      getKeySchema(keyEncoder.schema),
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA))
  }

  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    val compositeKeySchema = getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
    ColumnFamilySchemaV1(
      stateName,
      compositeKeySchema,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      PrefixKeyScanStateEncoderSpec(COMPOSITE_KEY_ROW_SCHEMA, 1),
      Some(userKeyEnc.schema))
  }
}
