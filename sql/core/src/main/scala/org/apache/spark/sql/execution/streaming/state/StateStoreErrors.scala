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

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}

/**
 * Object for grouping error messages from (most) exceptions thrown from State API V2
 *
 * ERROR_CLASS has a prefix of "STATE_STORE_" to indicate where the error is from
 */
object StateStoreErrors {
  def implicitKeyNotFound(stateName: String): SparkException = {
    SparkException.internalError(
      msg = s"Implicit key not found in state store for stateName=$stateName",
      category = "TWS"
    )
  }

  def multipleColumnFamiliesNotSupported(stateStoreProvider: String):
    StateStoreMultipleColumnFamiliesNotSupportedException = {
      new StateStoreMultipleColumnFamiliesNotSupportedException(stateStoreProvider)
    }

  def ttlNotSupportedWithProvider(stateStoreProvider: String):
  StateStoreTTLNotSupportedException = {
    new StateStoreTTLNotSupportedException(stateStoreProvider)
  }

  def removingColumnFamiliesNotSupported(stateStoreProvider: String):
    StateStoreRemovingColumnFamiliesNotSupportedException = {
        new StateStoreRemovingColumnFamiliesNotSupportedException(stateStoreProvider)
    }

  def cannotRemoveDefaultColumnFamily(colFamilyName: String):
    StateStoreCannotRemoveDefaultColumnFamily = {
        new StateStoreCannotRemoveDefaultColumnFamily(colFamilyName)
    }

  def cannotCreateColumnFamilyWithInvalidName(colFamilyName: String):
    StateStoreCannotCreateColumnFamilyWithInvalidName = {
      new StateStoreCannotCreateColumnFamilyWithInvalidName(colFamilyName)
  }

  def unsupportedOperationException(operationName: String, entity: String):
    StateStoreUnsupportedOperationException = {
      new StateStoreUnsupportedOperationException(operationName, entity)
    }

  def requireNonNullStateValue(value: Any, stateName: String): Unit = {
    SparkException.require(value != null,
      errorClass = "ILLEGAL_STATE_STORE_VALUE.NULL_VALUE",
      messageParameters = Map("stateName" -> stateName))
  }

  def requireNonEmptyListStateValue[S](value: Array[S], stateName: String): Unit = {
    SparkException.require(value.nonEmpty,
      errorClass = "ILLEGAL_STATE_STORE_VALUE.EMPTY_LIST_VALUE",
      messageParameters = Map("stateName" -> stateName))
  }

  def cannotCreateColumnFamilyWithReservedChars(colFamilyName: String):
    StateStoreCannotCreateColumnFamilyWithReservedChars = {
      new StateStoreCannotCreateColumnFamilyWithReservedChars(colFamilyName)
  }

  def cannotUseTimersWithInvalidTimeoutMode(timeoutMode: String):
    StatefulProcessorCannotUseTimersWithInvalidTimeoutMode = {
      new StatefulProcessorCannotUseTimersWithInvalidTimeoutMode(timeoutMode)
  }

  def cannotUseTimersWithInvalidHandleState(handleState: String):
    StatefulProcessorCannotUseTimersWithInvalidHandleState = {
    new StatefulProcessorCannotUseTimersWithInvalidHandleState(handleState)
  }
}

class StateStoreMultipleColumnFamiliesNotSupportedException(stateStoreProvider: String)
  extends SparkUnsupportedOperationException(
    errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES",
    messageParameters = Map("stateStoreProvider" -> stateStoreProvider)
  )

class StateStoreTTLNotSupportedException(stateStoreProvider: String)
  extends SparkUnsupportedOperationException(
    errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_TTL",
    messageParameters = Map("stateStoreProvider" -> stateStoreProvider)
  )

class StateStoreRemovingColumnFamiliesNotSupportedException(stateStoreProvider: String)
  extends SparkUnsupportedOperationException(
    errorClass = "UNSUPPORTED_FEATURE.STATE_STORE_REMOVING_COLUMN_FAMILIES",
    messageParameters = Map("stateStoreProvider" -> stateStoreProvider)
  )

class StateStoreCannotRemoveDefaultColumnFamily(colFamilyName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_CANNOT_REMOVE_DEFAULT_COLUMN_FAMILY",
    messageParameters = Map("colFamilyName" -> colFamilyName)
  )

class StateStoreCannotCreateColumnFamilyWithReservedChars(colFamilyName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_CANNOT_CREATE_COLUMN_FAMILY_WITH_RESERVED_CHARS",
    messageParameters = Map("colFamilyName" -> colFamilyName)
  )

class StateStoreCannotCreateColumnFamilyWithInvalidName(colFamilyName: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_CANNOT_CREATE_COLUMN_FAMILY_WITH_INVALID_NAME",
    messageParameters = Map("colFamilyName" -> colFamilyName)
  )

class StateStoreUnsupportedOperationException(operationType: String, entity: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATE_STORE_UNSUPPORTED_OPERATION",
    messageParameters = Map("operationType" -> operationType, "entity" -> entity)
  )

class StatefulProcessorCannotUseTimersWithInvalidTimeoutMode(timeoutMode: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_USE_TIMERS_WITH_INVALID_TIMEOUT_MODE",
    messageParameters = Map("timeoutMode" -> timeoutMode)
  )

class StatefulProcessorCannotUseTimersWithInvalidHandleState(handleState: String)
  extends SparkUnsupportedOperationException(
    errorClass = "STATEFUL_PROCESSOR_CANNOT_USE_TIMERS_WITH_INVALID_HANDLE_STATE",
    messageParameters = Map("handleState" -> handleState)
  )
