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

import java.time.Duration

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.StateKeyValueRowSchema.{KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore, StateStoreErrors}
import org.apache.spark.sql.streaming.{ListState, TTLMode}

/**
 * Provides concrete implementation for list of values associated with a state variable
 * used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @param keyEnc - Spark SQL encoder for key
 * @param valEncoder - Spark SQL encoder for value
 * @tparam S - data type of object that will be stored in the list
 */
class ListStateImpl[S](
     store: StateStore,
     stateName: String,
     keyExprEnc: ExpressionEncoder[Any],
     valEncoder: Encoder[S],
     ttlMode: TTLMode,
     batchTimestampMs: Option[Long],
     eventTimeWatermarkMs: Option[Long])
  extends ListState[S]
    with Logging
    with StateVariableWithTTLSupport {

  private val keySerializer = keyExprEnc.createSerializer()
  private val stateTypesEncoder = StateTypesEncoder(keySerializer, valEncoder, stateName)
  private[sql] var ttlState: Option[SingleKeyTTLState] = None

  initialize()
  private def initialize(): Unit = {
    store.createColFamilyIfAbsent(stateName, KEY_ROW_SCHEMA, VALUE_ROW_SCHEMA,
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA), useMultipleValuesPerKey = true)

    if (ttlMode != TTLMode.NoTTL()) {
      val _ttlState = new SingleKeyTTLState(ttlMode, stateName, store,
        batchTimestampMs, eventTimeWatermarkMs)
      ttlState = Some(_ttlState)
    }
  }
  /** Whether state exists or not. */
   override def exists(): Boolean = {
     val encodedGroupingKey = stateTypesEncoder.encodeGroupingKey()
     val stateValue = store.get(encodedGroupingKey, stateName)
     stateValue != null
   }

   /**
    * Get the state value if it exists. If the state does not exist in state store, an
    * empty iterator is returned.
    */
   override def get(): Iterator[S] = {

     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)
     var currentRow: UnsafeRow = null
     var shouldGetNextValidRow = true
     var isFirst = true

     new Iterator[S] {
       override def hasNext: Boolean = {
         if (shouldGetNextValidRow) {
           setNextValidRow()
         }
         currentRow != null
       }

       override def next(): S = {
         if (shouldGetNextValidRow) {
           setNextValidRow()
         }
         if (currentRow == null) {
           throw new NoSuchElementException("Iterator is at the end")
         }
         shouldGetNextValidRow = true
         stateTypesEncoder.decodeValue(currentRow)
       }

       // sets currentRow to a valid state, where we are
       // pointing to a non-expired row
       private def setNextValidRow(): Unit = {
         assert(shouldGetNextValidRow)
         shouldGetNextValidRow = false
         if (unsafeRowValuesIterator.hasNext) {
           currentRow = unsafeRowValuesIterator.next()
         } else {
           currentRow = null
           return
         }
         while (unsafeRowValuesIterator.hasNext && (isFirst || isExpired(currentRow))) {
           isFirst = false
           currentRow = unsafeRowValuesIterator.next()
         }
         // in this case, we have iterated to the end, and there are no
         // non-expired values
         if (currentRow != null && isExpired(currentRow)) {
           currentRow = null
         }
       }
     }
   }

   /** Update the value of the list. */
   override def put(newState: Array[S], ttlDuration: Duration = Duration.ZERO): Unit = {
     validateNewState(newState)

     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     val serializedGroupingKey = stateTypesEncoder.serializeGroupingKey()
     var isFirst = true

     var expirationMs: Long = -1
     if (ttlDuration != null && ttlDuration != Duration.ZERO) {
       expirationMs = StateTTL.calculateExpirationTimeForDuration(
         ttlMode, ttlDuration, batchTimestampMs, eventTimeWatermarkMs)
     }

     newState.foreach { v =>
       val encodedValue = stateTypesEncoder.encodeValue(v, expirationMs)
       if (isFirst) {
         store.put(encodedKey, encodedValue, stateName)
         isFirst = false
       } else {
          store.merge(encodedKey, encodedValue, stateName)
       }
     }
     ttlState.foreach(_.upsertTTLForStateKey(expirationMs,
       serializedGroupingKey))
   }

   /** Append an entry to the list. */
   override def appendValue(newState: S, ttlDuration: Duration = Duration.ZERO): Unit = {
     StateStoreErrors.requireNonNullStateValue(newState, stateName)
     var expirationMs: Long = -1
     if (ttlDuration != null && ttlDuration != Duration.ZERO) {
       expirationMs = StateTTL.calculateExpirationTimeForDuration(
         ttlMode, ttlDuration, batchTimestampMs, eventTimeWatermarkMs)
     }
     val encodedValue = stateTypesEncoder.encodeValue(newState, expirationMs)
     store.merge(stateTypesEncoder.encodeGroupingKey(),
       encodedValue, stateName)
     ttlState.foreach(_.upsertTTLForStateKey(expirationMs,
       stateTypesEncoder.serializeGroupingKey()))
   }

   /** Append an entire list to the existing value. */
   override def appendList(newState: Array[S], ttlDuration: Duration = Duration.ZERO): Unit = {
     validateNewState(newState)

     var expirationMs: Long = -1
     if (ttlDuration != null && ttlDuration != Duration.ZERO) {
       expirationMs = StateTTL.calculateExpirationTimeForDuration(
         ttlMode, ttlDuration, batchTimestampMs, eventTimeWatermarkMs)
     }

     val encodedKey = stateTypesEncoder.encodeGroupingKey()
     newState.foreach { v =>
       val encodedValue = stateTypesEncoder.encodeValue(v, expirationMs)
       store.merge(encodedKey, encodedValue, stateName)
     }
     ttlState.foreach(_.upsertTTLForStateKey(expirationMs,
       stateTypesEncoder.serializeGroupingKey()))
   }

   /** Remove this state. */
   override def clear(): Unit = {
     store.remove(stateTypesEncoder.encodeGroupingKey(), stateName)
   }

   private def validateNewState(newState: Array[S]): Unit = {
     StateStoreErrors.requireNonNullStateValue(newState, stateName)
     StateStoreErrors.requireNonEmptyListStateValue(newState, stateName)

     newState.foreach { v =>
       StateStoreErrors.requireNonNullStateValue(v, stateName)
     }
   }

  /**
   * Clears the user state associated with this grouping key
   * if it has expired. This function is called by Spark to perform
   * cleanup at the end of transformWithState processing.
   *
   * Spark uses a secondary index to determine if the user state for
   * this grouping key has expired. However, its possible that the user
   * has updated the TTL and secondary index is out of date. Implementations
   * must validate that the user State has actually expired before cleanup based
   * on their own State data.
   *
   * @param groupingKey grouping key for which cleanup should be performed.
   */
  override def clearIfExpired(groupingKey: Array[Byte]): Unit = {
    val encodedGroupingKey = stateTypesEncoder.encodeSerializedGroupingKey(groupingKey)
    val unsafeRowValuesIterator = store.valuesIterator(encodedGroupingKey, stateName)
    // We clear the list, and use the iterator to put back all of the non-expired values
    store.remove(encodedGroupingKey, stateName)
    var isFirst = true
    unsafeRowValuesIterator.foreach { encodedValue =>
      if (!isExpired(encodedValue)) {
        if (isFirst) {
          store.put(encodedGroupingKey, encodedValue, stateName)
          isFirst = false
        } else {
          store.merge(encodedGroupingKey, encodedValue, stateName)
        }
      }
    }
  }

  private def isExpired(valueRow: UnsafeRow): Boolean = {
    val expirationMs = stateTypesEncoder.decodeTtlExpirationMs(valueRow)
    val isExpired = expirationMs.map(
      StateTTL.isExpired(ttlMode, _, batchTimestampMs, eventTimeWatermarkMs))

    isExpired.isDefined && isExpired.get
  }

  /*
   * Internal methods to probe state for testing. The below methods exist for unit tests
   * to read the state ttl values, and ensure that values are persisted correctly in
   * the underlying  state store.
   */

  /**
   * Retrieves the value from State even if its expired. This method is used
   * in tests to read the state store value, and ensure if its cleaned up at the
   * end of the micro-batch.
   */
  private[sql] def getWithoutEnforcingTTL(): Iterator[S] = {
    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)
    new Iterator[S] {
      override def hasNext: Boolean = {
        unsafeRowValuesIterator.hasNext
      }
      override def next(): S = {
        val valueUnsafeRow = unsafeRowValuesIterator.next()
        stateTypesEncoder.decodeValue(valueUnsafeRow)
      }
    }
  }

  /**
   * Read the ttl value associated with the grouping key.
   */
  private[sql] def getTTLValues(): Iterator[Option[Long]] = {
    val encodedKey = stateTypesEncoder.encodeGroupingKey()
    val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)
    new Iterator[Option[Long]] {
      override def hasNext: Boolean = {
        unsafeRowValuesIterator.hasNext
      }

      override def next(): Option[Long] = {
        val valueUnsafeRow = unsafeRowValuesIterator.next()
        stateTypesEncoder.decodeTtlExpirationMs(valueUnsafeRow)
      }
    }
  }

  /**
   * Get all ttl values stored in ttl state for current implicit
   * grouping key.
   */
  private[sql] def getValuesInTTLState(): Iterator[Long] = {
    if (ttlState.isEmpty) {
      Iterator.empty
    }

    val ttlIterator = ttlState.get.iterator()
    val implicitGroupingKey = stateTypesEncoder.serializeGroupingKey()
    var nextValue: Option[Long] = None

    new Iterator[Long] {
      override def hasNext: Boolean = {
        while (nextValue.isEmpty && ttlIterator.hasNext) {
          val nextTtlValue = ttlIterator.next()
          val groupingKey = nextTtlValue.groupingKey

          if (groupingKey sameElements implicitGroupingKey) {
            nextValue = Some(nextTtlValue.expirationMs)
          }
        }

        nextValue.isDefined
      }

      override def next(): Long = {
        val result = nextValue.get
        nextValue = None

        result
      }
    }
  }
}
