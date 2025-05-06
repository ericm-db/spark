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

import scala.reflect.ClassTag

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.classic.ClassicConversions.castToImpl
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.TaskFailureListener

package object state {

  implicit class StateStoreOps[T: ClassTag](dataRDD: RDD[T]) {

    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    def mapPartitionsWithStateStore[U: ClassTag](
        sqlContext: SQLContext,
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec)(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      mapPartitionsWithStateStore(
        stateInfo,
        keySchema,
        valueSchema,
        keyStateEncoderSpec,
        sqlContext.sparkSession.sessionState,
        Some(castToImpl(sqlContext.sparkSession).streams.stateStoreCoordinator))(
        storeUpdateFunction)
    }

    // Disable scala style because num parameters exceeds the max limit used to enforce scala style
    // scalastyle:off
    /** Map each partition of an RDD along with data in a [[StateStore]]. */
    def mapPartitionsWithStateStore[U: ClassTag](
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        sessionState: SessionState,
        storeCoordinator: Option[StateStoreCoordinatorRef],
        useColumnFamilies: Boolean = false,
        extraOptions: Map[String, String] = Map.empty,
        useMultipleValuesPerKey: Boolean = false)(
        storeUpdateFunction: (StateStore, Iterator[T]) => Iterator[U]): StateStoreRDD[T, U] = {

      val cleanedF = dataRDD.sparkContext.clean(storeUpdateFunction)
      val wrappedF = (store: StateStore, iter: Iterator[T]) => {
        // Do not add CompletionListener here to clean up the state store because
        // it is already added in RocksDBStateStore/HDFSBackedStateStore.
        cleanedF(store, iter)
      }

      new StateStoreRDD(
        dataRDD,
        wrappedF,
        stateInfo.checkpointLocation,
        stateInfo.queryRunId,
        stateInfo.operatorId,
        stateInfo.storeVersion,
        stateInfo.stateStoreCkptIds,
        stateInfo.stateSchemaMetadata,
        keySchema,
        valueSchema,
        keyStateEncoderSpec,
        sessionState,
        storeCoordinator,
        useColumnFamilies,
        extraOptions,
        useMultipleValuesPerKey)
    }
    // scalastyle:on

    /** Map each partition of an RDD along with data in a [[ReadStateStore]]. */
    private[streaming] def mapPartitionsWithReadStateStore[U: ClassTag](
        stateInfo: StatefulOperatorStateInfo,
        keySchema: StructType,
        valueSchema: StructType,
        keyStateEncoderSpec: KeyStateEncoderSpec,
        sessionState: SessionState,
        storeCoordinator: Option[StateStoreCoordinatorRef],
        useColumnFamilies: Boolean = false,
        extraOptions: Map[String, String] = Map.empty)(
        storeReadFn: (ReadStateStore, Iterator[T]) => Iterator[U])
      : ReadStateStoreRDD[T, U] = {

      val cleanedF = dataRDD.sparkContext.clean(storeReadFn)
      val wrappedF = (store: ReadStateStore, iter: Iterator[T]) => {
        // Do not call abort/release here to clean up the state store because
        // it is already added in RocksDBStateStore/HDFSBackedStateStore.
        // However, we still do need to clear the store from the StateStoreThreadLocalTracker.
        TaskContext.get().addTaskCompletionListener[Unit](_ => {
          StateStoreThreadLocalTracker.clearStore()
        })
        cleanedF(store, iter)
      }
      new ReadStateStoreRDD(
        dataRDD,
        wrappedF,
        stateInfo.checkpointLocation,
        stateInfo.queryRunId,
        stateInfo.operatorId,
        stateInfo.storeVersion,
        stateInfo.stateStoreCkptIds,
        stateInfo.stateSchemaMetadata,
        keySchema,
        valueSchema,
        keyStateEncoderSpec,
        sessionState,
        storeCoordinator,
        useColumnFamilies,
        extraOptions)
    }
  }
}
