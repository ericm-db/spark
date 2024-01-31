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


import java.util.UUID
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.hadoop.conf.Configuration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeoutMode}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, Utils}

/**
 * Physical operator for executing `TransformWithState`
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param statefulProcessor processor methods called on underlying data
 * @param timeoutMode defines the timeout mode
 * @param outputMode defines the output mode for the statefulProcessor
 * @param outputObjAttr Defines the output object
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param isStreaming defines whether the query is streaming or batch
 * @param child the physical plan for the underlying data
 */
case class TransformWithStateExec(
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    statefulProcessor: StatefulProcessor[Any, Any, Any],
    timeoutMode: TimeoutMode,
    outputMode: OutputMode,
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    isStreaming: Boolean = true,
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // TODO: update this to run no-data batches when timer support is added
  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = false

  override protected def withNewChildInternal(
    newChild: SparkPlan): TransformWithStateExec = copy(child = newChild)

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  protected val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  protected val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(groupingAttributes,
      getStateInfo, conf) ::
      Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow)  // convert key to objects
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val valueObjIter = valueRowIter.map(getValueObj.apply)
    val mappedIterator = statefulProcessor.handleInputRows(keyObj, valueObjIter,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForLateEvents)).map { obj =>
      getOutputRow(obj)
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()
    mappedIterator
  }

  private def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
    groupedIter.flatMap { case (keyRow, valueRowIter) =>
      val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
      handleInputRows(keyUnsafeRow, valueRowIter)
    }
  }

  private def processDataWithPartition(
      iter: Iterator[InternalRow],
      store: StateStore,
      processorHandle: StatefulProcessorHandleImpl):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForDataForLateEvents match {
      case Some(predicate) =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val outputIterator = processNewData(filteredIter)
    processorHandle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
    // Return an iterator of all the rows generated by all the keys, such that when fully
    // consumed, all the state updates will be committed by the state store
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
      // Note: Due to the iterator lazy execution, this metric also captures the time taken
      // by the upstream (consumer) operators in addition to the processing in this operator.
      allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
      commitTimeMs += timeTakenMs {
        store.commit()
      }
      setStoreMetrics(store)
      setOperatorMetrics()
      statefulProcessor.close()
      processorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
    })
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    if (isStreaming) {
      child.execute().mapPartitionsWithStateStore[InternalRow](
        getStateInfo,
        schemaForKeyRow,
        schemaForValueRow,
        numColsPrefixKey = 0,
        session.sqlContext.sessionState,
        Some(session.sqlContext.streams.stateStoreCoordinator),
        useColumnFamilies = true
      ) {
        case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
          val processorHandle = new StatefulProcessorHandleImpl(
            store, getStateInfo.queryRunId, isStreaming)
          assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
          statefulProcessor.init(processorHandle, outputMode)
          processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          val result = processDataWithPartition(singleIterator, store, processorHandle)
          result
      }
    } else {
      child.execute().mapPartitions[InternalRow](
        iter => {
          val sqlConf = new SQLConf()
          sqlConf.setConf(SQLConf.NUM_STATE_STORE_MAINTENANCE_THREADS, 1)
          sqlConf.setConf(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT, 1)
          sqlConf.setConf(SQLConf.MIN_BATCHES_TO_RETAIN, 1)
          sqlConf.setConf(SQLConf.STATE_STORE_PROVIDER_CLASS,
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
          sqlConf.setConf(SQLConf.STATE_STORE_FORMAT_VALIDATION_ENABLED, false)
          sqlConf.setConf(SQLConf.STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS, false)
          sqlConf.setConf(SQLConf.STATE_STORE_COMPRESSION_CODEC, "lz4")
          sqlConf.setConf(SQLConf.STATE_SCHEMA_CHECK_ENABLED, false)
          sqlConf.setConf(SQLConf.STREAMING_MAINTENANCE_INTERVAL, 1000L)

          val providerId = new StateStoreProviderId(
            StateStoreId(Utils.createTempDir().getAbsolutePath, 0, 0), getStateInfo.queryRunId)

          val stateStoreProvider = StateStoreProvider.createAndInit(
            providerId,
            schemaForKeyRow,
            schemaForValueRow,
            numColsPrefixKey = 0,
            useColumnFamilies = true,
            storeConf = StateStoreConf(sqlConf),
            hadoopConf = new Configuration())

          val store = stateStoreProvider.getStore(0)
          val processorHandle =
            new StatefulProcessorHandleImpl(store, UUID.randomUUID(), isStreaming)
          assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
          statefulProcessor.init(processorHandle, outputMode)
          processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
          val result = processDataWithPartition(iter, null, processorHandle)
          result
        }
      )
    }
  }
}


object TransformWithStateExec {

  // Plan logical transformWithState for batch queries
  def generateSparkPlanForBatchQueries(
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      statefulProcessor: StatefulProcessor[Any, Any, Any],
      timeoutMode: TimeoutMode,
      outputMode: OutputMode,
      outputObjAttr: Attribute,
      child: SparkPlan): SparkPlan = {
    val shufflePartitions = child.session.sessionState.conf.numShufflePartitions
    val statefulOperatorStateInfo = StatefulOperatorStateInfo(
      Utils.createTempDir().getAbsolutePath,
      queryRunId = UUID.randomUUID(),
      operatorId = 0,
      storeVersion = 0,
      numPartitions = shufflePartitions
    )

    new TransformWithStateExec(
      keyDeserializer,
      valueDeserializer,
      groupingAttributes,
      dataAttributes,
      statefulProcessor,
      timeoutMode,
      outputMode,
      outputObjAttr,
      Some(statefulOperatorStateInfo),
      Some(System.currentTimeMillis),
      None,
      None,
      isStreaming = false,
      child)
  }
}
