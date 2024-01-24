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
package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, TransformWithStateExec}
import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.atomic.AtomicInteger

// Create batch equivalent of StateOpIdRule for streaming queries

object StateOpIdBatchRule extends Rule[SparkPlan] {

  private[sql] val numStateStores = offsetSeqMetadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key)
    .map(SQLConf.SHUFFLE_PARTITIONS.valueConverter)
    .getOrElse(sparkSession.sessionState.conf.numShufflePartitions)

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private val statefulOperatorId = new AtomicInteger(0)
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(
      checkpointLocation,
      runId,
      statefulOperatorId.getAndIncrement(),
      currentBatchId,
      numStateStores)
  }
  override def apply(plan: SparkPlan): SparkPlan = {
    case t: TransformWithStateExec =>
      // get numShufflePartitions from SQLConf

      val numShufflePartitions = plan.session.sessionState.conf.numShufflePartitions

      t.copy(
        stateInfo = Some(nextStatefulOperationStateInfo()),
        batchTimestampMs = t.batchTimestampMs,
        eventTimeWatermarkForLateEvents = None,
        eventTimeWatermarkForEviction = None
      )
  }
}
