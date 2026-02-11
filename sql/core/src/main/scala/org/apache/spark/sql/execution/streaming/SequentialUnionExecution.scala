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
import org.apache.spark.sql.catalyst.plans.logical.SequentialStreamingUnion
import org.apache.spark.sql.catalyst.streaming.WriteToStream
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, SequentialUnionOffset}
import org.apache.spark.sql.execution.streaming.runtime.{MicroBatchExecution, MicroBatchExecutionContext}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.util.Clock

/**
 * Execution engine for streaming queries containing [[SequentialStreamingUnion]] nodes.
 *
 * This execution layer supports queries with multiple SequentialStreamingUnion nodes,
 * each processing its sources sequentially while running concurrently with other
 * sequential unions and regular sources.
 *
 * Example:
 * {{{
 * val seq1 = historical1.followedBy(live1)  // Sequential union 1
 * val seq2 = historical2.followedBy(live2)  // Sequential union 2
 * val regular = spark.readStream...          // Regular source
 *
 * seq1.union(seq2).union(regular)  // All run concurrently
 *   .groupBy("key").count()
 *   .writeStream.start()
 * }}}
 *
 * Execution Model:
 * - Each SequentialStreamingUnion has its own [[SequentialUnionManager]]
 * - Managers independently track which source is active within their sequential union
 * - During batch construction, inactive sources are filtered out
 * - When a source is exhausted (latestOffset == startOffset), transition to next in that union
 * - Normal MicroBatchExecution handles the overall execution loop
 *
 * Recovery:
 * - Each sequential union's [[SequentialUnionOffset]] is persisted in the offset log
 * - On restart, managers restore their state from the offset log
 * - Completed sources are skipped, active sources resume
 *
 * @param sparkSession The SparkSession for this execution
 * @param trigger The trigger for the query
 * @param triggerClock Clock for trigger execution
 * @param extraOptions Additional options for the query
 * @param plan The WriteToStream plan containing SequentialStreamingUnion nodes
 */
class SequentialUnionExecution(
    sparkSession: SparkSession,
    trigger: Trigger,
    triggerClock: Clock,
    extraOptions: Map[String, String],
    plan: WriteToStream)
  extends MicroBatchExecution(sparkSession, trigger, triggerClock, extraOptions, plan)
  with Logging {

  // Multi-manager state: one manager per SequentialStreamingUnion in the plan
  private var managers: Seq[SequentialUnionManager] = Seq.empty

  // Mapping from source to its manager (if it belongs to a sequential union)
  // Sources not in this map are regular sources (always queried)
  private var sourceToManager: Map[SparkDataStream, SequentialUnionManager] = Map.empty

  // Mapping from manager to its SequentialUnionOffset name (for offset log)
  private var managerToOffsetName: Map[SequentialUnionManager, String] = Map.empty

  /**
   * Initialize sequential union managers for all SequentialStreamingUnion nodes in the plan.
   * This is called during query startup, after the plan has been analyzed.
   */
  override protected def populateStartOffsets(
      execCtx: MicroBatchExecutionContext,
      sparkSessionToRunBatches: SparkSession): Unit = {

    // First, do the normal source discovery and offset population
    super.populateStartOffsets(execCtx, sparkSessionToRunBatches)

    // Find all SequentialStreamingUnion nodes in the plan
    val sequentialUnions = logicalPlan.collect {
      case su: SequentialStreamingUnion => su
    }

    if (sequentialUnions.isEmpty) {
      throw new IllegalStateException(
        "SequentialUnionExecution requires at least one SequentialStreamingUnion in the plan")
    }

    logInfo(s"Found ${sequentialUnions.size} SequentialStreamingUnion(s) in the query plan")

    // Create a manager for each sequential union
    val managersWithMeta = sequentialUnions.zipWithIndex.map { case (seqUnion, index) =>
      createManagerForSequentialUnion(seqUnion, index, execCtx)
    }

    managers = managersWithMeta.map(_._1)
    sourceToManager = managersWithMeta.flatMap(_._2).toMap
    managerToOffsetName = managersWithMeta.map { case (mgr, _, name) => mgr -> name }.toMap

    logInfo(
      s"SequentialUnionExecution initialized with ${managers.size} manager(s), " +
        s"managing ${sourceToManager.size} source(s)")
  }

  /**
   * Create a manager for a single SequentialStreamingUnion.
   * Returns (manager, sourceToManagerMap, offsetName).
   */
  private def createManagerForSequentialUnion(
      seqUnion: SequentialStreamingUnion,
      unionIndex: Int,
      execCtx: MicroBatchExecutionContext):
      (SequentialUnionManager, Map[SparkDataStream, SequentialUnionManager], String) = {

    // TODO: Extract actual source names from the plan (via .name() API)
    // For now, use sequential-union-{index}-source-{sourceIndex}
    val sourceNames = (0 until seqUnion.children.size).map { i =>
      s"seq-union-$unionIndex-source-$i"
    }

    // Map source names to actual sources
    // TODO: This is a simplified mapping - need to properly identify which sources
    // belong to which sequential union based on the logical plan structure
    val sourcesForThisUnion = sources.take(seqUnion.children.size)
    val sourceMap = sourceNames.zip(sourcesForThisUnion).toMap

    // Create the manager
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Offset name for this sequential union (used in offset log)
    val offsetName = s"sequential-union-$unionIndex"

    // Restore from checkpoint if available
    restoreSequentialUnionOffset(execCtx, offsetName).foreach { offset =>
      manager.restoreFromOffset(offset)
      logInfo(
        s"Restored sequential union $unionIndex from checkpoint: " +
          s"active=${offset.activeSourceName}, completed=${offset.completedSourceNames.size}")
    }

    // Build source-to-manager map for this union
    val sourceToMgrMap = sourcesForThisUnion.map(_ -> manager).toMap

    logInfo(
      s"Created manager for sequential union $unionIndex: " +
        s"${sourceNames.size} sources, active=${manager.activeSourceName}")

    (manager, sourceToMgrMap, offsetName)
  }

  /**
   * Restore the SequentialUnionOffset for a specific sequential union from the offset log.
   * Returns None if this is a new query (no checkpoint).
   *
   * @param execCtx The execution context
   * @param offsetName The name/key for this sequential union's offset in the offset log
   */
  private def restoreSequentialUnionOffset(
      execCtx: MicroBatchExecutionContext,
      offsetName: String): Option[SequentialUnionOffset] = {

    // Get the latest offset log entry (if it exists)
    offsetLog.getLatest() match {
      case Some((batchId, offsetSeqBase)) =>
        // Extract SequentialUnionOffset from the offset log
        offsetSeqBase match {
          case offsetMap: OffsetMap =>
            // VERSION_2 - look up by name in the map
            offsetMap.offsetsMap.get(offsetName).flatten match {
              case Some(offset: SequentialUnionOffset) =>
                logInfo(
                  s"Restored $offsetName from batch $batchId: " +
                    s"active=${offset.activeSourceName}, " +
                    s"completed=${offset.completedSourceNames.size}")
                Some(offset)
              case Some(other) =>
                logWarning(s"Found offset for $offsetName but wrong type: ${other.getClass}")
                None
              case None =>
                // Not found - this is the first time running with sequential union
                logInfo(s"No checkpoint found for $offsetName, starting from beginning")
                None
            }
          case other =>
            // VERSION_1 or other - not supported
            logWarning(s"Cannot restore SequentialUnionOffset from VERSION_1 offset log. " +
              s"Found version: ${other.version}")
            None
        }
      case None =>
        // No checkpoint - new query
        logInfo("No offset log found, starting new query")
        None
    }
  }

  /**
   * Check if a source should be queried for offsets in this batch.
   * Sources that belong to a sequential union are only queried if they are the active source.
   *
   * @param source The source to check
   * @return true if the source should be queried, false otherwise
   */
  private def shouldQuerySource(source: SparkDataStream): Boolean = {
    sourceToManager.get(source) match {
      case Some(manager) =>
        // This source belongs to a sequential union - only query if it's active
        val sourceId = sourceToIdMap.getOrElse(source, source.toString)
        val isActive = manager.isSourceActive(sourceId)
        if (!isActive) {
          logDebug(s"Skipping inactive source in sequential union: $source")
        }
        isActive
      case None =>
        // Regular source - always query
        true
    }
  }

  /**
   * Detect if a source has been exhausted (no new data) and handle transition if needed.
   * Called after querying offsets to check if we need to transition to the next source.
   *
   * @param source The source that was just queried
   * @param startOffset The start offset for this source
   * @param latestOffset The latest offset returned by the source
   */
  private def handleSourceExhaustion(
      source: SparkDataStream,
      startOffset: Option[OffsetV2],
      latestOffset: Option[OffsetV2]): Unit = {

    sourceToManager.get(source).foreach { manager =>
      // Check if this source is exhausted (latestOffset == startOffset, meaning no new data)
      val isExhausted = (startOffset, latestOffset) match {
        case (Some(start), Some(latest)) => start == latest
        case (None, None) => true
        case _ => false
      }

      if (isExhausted && !manager.isOnFinalSource) {
        // This source is exhausted and we're not on the final source - transition!
        logInfo(s"Source ${manager.activeSourceName} exhausted, transitioning to next source")
        val newOffset = manager.transitionToNextSource()

        // TODO: Record the transition in the offset log
        // This would write a SequentialUnionOffset entry marking the transition

        logInfo(
          s"Transitioned to ${manager.activeSourceName}, " +
            s"completed sources: ${manager.completedSources.size}")
      }
    }
  }

  /**
   * Override batch construction to filter sources based on sequential union state.
   * Only active sources in each sequential union are queried for offsets.
   *
   * After querying offsets, we check for source exhaustion and transition to the next
   * source in the sequential union if needed.
   */
  override protected def constructNextBatch(
      execCtx: MicroBatchExecutionContext,
      noDataBatchesEnabled: Boolean): Boolean = {

    // Filter uniqueSources to only include sources that should be queried
    val filteredSources = uniqueSources.filter { case (source, _) =>
      shouldQuerySource(source)
    }

    if (filteredSources.size != uniqueSources.size) {
      logDebug(
        s"Filtered sources from ${uniqueSources.size} to ${filteredSources.size} " +
          s"based on sequential union active state")
    }

    // Temporarily replace uniqueSources with filtered sources for this batch
    val originalSources = uniqueSources
    uniqueSources = filteredSources

    // Call parent to query offsets and construct the batch
    val result = try {
      super.constructNextBatch(execCtx, noDataBatchesEnabled)
    } finally {
      // Always restore original sources (important for next batch)
      uniqueSources = originalSources
    }

    // After querying offsets, check each source for exhaustion and handle transitions
    filteredSources.keys.foreach { source =>
      val startOffset = execCtx.startOffsets.get(source)
      val endOffset = execCtx.endOffsets.get(source)
      handleSourceExhaustion(source, startOffset, endOffset)
    }

    result
  }

  /**
   * Override to inject SequentialUnionOffsets into the offset log alongside source offsets.
   */
  override protected def markMicroBatchStart(execCtx: MicroBatchExecutionContext): Unit = {
    // Get the base offsets from parent (source offsets)
    val baseOffsets = execCtx.endOffsets.toOffsets(sources, sourceIdMap, execCtx.offsetSeqMetadata)

    // Add SequentialUnionOffsets to the offset log
    val offsetsWithSequentialUnion = baseOffsets match {
      case offsetMap: OffsetMap =>
        // VERSION_2 - add our offsets to the map
        val updatedOffsetsMap = offsetMap.offsetsMap ++ managers.map { manager =>
          val offsetName = managerToOffsetName(manager)
          val seqUnionOffset = manager.currentOffset
          offsetName -> Some(seqUnionOffset)
        }.toMap

        OffsetMap(updatedOffsetsMap, offsetMap.metadata)

      case other =>
        // VERSION_1 or other - just use as-is
        // TODO: Support VERSION_1 if needed
        logWarning("SequentialUnionOffset storage requires VERSION_2 offset log format. " +
          s"Current format: ${other.version}")
        other
    }

    // Write to offset log
    if (!offsetLog.add(execCtx.batchId, offsetsWithSequentialUnion)) {
      throw new IllegalStateException(
        s"Concurrent update to the log. Multiple streaming jobs detected for ${execCtx.batchId}")
    }

    logInfo(s"Committed offsets for batch ${execCtx.batchId} including " +
      s"${managers.size} SequentialUnionOffset(s)")
  }

  // Note: We override constructNextBatch to filter sources dynamically.
  // The normal MicroBatchExecution loop handles the rest of execution.
}
