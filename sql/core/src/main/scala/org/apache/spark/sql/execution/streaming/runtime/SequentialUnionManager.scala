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

package org.apache.spark.sql.execution.streaming.runtime

import org.apache.spark.sql.connector.read.streaming.{Offset => OffsetV2, SparkDataStream, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.checkpointing.SequentialUnionOffset

/**
 * Manages the state and transitions for Sequential Union streaming queries.
 *
 * Sequential Union processes multiple streaming sources one at a time, completing each source
 * before transitioning to the next. This enables backfill-to-live streaming scenarios where
 * historical data from bounded sources is processed first, followed by live unbounded sources,
 * while preserving all stateful operator state across transitions.
 *
 * Key responsibilities:
 * - Track which source is currently active
 * - Determine when to transition between sources
 * - Filter offset collection (return None for inactive sources)
 * - Generate metadata for offset log persistence
 * - Restore state from checkpoint on query restart
 *
 * Thread-safety: This class is not thread-safe and should be used only from the streaming
 * execution thread.
 *
 * @param sourceNames The ordered list of all source names in the sequential union
 * @param initialActiveSource The initially active source (defaults to first source)
 * @param initialCompletedSources The set of sources that have already completed
 */
class SequentialUnionManager(
    sourceNames: Seq[String],
    initialActiveSource: Option[String] = None,
    initialCompletedSources: Set[String] = Set.empty) {

  /**
   * The name of the currently active source.
   * Only this source will have offsets collected and produce data in each batch.
   */
  private var activeSourceName: String = initialActiveSource.getOrElse {
    require(sourceNames.nonEmpty, "sourceNames cannot be empty")
    sourceNames.head
  }

  /**
   * The set of source names that have completed processing.
   * Once a source completes, it will never be active again.
   */
  private var completedSources: Set[String] = initialCompletedSources

  /**
   * Checks if a source should participate in the current batch.
   *
   * Only the currently active source should participate; all other sources are inactive.
   * Inactive sources will not have offsets collected and will produce empty data.
   *
   * @param sourceName The name of the source to check
   * @return true if the source is currently active, false otherwise
   */
  def isSourceActive(sourceName: String): Boolean = {
    sourceName == activeSourceName
  }

  /**
   * Checks if the active source has completed based on its offsets.
   *
   * A source is considered complete when:
   * 1. It is the currently active source (inactive sources cannot complete)
   * 2. It is not the final source (the final source never completes)
   * 3. For sources with SupportsTriggerAvailableNow: startOffset == endOffset
   *    (no offset progression means caught up to target)
   *
   * The completion detection leverages the SupportsTriggerAvailableNow interface:
   * - prepareForTriggerAvailableNow() is called once at query start to set the target offset
   * - When startOffset == endOffset, the source has caught up to the target and is complete
   *
   * @param sourceName The name of the source to check
   * @param startOffset The starting offset for this batch
   * @param endOffset The ending offset for this batch
   * @param source The SparkDataStream instance for this source
   * @return true if the source is complete and ready for transition, false otherwise
   */
  def isSourceComplete(
      sourceName: String,
      startOffset: OffsetV2,
      endOffset: OffsetV2,
      source: SparkDataStream): Boolean = {

    // Only the active source can be complete
    if (sourceName != activeSourceName) {
      return false
    }

    // Check if this is the final source (never completes)
    val sourceIndex = sourceNames.indexOf(sourceName)
    if (sourceIndex == sourceNames.length - 1) {
      return false
    }

    // For sources with SupportsTriggerAvailableNow:
    // Complete when no offset progression (caught up to target)
    source match {
      case _: SupportsTriggerAvailableNow =>
        startOffset == endOffset
      case _ =>
        // Should not happen (validation prevents this)
        throw new IllegalStateException(
          s"Source $sourceName must implement SupportsTriggerAvailableNow " +
          s"for Sequential Union. Only the final source can be unbounded.")
    }
  }

  /**
   * Advances to the next source in the sequence.
   *
   * Marks the current active source as completed and activates the next source.
   * If all sources are complete, returns None.
   *
   * @return Some(nextSourceName) if there is a next source, None if all sources are complete
   */
  def advanceToNextSource(): Option[String] = {
    // Mark current source as completed
    completedSources = completedSources + activeSourceName

    // Find next source
    val currentIndex = sourceNames.indexOf(activeSourceName)
    val nextIndex = currentIndex + 1

    if (nextIndex < sourceNames.length) {
      val nextSource = sourceNames(nextIndex)
      activeSourceName = nextSource
      Some(nextSource)
    } else {
      // All sources complete
      None
    }
  }

  /**
   * Gets the current state for persisting to the offset log.
   *
   * This offset is written to the offset log with each batch and enables
   * crash recovery by restoring the exact state of source transitions.
   *
   * @return SequentialUnionOffset containing current state
   */
  def getOffset(): SequentialUnionOffset = {
    SequentialUnionOffset(
      activeSourceName = activeSourceName,
      completedSources = completedSources,
      sourceNames = sourceNames
    )
  }

  /**
   * Restores state from checkpoint offset.
   *
   * This is called when restarting a query from a checkpoint to restore the exact
   * state of which source is active and which sources have completed.
   *
   * @param offset The offset to restore from
   */
  def restore(offset: SequentialUnionOffset): Unit = {
    this.activeSourceName = offset.activeSourceName
    this.completedSources = offset.completedSources
    // Note: sourceNames from offset should match constructor sourceNames
    // Validation of this happens in the caller
  }

  /**
   * Checks if all bounded sources have finished processing.
   *
   * Returns true when all non-final (bounded) sources have completed and we're now on the final
   * unbounded source. This indicates we've successfully transitioned through all bounded
   * sources and are now processing the live unbounded source.
   *
   * @return true if all bounded sources are complete and we're on the final unbounded source
   */
  def boundedSourcesComplete: Boolean = {
    val finalSourceIndex = sourceNames.length - 1
    val onFinalSource = sourceNames.indexOf(activeSourceName) == finalSourceIndex
    val allNonFinalComplete = completedSources.size == finalSourceIndex

    onFinalSource && allNonFinalComplete
  }
}
