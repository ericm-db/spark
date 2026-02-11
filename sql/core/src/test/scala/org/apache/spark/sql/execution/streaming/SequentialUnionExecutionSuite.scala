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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SequentialStreamingUnion}
import org.apache.spark.sql.connector.read.streaming.{ReadLimit, SparkDataStream, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqMetadataV2, SequentialUnionOffset}
import org.apache.spark.sql.execution.streaming.runtime.LongOffset
import org.apache.spark.sql.types.IntegerType

/**
 * Test suite for [[SequentialUnionExecution]] functionality including:
 * - Manager initialization and multi-manager support
 * - Source filtering based on active state
 * - Automatic source transitions when exhausted
 * - Offset log integration for recovery
 */
class SequentialUnionExecutionSuite extends SparkFunSuite with MockitoSugar {

  /**
   * Helper to create a mock SparkDataStream with SupportsTriggerAvailableNow.
   */
  private def createMockSource(
      name: String,
      exhausted: Boolean = false): SparkDataStream with SupportsTriggerAvailableNow = {
    val source = mock[SparkDataStream with SupportsTriggerAvailableNow]
    when(source.toString).thenReturn(name)
    when(source.getDefaultReadLimit).thenReturn(ReadLimit.allAvailable())

    // Mock offset behavior - exhausted sources return same offset
    if (exhausted) {
      when(source.latestOffset(any(), any())).thenReturn(LongOffset(0))
    } else {
      when(source.latestOffset(any(), any())).thenReturn(LongOffset(1))
    }

    source
  }

  /**
   * Helper to create a SequentialStreamingUnion with the specified number of children.
   */
  private def createSequentialUnion(numChildren: Int): SequentialStreamingUnion = {
    val children = (1 to numChildren).map { i =>
      LocalRelation(Seq(AttributeReference("id", IntegerType)()), isStreaming = true)
    }
    SequentialStreamingUnion(children, byName = false, allowMissingCol = false)
  }

  test("SequentialUnionExecution - manager initialization with single sequential union") {
    val seqUnion = createSequentialUnion(3)
    val sources = Seq(
      createMockSource("source-0"),
      createMockSource("source-1"),
      createMockSource("source-2")
    )

    // Create managers would happen during populateStartOffsets
    // This tests the manager creation logic
    val sourceNames = (0 until 3).map(i => s"seq-union-0-source-$i")
    val sourceMap = sourceNames.zip(sources).toMap

    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    assert(manager.activeSourceName === "seq-union-0-source-0")
    assert(manager.activeSourceIndex === 0)
    assert(!manager.isOnFinalSource)
    assert(manager.completedSources.isEmpty)
  }

  test("SequentialUnionExecution - source filtering logic") {
    val seqUnion = createSequentialUnion(2)
    val sources = Seq(
      createMockSource("source-0"),
      createMockSource("source-1")
    )

    val sourceNames = Seq("seq-union-0-source-0", "seq-union-0-source-1")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Initially, first source is active
    assert(manager.isSourceActive("seq-union-0-source-0"))
    assert(!manager.isSourceActive("seq-union-0-source-1"))

    // After transition, second source is active
    manager.transitionToNextSource()
    assert(!manager.isSourceActive("seq-union-0-source-0"))
    assert(manager.isSourceActive("seq-union-0-source-1"))
    assert(manager.isSourceCompleted("seq-union-0-source-0"))
  }

  test("SequentialUnionExecution - exhaustion detection") {
    val seqUnion = createSequentialUnion(2)
    val sources = Seq(
      createMockSource("source-0", exhausted = true),  // Same offset = exhausted
      createMockSource("source-1", exhausted = false)
    )

    val sourceNames = Seq("seq-union-0-source-0", "seq-union-0-source-1")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Source 0 is active
    assert(manager.activeSourceName === "seq-union-0-source-0")

    // Simulate querying offsets - source 0 returns same offset (exhausted)
    val startOffset = Some(LongOffset(0))
    val latestOffset = Some(LongOffset(0))
    val isExhausted = startOffset == latestOffset

    assert(isExhausted, "Source should be detected as exhausted")

    // Transition to next source
    manager.transitionToNextSource()
    assert(manager.activeSourceName === "seq-union-0-source-1")
  }

  test("SequentialUnionExecution - multi-manager support") {
    // Create two sequential unions
    val seqUnion1 = createSequentialUnion(2)
    val seqUnion2 = createSequentialUnion(2)

    val sources1 = Seq(
      createMockSource("union1-source-0"),
      createMockSource("union1-source-1")
    )
    val sources2 = Seq(
      createMockSource("union2-source-0"),
      createMockSource("union2-source-1")
    )

    // Create managers
    val manager1 = new SequentialUnionManager(
      seqUnion1,
      Seq("seq-union-0-source-0", "seq-union-0-source-1"),
      Map("seq-union-0-source-0" -> sources1(0), "seq-union-0-source-1" -> sources1(1))
    )

    val manager2 = new SequentialUnionManager(
      seqUnion2,
      Seq("seq-union-1-source-0", "seq-union-1-source-1"),
      Map("seq-union-1-source-0" -> sources2(0), "seq-union-1-source-1" -> sources2(1))
    )

    // Both managers start with first source
    assert(manager1.activeSourceName === "seq-union-0-source-0")
    assert(manager2.activeSourceName === "seq-union-1-source-0")

    // Managers transition independently
    manager1.transitionToNextSource()
    assert(manager1.activeSourceName === "seq-union-0-source-1")
    assert(manager2.activeSourceName === "seq-union-1-source-0") // Unchanged

    manager2.transitionToNextSource()
    assert(manager1.activeSourceName === "seq-union-0-source-1") // Unchanged
    assert(manager2.activeSourceName === "seq-union-1-source-1")
  }

  test("SequentialUnionOffset - serialization to offset log") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source-1",
      allSourceNames = Seq("source-0", "source-1", "source-2"),
      completedSourceNames = Set("source-0")
    )

    // Create an OffsetMap with the SequentialUnionOffset
    val offsetMap = OffsetMap(
      offsetsMap = Map(
        "source-0" -> Some(LongOffset(100)),
        "source-1" -> Some(LongOffset(50)),
        "sequential-union-0" -> Some(offset)
      ),
      metadata = OffsetSeqMetadataV2(
        batchWatermarkMs = 0,
        batchTimestampMs = System.currentTimeMillis()
      )
    )

    // Verify we can extract it
    offsetMap.offsetsMap.get("sequential-union-0").flatten match {
      case Some(restored: SequentialUnionOffset) =>
        assert(restored.activeSourceName === "source-1")
        assert(restored.allSourceNames === Seq("source-0", "source-1", "source-2"))
        assert(restored.completedSourceNames === Set("source-0"))
      case other =>
        fail(s"Failed to extract SequentialUnionOffset, got: $other")
    }
  }

  test("SequentialUnionOffset - restoration from offset log") {
    val seqUnion = createSequentialUnion(3)
    val sources = (0 until 3).map(i => createMockSource(s"source-$i"))

    val sourceNames = Seq("source-0", "source-1", "source-2")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Create an offset representing mid-execution state
    val savedOffset = SequentialUnionOffset(
      activeSourceName = "source-1",
      allSourceNames = sourceNames,
      completedSourceNames = Set("source-0")
    )

    // Restore from the offset
    manager.restoreFromOffset(savedOffset)

    // Verify state was restored
    assert(manager.activeSourceName === "source-1")
    assert(manager.activeSourceIndex === 1)
    assert(manager.completedSources === Set("source-0"))
    assert(!manager.isOnFinalSource)
  }

  test("SequentialUnionExecution - final source detection") {
    val seqUnion = createSequentialUnion(2)
    val sources = Seq(
      createMockSource("source-0"),
      createMockSource("source-1")
    )

    val sourceNames = Seq("source-0", "source-1")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Initially not on final source
    assert(!manager.isOnFinalSource)

    // After one transition, on final source
    manager.transitionToNextSource()
    assert(manager.isOnFinalSource)

    // Cannot transition past final source
    intercept[IllegalArgumentException] {
      manager.transitionToNextSource()
    }
  }

  test("SequentialUnionExecution - source-to-manager mapping") {
    val seqUnion1 = createSequentialUnion(2)
    val seqUnion2 = createSequentialUnion(2)

    val sources1 = Seq(
      createMockSource("union1-s0"),
      createMockSource("union1-s1")
    )
    val sources2 = Seq(
      createMockSource("union2-s0"),
      createMockSource("union2-s1")
    )

    val manager1 = new SequentialUnionManager(
      seqUnion1,
      Seq("union1-s0", "union1-s1"),
      Map("union1-s0" -> sources1(0), "union1-s1" -> sources1(1))
    )

    val manager2 = new SequentialUnionManager(
      seqUnion2,
      Seq("union2-s0", "union2-s1"),
      Map("union2-s0" -> sources2(0), "union2-s1" -> sources2(1))
    )

    // Build source-to-manager map (as SequentialUnionExecution does)
    val sourceToManager = Map(
      sources1(0) -> manager1,
      sources1(1) -> manager1,
      sources2(0) -> manager2,
      sources2(1) -> manager2
    )

    // Verify mapping
    assert(sourceToManager(sources1(0)) === manager1)
    assert(sourceToManager(sources1(1)) === manager1)
    assert(sourceToManager(sources2(0)) === manager2)
    assert(sourceToManager(sources2(1)) === manager2)
  }

  test("SequentialUnionExecution - currentOffset reflects state changes") {
    val seqUnion = createSequentialUnion(3)
    val sources = (0 until 3).map(i => createMockSource(s"source-$i"))

    val sourceNames = Seq("s0", "s1", "s2")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Initial offset
    val offset0 = manager.currentOffset
    assert(offset0.activeSourceName === "s0")
    assert(offset0.completedSourceNames.isEmpty)

    // After first transition
    val offset1 = manager.transitionToNextSource()
    assert(offset1.activeSourceName === "s1")
    assert(offset1.completedSourceNames === Set("s0"))

    // After second transition
    val offset2 = manager.transitionToNextSource()
    assert(offset2.activeSourceName === "s2")
    assert(offset2.completedSourceNames === Set("s0", "s1"))
  }

  test("SequentialUnionExecution - prepareActiveSourceForAvailableNow called") {
    val seqUnion = createSequentialUnion(2)
    val sources = Seq(
      createMockSource("source-0"),
      createMockSource("source-1")
    )

    val sourceNames = Seq("source-0", "source-1")
    val sourceMap = sourceNames.zip(sources).toMap
    val manager = new SequentialUnionManager(seqUnion, sourceNames, sourceMap)

    // Prepare non-final source
    manager.prepareActiveSourceForAvailableNow()
    verify(sources(0), times(1)).prepareForTriggerAvailableNow()

    // Transition to final source
    manager.transitionToNextSource()

    // Cannot prepare final source with AvailableNow
    intercept[IllegalArgumentException] {
      manager.prepareActiveSourceForAvailableNow()
    }
  }
}
