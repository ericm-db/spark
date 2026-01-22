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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.read.streaming.{Offset, ReadLimit, SparkDataStream, SupportsTriggerAvailableNow}
import org.apache.spark.sql.execution.streaming.checkpointing.SequentialUnionOffset
import org.apache.spark.sql.execution.streaming.runtime.SequentialUnionManager

class SequentialUnionManagerSuite extends SparkFunSuite {

  // Simple test offset implementation
  case class TestOffset(value: Long) extends Offset {
    override def json(): String = s"""{"value":$value}"""
  }

  // Mock bounded source that implements SupportsTriggerAvailableNow
  class MockBoundedSource(val name: String)
      extends SparkDataStream with SupportsTriggerAvailableNow {
    override def initialOffset(): Offset = TestOffset(0)
    override def latestOffset(startOffset: Offset, limit: ReadLimit): Offset = {
      TestOffset(100)
    }
    override def deserializeOffset(json: String): Offset = TestOffset(0)
    override def commit(end: Offset): Unit = {}
    override def stop(): Unit = {}
    override def prepareForTriggerAvailableNow(): Unit = {}
  }

  // Mock unbounded source (does not implement SupportsTriggerAvailableNow)
  // This only extends SparkDataStream, which doesn't have latestOffset
  class MockUnboundedSource(val name: String) extends SparkDataStream {
    override def initialOffset(): Offset = TestOffset(0)
    override def deserializeOffset(json: String): Offset = TestOffset(0)
    override def commit(end: Offset): Unit = {}
    override def stop(): Unit = {}
  }

  test("initial state - first source active") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))

    assert(manager.isSourceActive("source1"))
    assert(!manager.isSourceActive("source2"))
    assert(!manager.isSourceActive("source3"))
  }

  test("initial state - explicit active source") {
    val manager = new SequentialUnionManager(
      sourceNames = Seq("source1", "source2", "source3"),
      initialActiveSource = Some("source2"),
      initialCompletedSources = Set("source1")
    )

    assert(!manager.isSourceActive("source1"))
    assert(manager.isSourceActive("source2"))
    assert(!manager.isSourceActive("source3"))
  }

  test("source transition - advance to next source") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))

    // Initially on source1
    assert(manager.isSourceActive("source1"))

    // Advance to source2
    val nextSource = manager.advanceToNextSource()
    assert(nextSource === Some("source2"))
    assert(!manager.isSourceActive("source1"))
    assert(manager.isSourceActive("source2"))

    // Advance to source3
    val nextSource2 = manager.advanceToNextSource()
    assert(nextSource2 === Some("source3"))
    assert(!manager.isSourceActive("source2"))
    assert(manager.isSourceActive("source3"))
  }

  test("source transition - no more sources after final") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))

    // Advance to source2 (final source)
    manager.advanceToNextSource()
    assert(manager.isSourceActive("source2"))

    // Try to advance beyond final source
    val nextSource = manager.advanceToNextSource()
    assert(nextSource === None)

    // Should still be on source2
    assert(manager.isSourceActive("source2"))
  }

  test("completion detection - matching offsets means complete") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    val source = new MockBoundedSource("source1")
    val offset = TestOffset(10)

    // Same start and end offset means source is complete
    assert(manager.isSourceComplete("source1", offset, offset, source))
  }

  test("completion detection - different offsets means not complete") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    val source = new MockBoundedSource("source1")

    // Different offsets means source is not complete
    val isComplete = manager.isSourceComplete(
      "source1", TestOffset(10), TestOffset(20), source)
    assert(!isComplete)
  }

  test("completion detection - inactive source cannot complete") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    val source = new MockBoundedSource("source2")
    val offset = TestOffset(10)

    // source2 is inactive (source1 is active), so it cannot complete
    assert(!manager.isSourceComplete("source2", offset, offset, source))
  }

  test("completion detection - final source never completes") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    manager.advanceToNextSource() // Move to source2 (final source)

    val source = new MockBoundedSource("source2")
    val offset = TestOffset(10)

    // Final source never completes, even with matching offsets
    assert(!manager.isSourceComplete("source2", offset, offset, source))
  }

  test("completion detection - unbounded source throws exception") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    val unboundedSource = new MockUnboundedSource("source1")
    val offset = TestOffset(10)

    // Unbounded source (non-final) should throw exception
    val ex = intercept[IllegalStateException] {
      manager.isSourceComplete("source1", offset, offset, unboundedSource)
    }
    val expectedMsg = "must implement SupportsTriggerAvailableNow"
    assert(ex.getMessage.contains(expectedMsg))
  }

  test("state restoration from offset") {
    val offset = SequentialUnionOffset(
      activeSourceName = "source2",
      completedSources = Set("source1"),
      sourceNames = Seq("source1", "source2", "source3")
    )

    // Create manager with sourceNames from offset, then restore state
    val manager = new SequentialUnionManager(offset.sourceNames)
    manager.restore(offset)

    assert(!manager.isSourceActive("source1"))
    assert(manager.isSourceActive("source2"))
    assert(!manager.isSourceActive("source3"))
  }

  test("getOffset returns current state") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))

    val offset = manager.getOffset()
    assert(offset.activeSourceName === "source1")
    assert(offset.completedSources === Set.empty)
    assert(offset.sourceNames === Seq("source1", "source2", "source3"))

    // Advance and check again
    manager.advanceToNextSource()
    val offset2 = manager.getOffset()
    assert(offset2.activeSourceName === "source2")
    assert(offset2.completedSources === Set("source1"))
  }

  test("offset roundtrip through JSON") {
    val manager = new SequentialUnionManager(
      sourceNames = Seq("delta1", "delta2", "kafka"),
      initialActiveSource = Some("delta2"),
      initialCompletedSources = Set("delta1")
    )

    val offset = manager.getOffset()
    val json = offset.json()
    val restored = SequentialUnionOffset(json)

    assert(restored.activeSourceName === "delta2")
    assert(restored.completedSources === Set("delta1"))
    assert(restored.sourceNames === Seq("delta1", "delta2", "kafka"))
  }

  test("boundedSourcesComplete - false when on first source") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))

    assert(!manager.boundedSourcesComplete)
  }

  test("boundedSourcesComplete - false when on middle source") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))
    manager.advanceToNextSource() // Move to source2

    assert(!manager.boundedSourcesComplete)
  }

  test("boundedSourcesComplete - true when on final source") {
    val manager = new SequentialUnionManager(Seq("source1", "source2", "source3"))
    manager.advanceToNextSource() // Move to source2
    manager.advanceToNextSource() // Move to source3 (final)

    assert(manager.boundedSourcesComplete)
  }

  test("boundedSourcesComplete - true with two sources") {
    val manager = new SequentialUnionManager(Seq("source1", "source2"))
    manager.advanceToNextSource() // Move to source2 (final)

    assert(manager.boundedSourcesComplete)
  }

  test("multiple source transitions") {
    val manager = new SequentialUnionManager(
      Seq("source1", "source2", "source3", "source4", "source5")
    )

    // Verify initial state
    assert(manager.isSourceActive("source1"))

    // Transition through all sources
    assert(manager.advanceToNextSource() === Some("source2"))
    assert(manager.isSourceActive("source2"))

    assert(manager.advanceToNextSource() === Some("source3"))
    assert(manager.isSourceActive("source3"))

    assert(manager.advanceToNextSource() === Some("source4"))
    assert(manager.isSourceActive("source4"))

    assert(manager.advanceToNextSource() === Some("source5"))
    assert(manager.isSourceActive("source5"))

    // All bounded sources complete, now on final source
    assert(manager.boundedSourcesComplete)

    // Trying to advance beyond final source returns None
    assert(manager.advanceToNextSource() === None)
  }

  test("restore preserves completed sources across multiple transitions") {
    val manager1 = new SequentialUnionManager(
      Seq("source1", "source2", "source3", "source4")
    )

    // Advance through multiple sources
    manager1.advanceToNextSource()
    manager1.advanceToNextSource()
    manager1.advanceToNextSource()

    // Save state
    val offset = manager1.getOffset()
    assert(offset.activeSourceName === "source4")
    assert(offset.completedSources === Set("source1", "source2", "source3"))

    // Restore to new manager - use sourceNames from offset
    val manager2 = new SequentialUnionManager(offset.sourceNames)
    manager2.restore(offset)

    // Verify state was restored correctly
    assert(manager2.isSourceActive("source4"))
    assert(!manager2.isSourceActive("source1"))
    assert(!manager2.isSourceActive("source2"))
    assert(!manager2.isSourceActive("source3"))
  }

  test("empty sourceNames throws exception") {
    val ex = intercept[IllegalArgumentException] {
      new SequentialUnionManager(Seq.empty)
    }
    assert(ex.getMessage.contains("sourceNames cannot be empty"))
  }

  test("single source - immediately on final source") {
    val manager = new SequentialUnionManager(Seq("onlySource"))

    assert(manager.isSourceActive("onlySource"))
    assert(manager.boundedSourcesComplete)

    // Cannot advance beyond single source
    assert(manager.advanceToNextSource() === None)
  }

  test("getOffset creates valid SequentialUnionOffset") {
    val manager = new SequentialUnionManager(
      sourceNames = Seq("source1", "source2"),
      initialActiveSource = Some("source2"),
      initialCompletedSources = Set("source1")
    )

    val offset = manager.getOffset()

    // Verify offset is valid (won't throw validation errors)
    assert(offset.activeSourceName === "source2")
    assert(offset.completedSources === Set("source1"))
    assert(offset.sourceNames === Seq("source1", "source2"))
    assert(offset.version === 1)
  }
}
