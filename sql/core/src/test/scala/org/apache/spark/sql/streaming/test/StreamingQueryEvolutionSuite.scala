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

package org.apache.spark.sql.streaming.test

import scala.concurrent.duration._

import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, Tag}

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.checkpointing.{OffsetMap, OffsetSeqLog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.util.Utils

/**
 * Test suite for streaming source naming and validation.
 * Tests cover the naming API, validation rules, and resolution pipeline.
 */
class StreamingQueryEvolutionSuite extends StreamTest with BeforeAndAfterEach {

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  override def afterEach(): Unit = {
    spark.streams.active.foreach(_.stop())
    super.afterEach()
  }

  /**
   * Helper to verify that a source was created with the expected metadata path.
   * @param checkpointLocation the checkpoint location path
   * @param sourcePath the expected source path (e.g., "source1" or "0")
   * @param mode mockito verification mode (default: times(1))
   */
  private def verifySourcePath(
      checkpointLocation: Path,
      sourcePath: String,
      mode: org.mockito.verification.VerificationMode = times(1)): Unit = {
    verify(LastOptions.mockStreamSourceProvider, mode).createSource(
      any(),
      meq(s"${new Path(makeQualifiedPath(
        checkpointLocation.toString)).toString}/sources/$sourcePath"),
      meq(None),
      meq("org.apache.spark.sql.streaming.test"),
      meq(Map.empty))
  }

  // ====================
  // Name Validation Tests
  // ====================

  testWithSourceEvolution("invalid source name - contains hyphen") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my-source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my-source"))
  }

  testWithSourceEvolution("invalid source name - contains space") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my source"))
  }

  testWithSourceEvolution("invalid source name - contains dot") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source"))
  }

  testWithSourceEvolution("invalid source name - contains special characters") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name("my.source@123")
          .load()
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.INVALID_SOURCE_NAME",
      parameters = Map("sourceName" -> "my.source@123"))
  }

  testWithSourceEvolution("valid source names - various patterns") {
    // Test that valid names work correctly
    Seq("mySource", "my_source", "MySource123", "_private", "source_123_test", "123source")
      .foreach { name =>
        val df = spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .name(name)
          .load()
        assert(df.isStreaming, s"DataFrame should be streaming for name: $name")
      }
  }

  testWithSourceEvolution("method chaining - name() returns reader for chaining") {
    val df = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("my_source")
      .option("opt1", "value1")
      .load()

    assert(df.isStreaming, "DataFrame should be streaming")
  }

  // ==========================
  // Duplicate Detection Tests
  // ==========================

  testWithSourceEvolution("duplicate source names - rejected during analysis") {
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("duplicate_name")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("duplicate_name")  // Same name - should fail
      .load()

    checkError(
      exception = intercept[AnalysisException] {
        // Union operation triggers analysis, which detects duplicates
        df1.union(df2).queryExecution.analyzed
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.DUPLICATE_SOURCE_NAMES",
      parameters = Map("names" -> "duplicate_name"))
  }

  testWithSourceEvolution("enforcement enabled - unnamed source rejected") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream
          .format("org.apache.spark.sql.streaming.test")
          .load() // Unnamed - throws error at load() time
      },
      condition = "STREAMING_QUERY_EVOLUTION_ERROR.UNNAMED_STREAMING_SOURCES_WITH_ENFORCEMENT",
      parameters = Map("sourceInfo" -> ".*"),
      matchPVals = true)
  }

  testWithSourceEvolution("enforcement enabled - all sources named succeeds") {
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("alpha")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("beta")
      .load()

    // Should not throw - all sources are named
    val union = df1.union(df2)
    assert(union.isStreaming, "Union should be streaming")
  }

  // ===========================
  // Checkpoint Path Tests (PR2)
  // ===========================

  testWithSourceEvolution("named sources - metadata path uses source name") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    verifySourcePath(checkpointLocation, "source1")
    verifySourcePath(checkpointLocation, "source2")
  }

  testWithSourceEvolution("source evolution - reorder sources with named sources") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    // First query: source1 then source2
    val df1a = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2a = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q1 = df1a.union(df2a).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q1.processAllAvailable()
    q1.stop()

    LastOptions.clear()

    // Second query: source2 then source1 (reordered) - should still work
    val df1b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q2 = df2b.union(df1b).writeStream // Note: reversed order
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q2.processAllAvailable()
    q2.stop()

    // Both sources should still use their named paths (verified at least once across both queries)
    verifySourcePath(checkpointLocation, "source1", atLeastOnce())
    verifySourcePath(checkpointLocation, "source2", atLeastOnce())
  }

  testWithSourceEvolution("source evolution - add new source with named sources") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    // First query: only source1
    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val q1 = df1.writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q1.processAllAvailable()
    q1.stop()

    LastOptions.clear()

    // Second query: add source2
    val df1b = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q2 = df1b.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q2.processAllAvailable()
    q2.stop()

    // Both sources should have been created with their named paths
    verifySourcePath(checkpointLocation, "source1", atLeastOnce())
    verifySourcePath(checkpointLocation, "source2", times(1))
  }

  testWithSourceEvolution("named sources enforcement uses V2 offset log format") {
    LastOptions.clear()

    val checkpointLocation = new Path(newMetadataDir)

    val df1 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source1")
      .load()

    val df2 = spark.readStream
      .format("org.apache.spark.sql.streaming.test")
      .name("source2")
      .load()

    val q = df1.union(df2).writeStream
      .format("org.apache.spark.sql.streaming.test")
      .option("checkpointLocation", checkpointLocation.toString)
      .trigger(ProcessingTime(10.seconds))
      .start()
    q.processAllAvailable()
    q.stop()

    // Verify V2 offset log format (OffsetMap) is used
    val offsetLog = new OffsetSeqLog(spark,
      makeQualifiedPath(checkpointLocation.toString).toString + "/offsets")
    val offsetSeq = offsetLog.get(0)
    assert(offsetSeq.isDefined, "Offset log should have batch 0")

    offsetSeq.get match {
      case _: OffsetMap =>
        // Expected: V2 format uses OffsetMap
        assert(true)
      case other =>
        fail(s"Expected OffsetMap but got ${other.getClass.getSimpleName}")
    }
  }

  test("backward compatibility - unnamed sources use sequential IDs") {
    // Without enforcement enabled, sources can be unnamed and use sequential IDs
    withSQLConf(SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "false") {
      LastOptions.clear()

      val checkpointLocation = new Path(newMetadataDir)

      val df1 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load() // No .name() - should use sequential ID

      val df2 = spark.readStream
        .format("org.apache.spark.sql.streaming.test")
        .load() // No .name() - should use sequential ID

      val q = df1.union(df2).writeStream
        .format("org.apache.spark.sql.streaming.test")
        .option("checkpointLocation", checkpointLocation.toString)
        .trigger(ProcessingTime(10.seconds))
        .start()
      q.processAllAvailable()
      q.stop()

      // Verify sources use sequential IDs (0, 1) instead of names
      verifySourcePath(checkpointLocation, "0")
      verifySourcePath(checkpointLocation, "1")
    }
  }

  // ==============
  // Helper Methods
  // ==============

  /**
   * Helper method to run tests with source evolution enabled.
   * Sets the offset log format version to V2 for OffsetMap support.
   */
  def testWithSourceEvolution(testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    test(testName, testTags: _*) {
      withSQLConf(
        SQLConf.ENABLE_STREAMING_SOURCE_EVOLUTION.key -> "true",
        SQLConf.STREAMING_OFFSET_LOG_FORMAT_VERSION.key -> "2") {
        testBody
      }
    }
  }
}
