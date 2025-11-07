/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.{File, RandomAccessFile}

import org.apache.hadoop.fs.Path

/**
 * Test suite for issue #5458: Delta lost the ability to recover from a corrupted checkpoint
 * https://github.com/delta-io/delta/issues/5458
 */
class SnapshotManagementSuite5458 extends DeltaSQLCommandTest {

  private def makeCorruptCheckpointFile(
      path: String,
      checkpointVersion: Long,
      shouldBeEmpty: Boolean): Unit = {
    val checkpointFile =
      FileNames.checkpointFileSingular(new Path(path, "_delta_log"), checkpointVersion).toString
    assert(new File(checkpointFile).exists)
    val cp = new RandomAccessFile(checkpointFile, "rw")
    cp.setLength(if (shouldBeEmpty) 0 else 10)
    cp.close()
  }

  test("Issue #5458: recover from corrupt checkpoint and read data") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      
      // Step 1: Create a Delta table with data
      spark.range(10).write.format("delta").save(path)
      var deltaLog = DeltaLog.forTable(spark, path)
      
      // Step 2: Create a checkpoint at version 0
      deltaLog.checkpoint()
      
      // Step 3: Verify table can be read normally
      val beforeCount = spark.read.format("delta").load(path).count()
      assert(beforeCount == 10, "Table should have 10 rows before corruption")
      
      // Step 4: Corrupt the checkpoint file
      DeltaLog.clearCache()
      deltaLog = DeltaLog.forTable(spark, path)
      
      // We have different code paths for empty and non-empty checkpoints
      for (testEmptyCheckpoint <- Seq(true, false)) {
        makeCorruptCheckpointFile(path, checkpointVersion = 0,
          shouldBeEmpty = testEmptyCheckpoint)
        DeltaLog.clearCache()
        
        // Step 5: Verify that we can still create the snapshot using existing json files
        val snapshot = DeltaLog.forTable(spark, path).snapshot
        assert(snapshot.version == 0, "Snapshot version should be 0")
        
        // Step 6: THIS IS THE KEY TEST - Try to READ the table data
        // In Delta 3.2: This works
        // In Delta 4.0: This fails (THIS IS THE BUG)
        val afterCount = spark.read.format("delta").load(path).count()
        assert(afterCount == 10, 
          s"Table should have 10 rows after corruption recovery (testEmptyCheckpoint=$testEmptyCheckpoint)")
      }
    }
  }
}

