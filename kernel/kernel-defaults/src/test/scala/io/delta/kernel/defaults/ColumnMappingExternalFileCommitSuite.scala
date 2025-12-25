/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtilsWithV1Builders, WriteUtilsWithV2Builders}
import io.delta.kernel.exceptions.KernelEngineException
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile, SingleAction}
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import org.scalatest.funsuite.AnyFunSuite

class ColumnMappingExternalFileCommitTransactionBuilderV1Suite
    extends ColumnMappingExternalFileCommitSuiteBase
    with WriteUtilsWithV1Builders

class ColumnMappingExternalFileCommitTransactionBuilderV2Suite
    extends ColumnMappingExternalFileCommitSuiteBase
    with WriteUtilsWithV2Builders

trait ColumnMappingExternalFileCommitSuiteBase extends AnyFunSuite with AbstractWriteUtils {

  test("fail only when an external parquet file uses logical names instead of physical names") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a column mapping enabled table (name mode).
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = Map("delta.columnMapping.mode" -> "name"))

      val meta = getMetadata(engine, tablePath)
      val physicalName = ColumnMapping.getPhysicalName(meta.getSchema.get("id"))

      // Create two parquet files:
      // - bad: column uses logical name "id"
      // - good: column uses physical name "col-..."
      // NOTE: This test uses DefaultEngine, so we can use Spark to write parquet locally.
      val badDir = s"$tablePath/_bad_parquet_${System.currentTimeMillis()}"
      val goodDir = s"$tablePath/_good_parquet_${System.currentTimeMillis()}"

      spark.range(1).selectExpr("cast(id as int) as id").write.mode("overwrite").parquet(badDir)
      spark
        .range(1)
        .selectExpr(s"cast(id as int) as `${physicalName}`")
        .write
        .mode("overwrite")
        .parquet(goodDir)

      def firstParquetFile(dir: String): String = {
        val fs =
          new org.apache.hadoop.fs.Path(dir).getFileSystem(spark.sparkContext.hadoopConfiguration)
        val statuses = fs.listStatus(new org.apache.hadoop.fs.Path(dir))
        statuses
          .map(_.getPath)
          .find(p => p.getName.endsWith(".parquet"))
          .get
          .toString
      }

      val badParquetPath = firstParquetFile(badDir)
      val goodParquetPath = firstParquetFile(goodDir)

      // Copy one parquet file into the table root to make AddFile path relative.
      val fs = new org.apache.hadoop.fs.Path(
        tablePath).getFileSystem(spark.sparkContext.hadoopConfiguration)
      def copyToTableRoot(src: String, destFileName: String): Unit = {
        val conf = spark.sparkContext.hadoopConfiguration
        val srcP = new org.apache.hadoop.fs.Path(src)
        val dstP = new org.apache.hadoop.fs.Path(s"$tablePath/$destFileName")
        org.apache.hadoop.fs.FileUtil.copy(fs, srcP, fs, dstP, false, conf)
      }

      copyToTableRoot(badParquetPath, "bad.parquet")
      copyToTableRoot(goodParquetPath, "good.parquet")

      def addFileActionRow(fileName: String): Row =
        KernelAddFile.createAddFileRow(
          new StructType(),
          fileName,
          VectorUtils.stringStringMapValue(Map.empty[String, String].asJava),
          1L,
          System.currentTimeMillis(),
          true,
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty())

      // Bad file should fail with a clear message about physical column name expectation.
      {
        val txn = getUpdateTxn(engine, tablePath)
        val actionRow = SingleAction.createAddFileSingleAction(addFileActionRow("bad.parquet"))
        val e = intercept[KernelEngineException] {
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(Seq(actionRow).asJava.iterator())))
        }
        val msg = e.getMessage.toLowerCase
        assert(msg.contains("column mapping is enabled"))
        assert(msg.contains("expects physical column"))
      }

      // Good file should succeed (file uses the physical column name).
      {
        val txn = getUpdateTxn(engine, tablePath)
        val actionRow = SingleAction.createAddFileSingleAction(addFileActionRow("good.parquet"))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(Seq(actionRow).asJava.iterator())))
      }
    }
  }
}
