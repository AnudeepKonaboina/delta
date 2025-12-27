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

package org.apache.spark.sql.delta

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Regression coverage for cluster-mode reads of tables created with DEFAULT columns.
 *
 * Spark may evaluate existence default values during Parquet scan initialization for missing
 * columns. In cluster mode, executors do not have an active/default SparkSession, so any
 * executor-side resolution of DEFAULT expressions can fail (see issue #4370).
 */
class ColumnDefaultsClusterModeSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = {
    super.sparkConf
      // Use separate executor JVMs to match the failing environment described in issue #4370.
      .setMaster("local-cluster[2,1,1024]")
      .set("spark.ui.enabled", "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  test("cluster-mode read of table created with DEFAULT columns (INSERT omits default column)") {
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        sql(
          s"""
             |CREATE TABLE delta.`$path` (
             |  time TIMESTAMP,
             |  rowIngestionTime TIMESTAMP DEFAULT current_timestamp()
             |)
             |USING delta
             |TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled')
             |""".stripMargin)

        sql(s"INSERT INTO delta.`$path` (time) VALUES (current_timestamp())")
        sql(s"INSERT INTO delta.`$path` (time, rowIngestionTime) " +
          s"VALUES (current_timestamp(), current_timestamp())")

        val rows = sql(s"SELECT * FROM delta.`$path`").collect()
        assert(rows.length == 2)
        assert(rows.forall(!_.isNullAt(1)))
      }
    }
  }
}


