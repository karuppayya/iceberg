/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark;

import org.junit.Before;
import org.junit.Test;

abstract class TestHiveTableIdentifiers extends SparkTestBase {

  @Before
  public void initialize() {
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @Test
  public void testHiveTableCreationWithInvalidTopLevelColumnName() throws Exception {
    // top level column with comma
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");

    String sql = "CREATE TABLE t (key STRUCT<x:STRING,`yi`:DOUBLE>, `value,a` STRING, `p` STRING) using iceberg";
    sql(sql);
  }

  @Test
  public void testHiveTableCreationWithInvalidNestedColumnName() throws Exception {
    // Nested column with comma
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");

    String sql = "CREATE TABLE t1 (key STRUCT<x:STRING,`y,i`:DOUBLE>, `value_a` STRING, `p` STRING) using iceberg\n";
    sql(sql);
  }
}
