/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.spark.extensions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

@RunWith(Parameterized.class)
public abstract class TestIdentifiers extends SparkTestBase {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  protected Catalog validationCatalog;
  protected SupportsNamespaces validationNamespaceCatalog;
  protected TableIdentifier tableIdent = TableIdentifier.of(Namespace.of("default"), "table");
  protected String tableName;
  protected String validChar;

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][]{
        {"."}, {","}, {";"}, {":"}
    };
  }

  @BeforeClass
  public static void startMetastoreAndSpark() {
    SparkTestBase.metastore = new TestHiveMetastore();
    metastore.start();
    SparkTestBase.hiveConf = metastore.hiveConf();

    SparkTestBase.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.testing", "true")
        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
        .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .config("spark.sql.shuffle.partitions", "4")
        .enableHiveSupport()
        .getOrCreate();

    SparkTestBase.catalog = (HiveCatalog)
        CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
  }

  @Test
  public void tableWithQuotedIdentifiers() {
    sql("CREATE TABLE %s (key STRUCT<x:STRING,`yi`:DOUBLE>, `value_a` STRING, `p_p1` STRING)" +
        " USING iceberg" +
        " PARTITIONED BY (`p_p1`)", tableName);
    String jsonData = "{ \"key\": { \"x\": \"X1\", \"yi\": 1.0 }, \"value_a\": \"A1\", \"p_p1\": \"P1\" }\n" +
        "{ \"key\": { \"x\": \"X2\", \"yi\": 2.0 }, \"value_a\": \"A2\", \"p_p1\": \"P2\" }";

    Dataset<Row> recordDF = toDS(jsonData).select("key", "value_a", "p_p1");

    recordDF.write().insertInto(tableName);

    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(row("X1", 1.0), "A1", "P1"),
        row(row("X2", 2.0), "A2", "P2")
    );

    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testColumnNamesWithSpecialChars() {
    sql("CREATE TABLE %s ( key STRUCT<x:STRING,`y" + validChar + "i`:DOUBLE>,`value" + validChar + "a` STRING," +
        "`p_p1` STRING)" +
        " USING iceberg" +
        " PARTITIONED BY (`p_p1`)", tableName);

    String jsonData =
        "{ \"key\": { \"x\": \"X1\", \"y" + validChar + "i\": 1.0 }," +
            " \"value" + validChar + "a\": \"A1\", \"p_p1\": \"P1\" }\n" +
        "{ \"key\": { \"x\": \"X2\", \"y" + validChar + "i\": 2.0 }," +
            " \"value" + validChar + "a\": \"A2\", \"p_p1\": \"P2\" }";

    Dataset<Row> recordDF = toDS(jsonData).select("key", "`value" + validChar + "a`", "p_p1");

    recordDF.write().insertInto(tableName);

    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(row("X1", 1.0), "A1", "P1"),
        row(row("X2", 2.0), "A2", "P2")
    );

    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testNestedColumnNamesWithSpecialChars() {
    sql("CREATE TABLE %s (key STRUCT<x:STRING,`y.i`:DOUBLE>, `value_a` STRING, `p` STRING)\n" +
        "USING iceberg\n" +
        "PARTITIONED BY (`p`)", tableName);
  }


  @Test
  public void testSortColumnNamesWithSpecialChars() {
    sql("CREATE TABLE %s ( key STRUCT<x:STRING,`y" + validChar + "i`:DOUBLE>,`value" + validChar + "a` STRING," +
        "`p_p1` STRING)\n" +
        " USING iceberg\n" +
        " PARTITIONED BY (`p_p1`)", tableName);

    sql("ALTER TABLE %s WRITE ORDERED BY (`key`.`y" + validChar + "i`)", tableName);

    String jsonData =
        "{ \"key\": { \"x\": \"X1\", \"y" + validChar + "i\": 1.0 }," +
            " \"value" + validChar + "a\": \"A1\", \"p_p1\": \"P1\" }\n" +
        "{ \"key\": { \"x\": \"X2\", \"y" + validChar + "i\": 2.0 }," +
            " \"value" + validChar + "a\": \"A2\", \"p_p1\": \"P2\" }";

    Dataset<Row> recordDF = toDS(jsonData).select("key", "`value" + validChar + "a`", "p_p1");

    recordDF.write().insertInto(tableName);

    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(row("X1", 1.0), "A1", "P1"),
        row(row("X2", 2.0), "A2", "P2")
    );

    assertEquals("Should have expected rows", expectedRows, sql("SELECT * FROM %s", tableName));
  }

  private Dataset<Row> toDS(String jsonData) {
    List<String> jsonRows = Arrays.stream(jsonData.split("\n"))
        .filter(str -> str.trim().length() > 0)
        .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, Encoders.STRING());
    return spark.read().json(jsonDS);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  abstract String implementationName();

  protected String catalogName() {
    return "test";
  }

  abstract Map<String, String> configs();

  protected String tableName(String name) {
    return (catalogName().equals("spark_catalog") ? "" : catalogName() + ".") + "default." + name;
  }
}
