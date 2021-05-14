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


package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestHiveTableNameIdentifiers extends SparkTestBase {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  protected String tableLocation = null;
  private File tableDir = null;

  @Before
  public void initialize() throws IOException {
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    System.out.println("a");
  }

  @Test
  public void testHiveTableCreationWithInvalidTopLevelColumnName() {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t1");
    final Schema schema = new Schema(
        optional(1, "key", Types.StructType.of(
            optional(2, "x", Types.StringType.get()),
            optional(3, "yi", Types.DoubleType.get())

        )),
        optional(4, "value,a", Types.StringType.get()),
        optional(5, "p", Types.StringType.get())
    );

    // top level column with comma
    catalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), tableLocation, Maps.newHashMap());
    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }

  @Test
  public void testHiveTableCreationWithInvalidNestedColumnName() {
    // Nested column with comma
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t2");
    final Schema schema = new Schema(
        optional(1, "key", Types.StructType.of(
            optional(2, "x", Types.StringType.get()),
            optional(3, "y,i", Types.DoubleType.get())
        )),
        optional(4, "value_a", Types.StringType.get()),
        optional(5, "p", Types.StringType.get())
    );

    catalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned(), tableLocation, Maps.newHashMap());
    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }

  @Test
  public void testHivePartitionedTableCreationWithInvalidPartitionedColumnName() {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t3");
    final Schema schema = new Schema(
        optional(1, "c1", Types.IntegerType.get()),
        optional(2, "c2", Types.StringType.get()),
        optional(3, "value,a", Types.StringType.get())
    );

    // Partition column name with comma
    catalog.createTable(tableIdentifier, schema,
        PartitionSpec.builderFor(schema).identity("value,a").build(), tableLocation, Maps.newHashMap());

    appendData(tableIdentifier, "AAAA");

    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }

  @Test
  public void testExpireSanpshotsWithHivePartitionedTable() {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t4");
    final Schema schema = new Schema(
        optional(1, "c1", Types.IntegerType.get()),
        optional(2, "c2", Types.StringType.get()),
        optional(3, "value,a", Types.StringType.get())
    );

    // Partition column name with comma
    Table table = catalog.createTable(tableIdentifier, schema,
        PartitionSpec.builderFor(schema).identity("value,a").build(), tableLocation, Maps.newHashMap());

    appendData(tableIdentifier, "AAAA");
    appendData(tableIdentifier, "AAAB");
    appendData(tableIdentifier, "AAAC");
    List<Row> rows = spark.read().format("iceberg")
        .load(tableIdentifier.toString() + ".files")
        .selectExpr("file_path").collectAsList();
    String filePath = rows.get(0).getString(0);

    Table tbl = catalog.loadTable(tableIdentifier);
    tbl.newDelete().deleteFile(filePath).commit();
    long tAfterCommits = rightAfterSnapshot(tbl);

    Actions.forTable(tbl).expireSnapshots()
        .expireOlderThan(tAfterCommits)
        .execute();

    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }

  private void appendData(TableIdentifier tableIdentifier, String partCol) {
    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", partCol)
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);
    Dataset<Row> renamedDF = df.withColumnRenamed("c3", "value,a");
    // normal write
    renamedDF.select("c1", "c2", "value,a")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableIdentifier.toString());
  }

  private Long rightAfterSnapshot(Table table) {
    long snapshotId = table.currentSnapshot().snapshotId();
    long end = System.currentTimeMillis();
    while (end <= table.snapshot(snapshotId).timestampMillis()) {
      end = System.currentTimeMillis();
    }
    return end;
  }
}
