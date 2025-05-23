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
package org.apache.iceberg.hadoop;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

public class HadoopTableTestBase {
  // Schema passed to create tables
  static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get(), "unique ID"),
          required(4, "data", Types.StringType.get()));

  // This is the actual schema for the table, with column IDs reassigned
  static final Schema TABLE_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  static final Schema UPDATED_SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()),
          optional(3, "n", Types.IntegerType.get()));

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  static final HadoopTables TABLES = new HadoopTables(new Configuration());

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();
  static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=1") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();
  static final DeleteFile FILE_B_DELETES =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-b-deletes.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(1)
          .build();
  static final DataFile FILE_C =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=2") // easy way to set partition data for now
          .withRecordCount(2) // needs at least one record or else metrics will filter it out
          .build();

  @TempDir File tempDir;
  @TempDir File tableDir;

  String tableLocation = null;
  File metadataDir = null;
  File versionHintFile = null;
  Table table = null;

  @BeforeEach
  public void setupTable() throws Exception {
    this.tableLocation = tableDir.toURI().toString();
    this.metadataDir = new File(tableDir, "metadata");
    this.versionHintFile = new File(metadataDir, "version-hint.text");
    this.table = TABLES.create(SCHEMA, SPEC, tableLocation);
  }

  List<File> listManifestFiles() {
    return Lists.newArrayList(
        metadataDir.listFiles(
            (dir, name) ->
                !name.startsWith("snap") && Files.getFileExtension(name).equalsIgnoreCase("avro")));
  }

  List<File> listMetadataJsonFiles() {
    return Lists.newArrayList(
        metadataDir.listFiles(
            (dir, name) -> name.endsWith(".metadata.json") || name.endsWith(".metadata.json.gz")));
  }

  File version(int versionNumber) {
    return new File(
        metadataDir, "v" + versionNumber + getFileExtension(TableMetadataParser.Codec.NONE));
  }

  TableMetadata readMetadataVersion(int version) {
    return TableMetadataParser.read(
        new TestTables.TestTableOperations("table", tableDir).io(), localInput(version(version)));
  }

  int readVersionHint() throws IOException {
    return Integer.parseInt(
        Files.asCharSource(versionHintFile, StandardCharsets.UTF_8).readFirstLine());
  }

  void replaceVersionHint(int version) throws IOException {
    // remove the checksum that will no longer match
    new File(metadataDir, ".version-hint.text.crc").delete();
    Files.asCharSink(versionHintFile, StandardCharsets.UTF_8).write(String.valueOf(version));
  }

  /*
   * Rewrites all current metadata files to gz compressed with extension .metadata.json.gz.
   * Assumes source files are not compressed.
   */
  void rewriteMetadataAsGzipWithOldExtension() throws IOException {
    List<File> metadataJsonFiles = listMetadataJsonFiles();
    for (File file : metadataJsonFiles) {
      try (FileInputStream input = new FileInputStream(file)) {
        try (GZIPOutputStream gzOutput =
            new GZIPOutputStream(new FileOutputStream(file.getAbsolutePath() + ".gz"))) {
          int bb;
          while ((bb = input.read()) != -1) {
            gzOutput.write(bb);
          }
        }
      }
      // delete original metadata file
      file.delete();
    }
  }

  protected HadoopCatalog hadoopCatalog() throws IOException {
    return hadoopCatalog(Collections.emptyMap());
  }

  protected HadoopCatalog hadoopCatalog(Map<String, String> catalogProperties) throws IOException {
    HadoopCatalog hadoopCatalog = new HadoopCatalog();
    hadoopCatalog.setConf(new Configuration());
    hadoopCatalog.initialize(
        "hadoop",
        ImmutableMap.<String, String>builder()
            .putAll(catalogProperties)
            .put(CatalogProperties.WAREHOUSE_LOCATION, tempDir.getAbsolutePath())
            .buildOrThrow());
    return hadoopCatalog;
  }
}
