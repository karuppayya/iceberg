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

package org.apache.iceberg.util;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataLocationUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TesMetadataLocationUtils {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get())
  );

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  private static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();
  private static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withRecordCount(1)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Table table;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    String tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
  }


  @Test
  public void testManifestListPaths() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    List<String> manifestListPaths = MetadataLocationUtils.manifestListPaths(table);
    Assert.assertEquals(manifestListPaths.size(), 2);
  }

  @Test
  public void testMiscMetadataFiles() {
    table.updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
        .commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();

    List<String> miscMetadataFilePaths = MetadataLocationUtils
        .miscMetadataFiles(((HasTableOperations) table).operations(), true);
    Assert.assertEquals(miscMetadataFilePaths.size(), 5);

    miscMetadataFilePaths = MetadataLocationUtils
        .miscMetadataFiles(((HasTableOperations) table).operations(), false);
    Assert.assertEquals(miscMetadataFilePaths.size(), 3);
  }
}
