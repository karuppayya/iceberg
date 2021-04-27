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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TesHiveTableNameIdentifiers24 extends SparkTestBase {

  @Before
  public void initialize() {
    spark.conf().set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog");
    spark.conf().set("spark.sql.catalog.spark_catalog.type", "hive");
  }

  @Test
  public void testHiveTableCreationWithInvalidTopLevelColumnName() {
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t");
    final Schema SCHEMA = new Schema(
        optional(1, "key", Types.StructType.of(
            optional(2, "x", Types.StringType.get()),
            optional(3, "yi", Types.DoubleType.get())

        )),
        optional(4, "value,a", Types.StringType.get()),
        optional(5, "p", Types.StringType.get())
    );

    // top level column with comma
    catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }

  @Test
  public void testHiveTableCreationWithInvalidNestedColumnName() {
    // Nested column with comma
    TableIdentifier tableIdentifier = TableIdentifier.of("default", "t1");
    final Schema SCHEMA = new Schema(
        optional(1, "key", Types.StructType.of(
            optional(2, "x", Types.StringType.get()),
            optional(3, "y,i", Types.DoubleType.get())
        )),
        optional(4, "value_a", Types.StringType.get()),
        optional(5, "p", Types.StringType.get())
    );

    catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
    Dataset<Row> resultDf = spark.read()
        .format("iceberg")
        .load(tableIdentifier.toString());
    resultDf.collectAsList();
  }
}
