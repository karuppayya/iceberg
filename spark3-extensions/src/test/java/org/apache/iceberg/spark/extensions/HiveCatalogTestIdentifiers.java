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

import java.util.Map;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;

public class HiveCatalogTestIdentifiers extends TestIdentifiers {

  public HiveCatalogTestIdentifiers(String validChar) {
    this.validChar = validChar;
    this.validationCatalog = catalog;
    this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

    String catalogName = catalogName();
    spark.conf().set("spark.sql.catalog." + catalogName, implementationName());
    configs().forEach((key, value) -> spark.conf().set("spark.sql.catalog." + catalogName + "." + key, value));

    this.tableName = (catalogName.equals("spark_catalog") ? "" : catalogName + ".") + "default.table";

    sql("CREATE NAMESPACE IF NOT EXISTS default");
  }

  @Override
  String implementationName() {
    return SparkCatalog.class.getName();
  }

  @Override
  Map<String, String> configs() {
    return ImmutableMap.of(
        "type", "hive",
        "default-namespace", "default"
    );
  }
}
