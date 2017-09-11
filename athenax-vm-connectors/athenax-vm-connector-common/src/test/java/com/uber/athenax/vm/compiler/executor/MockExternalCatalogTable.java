/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.athenax.vm.compiler.executor;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.types.Row;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockExternalCatalogTable implements Serializable {
  private final RowTypeInfo schema;
  private final List<Row> data;

  public MockExternalCatalogTable(RowTypeInfo schema, List<Row> data) {
    this.schema = schema;
    this.data = data;
  }

  ExternalCatalogTable toExternalCatalogTable() {
    TableSchema tableSchema = new TableSchema(schema.getFieldNames(), schema.getFieldTypes());
    Map<String, String> prop = Collections.singletonMap("data", serializeRows());
    return new ExternalCatalogTable("mock", tableSchema, prop, null, "", 0L, 0L);
  }

  private String serializeRows() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
      os.writeObject(data);
    } catch (IOException e) {
      return null;
    }
    return Base64.getEncoder().encodeToString(bos.toByteArray());
  }
}
