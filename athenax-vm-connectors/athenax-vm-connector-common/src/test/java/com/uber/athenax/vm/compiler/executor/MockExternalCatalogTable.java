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
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;
import scala.Option;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.List;

/**
 * External Catalog Table based on mock data and mock schema.
 */
public class MockExternalCatalogTable implements Serializable {
  static final String TABLE_SCHEMA_CONNECTOR_PROPERTY = "table.schema";
  static final String TABLE_DATA_CONNECTOR_PROPERTY = "table.data";
  static final String CONNECTOR_TYPE = "mock";
  static final int CONNECTOR_VERSION = 1;

  private final RowTypeInfo schema;
  private final List<Row> data;

  public MockExternalCatalogTable(RowTypeInfo schema, List<Row> data) {
    this.schema = schema;
    this.data = data;
  }

  ExternalCatalogTable toExternalCatalogTable() {
    TableSchema tableSchema = new TableSchema(schema.getFieldNames(), schema.getFieldTypes());
    ConnectorDescriptor descriptor = new ConnectorDescriptor(CONNECTOR_TYPE, CONNECTOR_VERSION, false) {
      @Override
      public void addConnectorProperties(DescriptorProperties properties) {
        properties.putTableSchema(TABLE_SCHEMA_CONNECTOR_PROPERTY, tableSchema);
        properties.putString(TABLE_DATA_CONNECTOR_PROPERTY, serializeRows());
      }
    };
    return new ExternalCatalogTable(descriptor, Option.empty(), Option.empty(), Option.empty(), Option.empty());
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
