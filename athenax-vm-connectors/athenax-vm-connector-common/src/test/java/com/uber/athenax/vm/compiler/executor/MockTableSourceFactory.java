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
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceFactory;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.uber.athenax.vm.compiler.executor.MockExternalCatalogTable.CONNECTOR_TYPE;
import static com.uber.athenax.vm.compiler.executor.MockExternalCatalogTable.CONNECTOR_VERSION;
import static com.uber.athenax.vm.compiler.executor.MockExternalCatalogTable.TABLE_DATA_CONNECTOR_PROPERTY;
import static com.uber.athenax.vm.compiler.executor.MockExternalCatalogTable.TABLE_SCHEMA_CONNECTOR_PROPERTY;

public class MockTableSourceFactory implements TableSourceFactory<Row> {

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put(ConnectorDescriptorValidator.CONNECTOR_TYPE(), CONNECTOR_TYPE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION(), String.valueOf(CONNECTOR_VERSION));
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = new ArrayList<>();

    properties.add(TABLE_DATA_CONNECTOR_PROPERTY);
    properties.add(TABLE_SCHEMA_CONNECTOR_PROPERTY + ".#." + "name");
    properties.add(TABLE_SCHEMA_CONNECTOR_PROPERTY + ".#." + "type");
    return properties;
  }

  @Override
  public TableSource<Row> create(Map<String, String> properties) {
    DescriptorProperties params = new DescriptorProperties(true);
    params.putProperties(properties);
    TableSchema schema = params.getTableSchema(TABLE_SCHEMA_CONNECTOR_PROPERTY);
    List<Row> rows = deserializeRows(params.getString(TABLE_DATA_CONNECTOR_PROPERTY));
    return new MockTableSource(rows, new RowTypeInfo(schema.getTypes(), schema.getColumnNames()));
  }

  private List<Row> deserializeRows(String encoded) {
    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(encoded));
    try (ObjectInputStream is = new ObjectInputStream(bis)) {
      @SuppressWarnings("unchecked")
      List<Row> res = (List<Row>) is.readObject();
      return res;
    } catch (ClassNotFoundException | IOException e) {
      return null;
    }
  }
}
