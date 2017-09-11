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
import org.apache.flink.table.annotation.TableType;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.TableSourceConverter;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@TableType("mock")
public class MockTableSourceConverter implements TableSourceConverter<MockTableSource> {

  @Override
  public Set<String> requiredProperties() {
    return Collections.singleton("data");
  }

  @Override
  public MockTableSource fromExternalCatalogTable(ExternalCatalogTable table) {
    List<Row> rows = deserializeRows(table.properties().get("data"));
    RowTypeInfo type = new RowTypeInfo(table.schema().getTypes(), table.schema().getColumnNames());
    return new MockTableSource(rows, type);
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
