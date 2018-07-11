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

import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SingleLevelMemoryCatalog implements AthenaXTableCatalog, Serializable {
  private static final long serialVersionUID = -1L;
  private final String database;
  private final Map<String, MockExternalCatalogTable> tables;

  public SingleLevelMemoryCatalog(String database, Map<String, MockExternalCatalogTable> tables) {
    this.database = database;
    this.tables = tables;
  }

  @Override
  public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
    MockExternalCatalogTable table = tables.get(tableName);
    if (table == null) {
      throw new TableNotExistException(database, "Table " + tableName + " does not exist");
    }
    return table.toExternalCatalogTable();
  }

  @Override
  public List<String> listTables() {
    return new ArrayList<>(tables.keySet());
  }

  @Override
  public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
    throw new CatalogNotExistException(dbName);
  }

  @Override
  public List<String> listSubCatalogs() {
    return Collections.emptyList();
  }
}
