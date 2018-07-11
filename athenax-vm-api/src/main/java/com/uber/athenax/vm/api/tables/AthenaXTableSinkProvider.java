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

package com.uber.athenax.vm.api.tables;

import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * A AthenaXTableSinkProvider constructs a data sink from an {@link ExternalCatalogTable}
 * so that AthenaX can pipe the data to the external system.
 */
public interface AthenaXTableSinkProvider {
  /**
   * The scheme that identifies the data sink.
   */
  String getType();

  /**
   * Construct an {@link AppendStreamTableSink} for streaming query.
   *
   * @param table
   *     The {@link ExternalCatalogTable} that contains all required information to
   *     construct an {@link AppendStreamTableSink} that accepts the same schema from the table.
   */
  AppendStreamTableSink<Row> getAppendStreamTableSink(ExternalCatalogTable table) throws IOException;

  /**
   * Construct an {@link BatchTableSink} for batch query.
   *
   * @param table
   *     The {@link ExternalCatalogTable} that contains all required information to
   *     construct an {@link BatchTableSink} that accepts the same schema from the table.
   */
  BatchTableSink<Row> getBatchTableSink(ExternalCatalogTable table) throws IOException;
}
