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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.List;

class MockTableSource implements StreamTableSource<Row> {
  private final List<Row> data;
  private final RowTypeInfo type;
  private final TableSchema schema;

  MockTableSource(List<Row> data, RowTypeInfo type) {
    this.data = data;
    this.type = type;
    this.schema = new TableSchema(type.getFieldNames(), type.getFieldTypes());
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
    return execEnv.fromCollection(data);
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    return type;
  }

  @Override
  public TableSchema getTableSchema() {
    return schema;
  }

  @Override
  public String explainSource() {
    return "mock";
  }
}
