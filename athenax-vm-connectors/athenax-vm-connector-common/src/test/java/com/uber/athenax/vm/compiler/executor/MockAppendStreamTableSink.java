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
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

class MockAppendStreamTableSink implements AppendStreamTableSink<Row> {
  private final List<Row> rows = new ArrayList<>();
  private final RowTypeInfo type;

  MockAppendStreamTableSink(RowTypeInfo type) {
    this.type = type;
  }

  @Override
  public void emitDataStream(DataStream<Row> dataStream) {
    dataStream.writeUsingOutputFormat(new LocalCollectionOutputFormat<>(rows));
  }

  @Override
  public TypeInformation<Row> getOutputType() {
    return type;
  }

  @Override
  public String[] getFieldNames() {
    return type.getFieldNames();
  }

  @Override
  public TypeInformation<?>[] getFieldTypes() {
    return type.getFieldTypes();
  }

  @Override
  public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    RowTypeInfo type = new RowTypeInfo(fieldTypes, fieldNames);
    return new MockAppendStreamTableSink(type);
  }
}
