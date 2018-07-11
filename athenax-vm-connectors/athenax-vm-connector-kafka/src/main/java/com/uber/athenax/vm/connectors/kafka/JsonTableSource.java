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

package com.uber.athenax.vm.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSource;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

class JsonTableSource extends Kafka09JsonTableSource {
  static final String KAFKA_JSON_TABLE_SOURCE_TYPE = "kafka+json";
  static final int KAFKA_JSON_TABLE_SOURCE_VERSION = 1;

  JsonTableSource(String topic, Properties properties, TableSchema schema) {
    super(topic, properties, schema, schema);
    super.setStartupMode(StartupMode.GROUP_OFFSETS);
  }

  @Override
  public String getProctimeAttribute() {
    return "proctime";
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
    return (List<RowtimeAttributeDescriptor>) Collections.EMPTY_LIST;
  }
}
