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

import com.uber.athenax.vm.api.DataSinkProvider;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.BatchTableSink;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.PARTITIONER_CLASS_NAME_DEFAULT;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.PARTITIONER_CLASS_NAME_KEY;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.TOPIC_NAME_KEY;

public class KafkaJsonConnector implements DataSinkProvider {
  private static final String TYPE = "kafka+json";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public AppendStreamTableSink<Row> getAppendStreamTableSink(ExternalCatalogTable table) throws IOException {
    Map<String, String> prop = table.properties();
    String topic = prop.get(TOPIC_NAME_KEY);
    Properties conf = KafkaUtils.getSubProperties(prop, KAFKA_CONFIG_PREFIX);
    String partitionerClass = prop.getOrDefault(PARTITIONER_CLASS_NAME_KEY, PARTITIONER_CLASS_NAME_DEFAULT);
    FlinkKafkaPartitioner<Row> partitioner;
    try {
      partitioner = KafkaUtils.instantiatePartitioner(partitionerClass);
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new IOException(e);
    }
    return new Kafka09JsonTableSink(topic, conf, partitioner);
  }

  @Override
  public BatchTableSink<Row> getBatchTableSink(ExternalCatalogTable table) throws IOException {
    throw new UnsupportedOperationException();
  }
}
