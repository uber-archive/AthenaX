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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSourceFactory;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.uber.athenax.vm.connectors.kafka.JsonTableSource.KAFKA_JSON_TABLE_SOURCE_TYPE;
import static com.uber.athenax.vm.connectors.kafka.JsonTableSource.KAFKA_JSON_TABLE_SOURCE_VERSION;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_NAME_KEY;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_SCHEMA_KEY;

@SuppressWarnings("unused")
public class JsonTableSourceFactory implements TableSourceFactory<Row> {

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put(ConnectorDescriptorValidator.CONNECTOR_TYPE(), KAFKA_JSON_TABLE_SOURCE_TYPE);
    context.put(ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION(),
        String.valueOf(KAFKA_JSON_TABLE_SOURCE_VERSION));
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = new ArrayList<>();

    // kafka
    properties.add(TOPIC_NAME_KEY);
    properties.add(KAFKA_CONFIG_PREFIX + "." + "group.id");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "auto.offset.reset");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "bootstrap.servers");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.topic");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.properties");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.properties.#.key");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.properties.#.value");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.startup-mode");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.specific-offsets.#.partition");
    properties.add(KAFKA_CONFIG_PREFIX + "." + "connector.specific-offsets.#.offset");

    // schema
    properties.add(TOPIC_SCHEMA_KEY + ".#." + "name");
    properties.add(TOPIC_SCHEMA_KEY + ".#." + "type");
    return properties;
  }

  @Override
  public StreamTableSource<Row> create(Map<String, String> properties) {
    DescriptorProperties params = new DescriptorProperties(true);
    params.putProperties(properties);
    TableSchema schema = params.getTableSchema(TOPIC_SCHEMA_KEY);
    String topic = params.getString(TOPIC_NAME_KEY);
    Properties conf = new Properties();
    conf.putAll(params.getPrefix(KAFKA_CONFIG_PREFIX));
    return new JsonTableSource(topic, conf, schema);
  }
}
