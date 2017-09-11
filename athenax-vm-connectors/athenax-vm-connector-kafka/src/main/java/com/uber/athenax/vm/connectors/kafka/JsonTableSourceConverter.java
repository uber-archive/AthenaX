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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.annotation.TableType;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.TableSourceConverter;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.TOPIC_NAME_KEY;

@TableType("kafka+json")
@SuppressWarnings("unused")
public class JsonTableSourceConverter implements TableSourceConverter<JsonTableSource> {

  @Override
  public Set<String> requiredProperties() {
    return Collections.singleton(TOPIC_NAME_KEY);
  }

  /**
   * Construct a data source for a Kafka topic.
   *
   * <p>The URI has the following format: <pre>kafka+json://zk-host1,zk-host-2/topic-name</pre></p>
   */
  @Override
  public JsonTableSource fromExternalCatalogTable(ExternalCatalogTable table) {
    Map<String, String> prop = table.properties();
    String topic = prop.get(TOPIC_NAME_KEY);
    RowTypeInfo rowType = KafkaUtils.toRowType(table.schema());

    Properties conf = KafkaUtils.getSubProperties(prop, KAFKA_CONFIG_PREFIX);
    return new JsonTableSource(topic, conf, rowType);
  }
}
