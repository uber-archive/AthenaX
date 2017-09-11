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
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.Properties;

final class KafkaUtils {
  private KafkaUtils() {
  }

  static Properties getSubProperties(Map<String, String> prop, String prefix) {
    Properties p = new Properties();
    for (Map.Entry<String, String> e : prop.entrySet()) {
      String key = e.getKey();
      if (key.startsWith(prefix)) {
        p.put(key.substring(prefix.length()), e.getValue());
      }
    }
    return p;
  }

  static RowTypeInfo toRowType(TableSchema schema) {
    return new RowTypeInfo(schema.getTypes(), schema.getColumnNames());
  }

  static FlinkKafkaPartitioner<Row> instantiatePartitioner(String clazzName)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    @SuppressWarnings("unchecked")
    Class<? extends FlinkKafkaPartitioner<Row>> clazz
        = (Class<? extends FlinkKafkaPartitioner<Row>>) Class.forName(clazzName);
    return clazz.newInstance();
  }
}
