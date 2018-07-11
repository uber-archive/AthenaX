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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

/**
 * The configuration keys for the Kafka connectors.
 */
public final class KafkaConnectorDescriptorValidator {
  /**
   * The prefix of all Kafka configurations that will be passed into
   * the Kafka consumer / producer.
   */
  public static final String KAFKA_CONFIG_PREFIX = "kafka";

  /**
   * Default partitioner.
   */
  public static final String PARTITIONER_CLASS_NAME_DEFAULT = FlinkFixedPartitioner.class.getCanonicalName();

  /**
   * The class name of the Kafka partitioner. The connector will instantiate a new class
   * to send the Kafka messages to corresponding partitions.
   */
  public static final String PARTITIONER_CLASS_NAME_KEY = "athenax.kafka.partitioner.class";

  /**
   * The name of the Kafka topic to be read or written.
   */
  public static final String TOPIC_NAME_KEY = "athenax.kafka.topic.name";

  /**
   * The prefix for Kafka topic schema.
   */
  public static final String TOPIC_SCHEMA_KEY = "athenax.kafka.topic.schema";

  private KafkaConnectorDescriptorValidator() {
  }
}
