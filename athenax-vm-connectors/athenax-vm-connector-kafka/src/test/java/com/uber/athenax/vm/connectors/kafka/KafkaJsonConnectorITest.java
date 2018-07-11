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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.jmx.JMXReporter;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.junit.Test;
import scala.Option;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;

import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_NAME_KEY;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_SCHEMA_KEY;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.configuration.ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX;
import static org.apache.flink.configuration.ConfigConstants.METRICS_REPORTER_PREFIX;
import static org.junit.Assert.assertTrue;

public class KafkaJsonConnectorITest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int DEFAULT_SO_TIMEOUT = 10000;
  private static final int DEFAULT_BUFFER_SIZE = 65536;
  private static final long STABILIZE_SLEEP_DELAYS = 3000;

  @Test
  public void testPassThrough() throws Exception {
    final long retries = 10;
    final long retryDelay = 1000;
    final String sourceTopic = "foo";
    final String sinkTopic = "bar";
    final TableSchema schema = new TableSchema(new String[] {"foo"}, new TypeInformation[] {INT_TYPE_INFO});

    KafkaJsonConnector connector = new KafkaJsonConnector();

    Configuration flinkConf = new Configuration();
    flinkConf.setString(MetricOptions.REPORTERS_LIST, "test");
    flinkConf.setString(METRICS_REPORTER_PREFIX + "test." + METRICS_REPORTER_CLASS_SUFFIX, JMXReporter.class.getName());

    try (MiniKafkaCluster cluster = new MiniKafkaCluster.Builder().newServer("0").build()) {
      cluster.start();
      String zkAddress = "127.0.0.1:" + cluster.getZkServer().getPort();
      String brokerAddress = "127.0.0.1:" + cluster.getKafkaServerPort(0);
      KafkaTestUtil.createKafkaTopicIfNecessary("zk://" + zkAddress, 1, 1, "foo");
      KafkaTestUtil.createKafkaTopicIfNecessary("zk://" + zkAddress, 1, 1, "bar");

      // Wait until Kafka / ZK stabilizes
      Thread.sleep(STABILIZE_SLEEP_DELAYS);

      try (KafkaProducer<byte[], byte[]> producer = getProducer(brokerAddress)) {
        producer.send(new ProducerRecord<>(sourceTopic, MAPPER.writeValueAsBytes(ImmutableMap.of("foo", 1))));
        producer.send(new ProducerRecord<>(sourceTopic, MAPPER.writeValueAsBytes(ImmutableMap.of("foo", 2))));
      }

      JsonTableSourceFactory factory = new JsonTableSourceFactory();

      ExternalCatalogTable sourceTable = mockExternalCatalogTable(sourceTopic, brokerAddress);
      DescriptorProperties props = new DescriptorProperties(true);
      sourceTable.addProperties(props);
      StreamTableSource<Row> source = factory.create(props.asMap());

      ExternalCatalogTable sinkTable = mockExternalCatalogTable(sinkTopic, brokerAddress);

      TableSink<Row> sink = connector.getAppendStreamTableSink(sinkTable)
          .configure(schema.getColumnNames(), schema.getTypes());

      LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
      DataStream<Row> ds = source.getDataStream(env);
      ((AppendStreamTableSink<Row>) sink).emitDataStream(ds);
      LocalFlinkMiniCluster flink = FlinkTestUtil.execute(env, flinkConf, "test-pass-through");
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

      try {
        boolean found = false;
        for (int i = 0; i < retries && !found; ++i) {
          Set<ObjectName> names = mBeanServer.queryNames(new ObjectName("*.current-offsets.foo-0:*"), null);
          for (ObjectName n : names) {
            Object o = mBeanServer.getAttribute(n, "Value");
            if (o instanceof Long && (Long) o > 0) {
              found = true;
            }
          }
          Thread.sleep(retryDelay);
        }
        assertTrue("The Kafka consumer offset makes no progress", found);
      } finally {
        flink.stop();
      }

      SimpleConsumer consumer = null;
      try {
        consumer = new SimpleConsumer("127.0.0.1", cluster.getKafkaServerPort(0),
            DEFAULT_SO_TIMEOUT, DEFAULT_BUFFER_SIZE, "foo");
        FetchRequest req = new FetchRequestBuilder().addFetch(sinkTopic, 0, 0, DEFAULT_BUFFER_SIZE).build();
        FetchResponse resp = consumer.fetch(req);
        ByteBufferMessageSet ms = resp.messageSet(sinkTopic, 0);
        assertTrue(ms.validBytes() > 0);
      } finally {
        if (consumer != null) {
          consumer.close();
        }
      }
    }
  }

  private static KafkaProducer<byte[], byte[]> getProducer(String brokerList) {
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    return new KafkaProducer<>(prop);
  }

  private static ExternalCatalogTable mockExternalCatalogTable(String topic, String brokerAddress) {
    TableSchema schema = new TableSchema(new String[] {"foo"}, new TypeInformation[] {INT_TYPE_INFO});
    ConnectorDescriptor descriptor = new ConnectorDescriptor("kafka+json", 1, false) {
      @Override
      public void addConnectorProperties(DescriptorProperties properties) {
        properties.putTableSchema(TOPIC_SCHEMA_KEY, schema);
        properties.putString(TOPIC_NAME_KEY, topic);
        properties.putString(KAFKA_CONFIG_PREFIX + "." + ConsumerConfig.GROUP_ID_CONFIG, "foo");
        properties.putString(KAFKA_CONFIG_PREFIX + "." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        properties.putString(KAFKA_CONFIG_PREFIX + "." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      }
    };

    return new ExternalCatalogTable(descriptor, Option.empty(), Option.empty(), Option.empty(), Option.empty());
  }
}
