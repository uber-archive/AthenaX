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

package com.uber.athenax.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.MiniAthenaXCluster;
import com.uber.athenax.vm.api.AthenaXTableCatalog;
import com.uber.athenax.vm.api.AthenaXTableCatalogProvider;
import com.uber.athenax.vm.connectors.kafka.KafkaTestUtil;
import com.uber.athenax.vm.connectors.kafka.MiniKafkaCluster;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.uber.athenax.backend.server.AthenaXExtraConfigOptions.INSTANCE_MANAGER_RESCAN_INTERVAL;
import static com.uber.athenax.backend.server.AthenaXExtraConfigOptions.JOBSTORE_LEVELDB_FILE;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorConfigKeys.TOPIC_NAME_KEY;

final class ITestUtil {
  static final String DEST_TOPIC = "bar";
  static final String SOURCE_TOPIC = "foo";

  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static String brokerAddress;

  private ITestUtil() {
  }

  static class KafkaInputExternalCatalogTable extends ExternalCatalogTable implements Serializable {
    private static final TableSchema SCHEMA = new TableSchema(
        new String[] {"id"},
        new TypeInformation<?>[] {BasicTypeInfo.INT_TYPE_INFO});

    KafkaInputExternalCatalogTable(Map<String, String> properties) {
      super("kafka+json", SCHEMA, properties, null, null, null, null);
    }
  }

  public static class KafkaCatalog implements AthenaXTableCatalog {
    private static final long serialVersionUID = -1L;

    private final String broker;
    private final List<String> availableTables;

    KafkaCatalog(String broker, List<String> availableTables) {
      this.broker = broker;
      this.availableTables = availableTables;
    }

    @Override
    public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
      Map<String, String> sourceTableProp = new HashMap<>();
      sourceTableProp.put(KAFKA_CONFIG_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, tableName);
      sourceTableProp.put(KAFKA_CONFIG_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
      sourceTableProp.put(KAFKA_CONFIG_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      sourceTableProp.put(TOPIC_NAME_KEY, tableName);
      return new KafkaInputExternalCatalogTable(sourceTableProp);
    }

    @Override
    public List<String> listTables() {
      return availableTables;
    }

    @Override
    public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
      throw new CatalogNotExistException(dbName);
    }

    @Override
    public List<String> listSubCatalogs() {
      return Collections.emptyList();
    }
  }

  public static class CatalogProvider implements AthenaXTableCatalogProvider {
    @Override
    public Map<String, AthenaXTableCatalog> getInputCatalog(String cluster) {
      Preconditions.checkNotNull(brokerAddress);
      return Collections.singletonMap(
          "input",
          new KafkaCatalog(brokerAddress, Collections.singletonList(SOURCE_TOPIC)));
    }

    @Override
    public AthenaXTableCatalog getOutputCatalog(String cluster, List<String> outputs) {
      Preconditions.checkNotNull(brokerAddress);
      return new KafkaCatalog(brokerAddress, Collections.singletonList(DEST_TOPIC));
    }
  }

  protected static AthenaXConfiguration generateConf(MiniAthenaXCluster cluster) throws IOException {
    StringBuffer sb = new StringBuffer();
    sb.append(String.format("catalog.impl: %s.%s$%s\n",
        CatalogProvider.class.getPackage().getName(),
        ITestUtil.class.getSimpleName(),
        CatalogProvider.class.getSimpleName()))
        .append("athenax.master.uri: http://localhost:0\n")
        .append(cluster.generateYarnClusterConfContent("foo"))
        .append("extras:\n")
        .append(String.format("  %s: 5000\n", INSTANCE_MANAGER_RESCAN_INTERVAL.key()))
        .append(String.format("  %s: %s\n", JOBSTORE_LEVELDB_FILE.key(), new File(cluster.workDir(), "db")));
    return AthenaXConfiguration.loadContent(sb.toString());
  }

  protected static void setUpKafka(MiniKafkaCluster cluster) throws IOException, InterruptedException {
    String zkAddress = "127.0.0.1:" + cluster.getZkServer().getPort();
    brokerAddress = "127.0.0.1:" + cluster.getKafkaServerPort(0);
    KafkaTestUtil.createKafkaTopicIfNecessary("zk://" + zkAddress, 1, 1, "foo");
    KafkaTestUtil.createKafkaTopicIfNecessary("zk://" + zkAddress, 1, 1, "bar");

    // Wait until Kafka / ZK stabilizes
    Thread.sleep(STABILIZE_SLEEP_DELAYS);

    try (KafkaProducer<byte[], byte[]> producer = getProducer(brokerAddress)) {
      producer.send(new ProducerRecord<>(SOURCE_TOPIC, MAPPER.writeValueAsBytes(Collections.singletonMap("id", 1))));
      producer.send(new ProducerRecord<>(SOURCE_TOPIC, MAPPER.writeValueAsBytes(Collections.singletonMap("id", 2))));
    }
  }

  private static KafkaProducer<byte[], byte[]> getProducer(String brokerList) {
    Properties prop = new Properties();
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    return new KafkaProducer<>(prop);
  }

  static KafkaConsumer<byte[], byte[]> getConsumer(String groupName, String brokerList) {
    Properties prop = new Properties();
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
    return new KafkaConsumer<>(prop);
  }

  static String brokerAddress() {
    return brokerAddress;
  }
}
