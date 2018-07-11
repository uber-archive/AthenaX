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
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.api.client.Configuration;
import com.uber.athenax.backend.api.client.JobsApi;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.MiniAthenaXCluster;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.WebServer;
import com.uber.athenax.vm.connectors.kafka.MiniKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static com.uber.athenax.tests.ITestUtil.DEST_TOPIC;
import static com.uber.athenax.tests.ITestUtil.brokerAddress;
import static com.uber.athenax.tests.ITestUtil.generateConf;
import static com.uber.athenax.tests.ITestUtil.getConsumer;
import static com.uber.athenax.tests.ITestUtil.setUpKafka;

public class StartJobITest {
  private static final Logger LOG = LoggerFactory.getLogger(StartJobITest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testStartJob() throws Exception {
    try (MiniKafkaCluster kafkaCluster = new MiniKafkaCluster.Builder().newServer("0").build();
         MiniAthenaXCluster cluster = new MiniAthenaXCluster(StartJobITest.class.getSimpleName())) {
      kafkaCluster.start();
      setUpKafka(kafkaCluster);
      cluster.start();
      AthenaXConfiguration conf = generateConf(cluster);
      ServerContext.INSTANCE.initialize(conf);
      ServerContext.INSTANCE.start();
      try (WebServer server = new WebServer(URI.create("http://localhost:0"))) {
        server.start();
        Configuration.getDefaultApiClient().setBasePath(String.format("http://localhost:%d%s", server.port(), WebServer.BASE_PATH));
        LOG.info("AthenaX server listening on http://localhost:{}", server.port());

        JobsApi api = new JobsApi();
        String uuid = api.allocateNewJob().getJobUuid();
        JobDefinitionDesiredstate state = new JobDefinitionDesiredstate()
            .clusterId("foo")
            .resource(new JobDefinitionResource().vCores(1L).memory(2048L).executionSlots(1L));
        JobDefinition job = new JobDefinition()
            .query("SELECT * FROM input.foo")
            .addDesiredStateItem(state);
        api.updateJob(UUID.fromString(uuid), job);

        try (KafkaConsumer<byte[], byte[]> consumer = getConsumer("observer", brokerAddress())) {
          consumer.subscribe(Collections.singletonList(DEST_TOPIC));
          boolean found = false;
          while (!found) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
            for (ConsumerRecord<byte[], byte[]> r : records.records(DEST_TOPIC)) {
              @SuppressWarnings("unchecked")
              Map<String, Object> m = MAPPER.readValue(r.value(), Map.class);
              if ((Integer) m.get("id") == 2) {
                found = true;
              }
            }
          }
          ServerContext.INSTANCE.executor().shutdown();
          ServerContext.INSTANCE.instanceManager().close();
        }
      }
    }
  }
}
