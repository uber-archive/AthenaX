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

package com.uber.athenax.backend.server.yarn;

import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.server.MiniAthenaXCluster;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.junit.Assert.assertEquals;

public class JobDeployerITest {

  @Test
  public void testCreateAthenaXCluster() throws Exception {
    ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
    Configuration flinkConf = new Configuration();
    flinkConf.setString(JobManagerOptions.ADDRESS, "localhost");

    try (MiniAthenaXCluster cluster = new MiniAthenaXCluster(JobDeployerITest.class.getSimpleName())) {
      cluster.start();
      YarnConfiguration conf = cluster.getYarnConfiguration();
      YarnClusterConfiguration clusterConf = cluster.getYarnClusterConf();

      final ApplicationId appId;
      try (YarnClient client = YarnClient.createYarnClient()) {
        client.init(conf);
        client.start();

        JobDeployer deployer = new JobDeployer(clusterConf, client, executor, flinkConf);
        appId = deployer.createApplication();
        InstanceMetadata md = new InstanceMetadata(UUID.randomUUID(), UUID.randomUUID());
        JobDefinitionResource resource = new JobDefinitionResource()
            .queue(null).vCores(1L).executionSlots(1L).memory(2048L);
        JobConf jobConf = new JobConf(appId, "test", Collections.emptyList(), resource, md);
        deployer.start(JobITestUtil.trivialJobGraph(), jobConf);

        YarnApplicationState state = MiniAthenaXCluster.pollFinishedApplicationState(client, appId);
        assertEquals(FINISHED, state);
      }
    }
  }
}
