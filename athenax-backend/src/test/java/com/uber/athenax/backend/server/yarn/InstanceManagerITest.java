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

import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.server.AthenaXExtraConfigOptions;
import com.uber.athenax.backend.server.InstanceStateUpdateListener;
import com.uber.athenax.backend.server.MiniAthenaXCluster;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class InstanceManagerITest {

  private static final String CLUSTER_NAME = "foo";

  @Test
  public void testInstantiation() throws Exception {
    Configuration flinkConf = new Configuration();
    flinkConf.setString(JobManagerOptions.ADDRESS, "localhost");

    try (MiniAthenaXCluster cluster = new MiniAthenaXCluster(InstanceManagerITest.class.getSimpleName())) {
      cluster.start();
      YarnClusterConfiguration clusterConf = cluster.getYarnClusterConf();

      JobDefinitionDesiredstate state = mock(JobDefinitionDesiredstate.class);
      JobDefinitionResource resource = mock(JobDefinitionResource.class);
      doReturn(1L).when(resource).getVCores();
      doReturn(1024L).when(resource).getMemory();
      doReturn(CLUSTER_NAME).when(state).getClusterId();
      doReturn(resource).when(state).getResource();

      JobCompilationResult res = mock(JobCompilationResult.class);
      doReturn(Collections.emptyList()).when(res).additionalJars();
      doReturn(JobITestUtil.trivialJobGraph()).when(res).jobGraph();

      try (YarnClient client = YarnClient.createYarnClient()) {
        ClusterInfo clusterInfo = new ClusterInfo(CLUSTER_NAME, clusterConf, client);
        YarnConfiguration conf = cluster.getYarnConfiguration();
        client.init(conf);
        client.start();
        UUID jobUUID = UUID.randomUUID();
        try (InstanceManager manager = new InstanceManager(
            Collections.singletonMap(CLUSTER_NAME, clusterInfo),
            mock(InstanceStateUpdateListener.class),
            mock(ScheduledExecutorService.class),
            AthenaXExtraConfigOptions.INSTANCE_MANAGER_RESCAN_INTERVAL.defaultValue())) {

          Map.Entry<UUID, ApplicationId> id = manager.instantiate(state, jobUUID, res);
          YarnApplicationState yarnState = MiniAthenaXCluster.pollFinishedApplicationState(client, id.getValue());
          assertEquals(FINISHED, yarnState);

          manager.scanAll();
          Map<UUID, InstanceInfo> instances = manager.instances();
          assertEquals(1, instances.size());
          assertEquals(jobUUID, instances.get(id.getKey()).metadata().jobDefinition());
        }
      }
    }
  }
}
