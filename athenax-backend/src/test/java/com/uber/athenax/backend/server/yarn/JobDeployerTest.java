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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import scala.concurrent.Future$;

import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class JobDeployerTest {

  @Test
  public void testDeployerWithIsolatedConfiguration() throws Exception {
    YarnClusterConfiguration clusterConf = mock(YarnClusterConfiguration.class);
    doReturn(new YarnConfiguration()).when(clusterConf).conf();
    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    Configuration flinkConf = new Configuration();
    YarnClient client = mock(YarnClient.class);
    JobDeployer deploy = new JobDeployer(clusterConf, client, executor, flinkConf);
    AthenaXYarnClusterDescriptor desc = mock(AthenaXYarnClusterDescriptor.class);

    YarnClusterClient clusterClient = mock(YarnClusterClient.class);
    doReturn(clusterClient).when(desc).deploy();

    ActorGateway actorGateway = mock(ActorGateway.class);
    doReturn(actorGateway).when(clusterClient).getJobManagerGateway();
    doReturn(Future$.MODULE$.successful(null)).when(actorGateway).ask(any(), any());

    JobGraph jobGraph = mock(JobGraph.class);
    doReturn(JobID.generate()).when(jobGraph).getJobID();
    deploy.start(desc, jobGraph);

    verify(clusterClient).runDetached(jobGraph, null);
  }
}
