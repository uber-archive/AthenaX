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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.messages.ShutdownClusterAfterJob;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link JobDeployer} takes a {@link JobGraph} and executes it on YARN.
 *
 * <p>The current deployment model closely follow the
 * <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/yarn_setup.html">Flink On YARN</a>
 * set ups. Each job has its own dedicated Flink cluster that is spawned on YARN.
 * The deployment model needs to be revisited once FLIP-6 has been landed.</p>
 *
 */
class JobDeployer {
  private static final FiniteDuration AKKA_TIMEOUT = new FiniteDuration(1, TimeUnit.MINUTES);
  private final YarnClusterConfiguration clusterConf;
  private final YarnClient yarnClient;
  private final ScheduledExecutorService executor;
  private final Configuration flinkConf;

  JobDeployer(YarnClusterConfiguration clusterConf, YarnClient yarnClient,
              ScheduledExecutorService executor, Configuration flinkConf) {
    this.clusterConf = clusterConf;
    this.executor = executor;
    this.flinkConf = flinkConf;
    this.yarnClient = yarnClient;
  }

  ApplicationId createApplication() throws IOException, YarnException {
    YarnClientApplication app = yarnClient.createApplication();
    return app.getApplicationSubmissionContext().getApplicationId();
  }

  void start(JobGraph job, JobConf desc) throws Exception {
    AthenaXYarnClusterDescriptor descriptor =
        new AthenaXYarnClusterDescriptor(clusterConf, yarnClient, flinkConf, desc);
    start(descriptor, job);
  }

  @VisibleForTesting
  void start(AthenaXYarnClusterDescriptor descriptor, JobGraph job) throws Exception {
    ClusterClient<ApplicationId> client = descriptor.deploy();
    try {
      client.runDetached(job, null);
      stopAfterJob(client, job.getJobID());
    } finally {
      client.shutdown();
    }
  }

  private void stopAfterJob(ClusterClient client, JobID jobID) {
    Preconditions.checkNotNull(jobID, "The job id must not be null");
    try {
      Future<Object> replyFuture =
          client.getJobManagerGateway().ask(
              new ShutdownClusterAfterJob(jobID),
              AKKA_TIMEOUT);
      Await.ready(replyFuture, AKKA_TIMEOUT);
    } catch (Exception e) {
      throw new RuntimeException("Unable to tell application master to stop"
          + " once the specified job has been finished", e);
    }
  }
}
