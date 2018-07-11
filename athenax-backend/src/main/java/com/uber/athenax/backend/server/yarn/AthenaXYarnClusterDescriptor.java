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

import com.google.common.collect.Iterables;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.Utils;
import org.apache.flink.yarn.YarnApplicationMasterRunner;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW;

/**
 * {@link AthenaXYarnClusterDescriptor} describes how to spawn the JobManager on YARN. This is essentially a
 * stripped down version of {@link org.apache.flink.yarn.YarnClusterDescriptor} that is customized for AthenaX.
 *
 * <p>NOTE: There is no support for secure YARN cluster yet.</p>
 */
class AthenaXYarnClusterDescriptor extends AbstractYarnClusterDescriptor {
  static final String ATHENAX_APPLICATION_TYPE = "AthenaX+Flink";
  private static final Logger LOG = LoggerFactory.getLogger(AthenaXYarnClusterDescriptor.class);
  private static final int MAX_ATTEMPT = 1;
  private static final long DEPLOY_TIMEOUT_MS = 600 * 1000;
  private static final long RETRY_DELAY_MS = 250;
  private static final ScheduledExecutorService YARN_POLL_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

  private final YarnClusterConfiguration clusterConf;
  private final YarnClient yarnClient;
  private final JobConf job;
  private final Configuration flinkConf;

  AthenaXYarnClusterDescriptor(
      YarnClusterConfiguration clusterConf,
      YarnClient yarnClient,
      Configuration flinkConf,
      JobConf job) {
    super(new Configuration(flinkConf),
        clusterConf.conf(),
        "",
        yarnClient,
        true);
    this.clusterConf = clusterConf;
    this.yarnClient = yarnClient;
    this.flinkConf = flinkConf;
    this.job = job;
  }

  ClusterClient<ApplicationId> deploy() {
    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    context.setApplicationId(job.yarnAppId());
    ApplicationReport report;
    try {
      report = startAppMaster(context);

      Configuration conf = getFlinkConfiguration();
      conf.setString(JobManagerOptions.ADDRESS.key(), report.getHost());
      conf.setInteger(JobManagerOptions.PORT.key(), report.getRpcPort());

      return createYarnClusterClient(this,
          (int) job.taskManagerCount(),
          (int) job.slotCountPerTaskManager(),
          report,
          conf,
          false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ApplicationReport startAppMaster(ApplicationSubmissionContext appContext) throws Exception {
    appContext.setMaxAppAttempts(MAX_ATTEMPT);

    Map<String, LocalResource> localResources = new HashMap<>();
    Set<Path> shippedPaths = new HashSet<>();
    collectLocalResources(localResources, shippedPaths);
    final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
        this.getYarnSessionClusterEntrypoint(),
        false,
        true,
        false, (int) job.taskManagerMemoryMb());

    amContainer.setLocalResources(localResources);

    final String classPath = localResources.keySet().stream().collect(Collectors.joining(File.pathSeparator));
    final String shippedFiles = shippedPaths.stream().map(x -> x.getName() + "=" + x.toString())
        .collect(Collectors.joining(","));

    // Setup CLASSPATH and environment variables for ApplicationMaster
    ApplicationId appId = appContext.getApplicationId();
    final Map<String, String> appMasterEnv = setUpAmEnvironment(
        appId,
        classPath,
        shippedFiles,
        getDynamicPropertiesEncoded()
    );

    amContainer.setEnvironment(appMasterEnv);

    // Set up resource type requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(getFlinkConfiguration()
        .getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY));
    capability.setVirtualCores(1);

    appContext.setApplicationName(job.name());
    appContext.setApplicationType(ATHENAX_APPLICATION_TYPE);
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setApplicationTags(Collections.singleton(job.metadata().serialize()));
    if (job.queue() != null) {
      appContext.setQueue(job.queue());
    }

    LOG.info("Submitting application master {}", appId);
    yarnClient.submitApplication(appContext);

    PollDeploymentStatus poll = new PollDeploymentStatus(appId);
    YARN_POLL_EXECUTOR.submit(poll);
    try {
      return poll.result.get();
    } catch (ExecutionException e) {
      LOG.warn("Failed to deploy {}, cause: {}", appId.toString(), e.getCause());
      yarnClient.killApplication(appId);
      throw (Exception) e.getCause();
    }
  }

  private void collectLocalResources(
      Map<String, LocalResource> resources,
      Set<Path> shippedPaths
  ) throws IOException {
    for (Path p : clusterConf.resourcesToLocalize()) {
      resources.put(p.getName(), toLocalResource(p, LocalResourceVisibility.APPLICATION));
    }

    for (Path p : Iterables.concat(clusterConf.systemJars(), job.userProvidedJars())) {
      String name = p.getName();
      if (resources.containsKey(name)) {
        throw new IllegalArgumentException("Duplicated name in the shipped files " + p);
      }
      resources.put(name, toLocalResource(p, LocalResourceVisibility.APPLICATION));
      shippedPaths.add(p);
    }
  }

  private LocalResource toLocalResource(Path path, LocalResourceVisibility visibility) throws IOException {
    FileSystem fs = path.getFileSystem(clusterConf.conf());
    FileStatus stat = fs.getFileStatus(path);
    return LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromPath(path),
            LocalResourceType.FILE,
            visibility,
            stat.getLen(), stat.getModificationTime()
    );
  }

  private Map<String, String> setUpAmEnvironment(
      ApplicationId appId,
      String amClassPath,
      String shipFiles,
      String dynamicProperties) throws IOException {
    final Map<String, String> env = new HashMap<>();

    // set Flink app class path
    env.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, amClassPath);

    // set Flink on YARN internal configuration values
    env.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(job.taskManagerCount()));
    env.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(job.taskManagerMemoryMb()));
    env.put(YarnConfigKeys.FLINK_JAR_PATH, clusterConf.flinkUberJar().toString());
    env.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
    env.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, clusterConf.homeDir());
    env.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, shipFiles);
    env.put(YarnConfigKeys.ENV_SLOTS, "-1");
    env.put(YarnConfigKeys.ENV_DETACHED, "true");

    // https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
    env.put(YarnConfigKeys.ENV_HADOOP_USER_NAME,
        UserGroupInformation.getCurrentUser().getUserName());

    if (dynamicProperties != null) {
      env.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicProperties);
    }

    // set classpath from YARN configuration
    Utils.setupYarnClassPath(clusterConf.conf(), env);

    return env;
  }

  @Override
  protected String getYarnSessionClusterEntrypoint() {
    return YarnApplicationMasterRunner.class.getCanonicalName();
  }

  @Override
  protected String getYarnJobClusterEntrypoint() {
    return null;
  }

  @Override
  protected ClusterClient<ApplicationId> createYarnClusterClient(
      AbstractYarnClusterDescriptor clusterDescriptor,
      int numberTaskManagers,
      int slotPerTaskManager,
      ApplicationReport applicationReport,
      Configuration configuration,
      boolean isNewlyCreatedCluster) throws Exception {
    return new YarnClusterClient(
        clusterDescriptor,
        numberTaskManagers,
        slotPerTaskManager,
        applicationReport,
        configuration,
        isNewlyCreatedCluster);
  }

  @Override
  public ClusterClient<ApplicationId> deployJobCluster(
      ClusterSpecification clusterSpecification,
      JobGraph jobGraph,
      boolean b) throws ClusterDeploymentException {
    return null;
  }

  private final class PollDeploymentStatus implements Runnable {
    private final CompletableFuture<ApplicationReport> result = new CompletableFuture<>();
    private final ApplicationId appId;
    private YarnApplicationState lastAppState = NEW;
    private long startTime;

    private PollDeploymentStatus(ApplicationId appId) {
      this.appId = appId;
    }

    @Override
    public void run() {
      if (startTime == 0) {
        startTime = System.currentTimeMillis();
      }

      try {
        ApplicationReport report = poll();
        if (report == null) {
          YARN_POLL_EXECUTOR.schedule(this, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        } else {
          result.complete(report);
        }
      } catch (YarnException | IOException e) {
        result.completeExceptionally(e);
      }
    }

    private ApplicationReport poll() throws IOException, YarnException {
      ApplicationReport report;
      report = yarnClient.getApplicationReport(appId);
      YarnApplicationState appState = report.getYarnApplicationState();
      LOG.debug("Application State: {}", appState);

      switch (appState) {
        case FAILED:
        case FINISHED:
          //TODO: the finished state may be valid in flip-6
        case KILLED:
          throw new IOException("The YARN application unexpectedly switched to state "
              + appState + " during deployment. \n"
              + "Diagnostics from YARN: " + report.getDiagnostics() + "\n"
              + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
              + "yarn logs -applicationId " + appId);
          //break ..
        case RUNNING:
          LOG.info("YARN application has been deployed successfully.");
          break;
        default:
          if (appState != lastAppState) {
            LOG.info("Deploying cluster, current state " + appState);
          }
          lastAppState = appState;
          if (System.currentTimeMillis() - startTime > DEPLOY_TIMEOUT_MS) {
            throw new RuntimeException(String.format("Deployment took more than %d seconds. "
                + "Please check if the requested resources are available in the YARN cluster", DEPLOY_TIMEOUT_MS));
          }
          return null;
      }
      return report;
    }
  }
}
