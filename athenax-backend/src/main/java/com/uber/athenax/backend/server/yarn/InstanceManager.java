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

import com.uber.athenax.backend.api.InstanceState;
import com.uber.athenax.backend.api.InstanceStatus;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.InstanceStateUpdateListener;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.uber.athenax.backend.server.AthenaXExtraConfigOptions.INSTANCE_MANAGER_RESCAN_INTERVAL;
import static com.uber.athenax.backend.server.yarn.AthenaXYarnClusterDescriptor.ATHENAX_APPLICATION_TYPE;

/**
 * InstanceManager manages the instances of jobs that are deployed and executed on YARN clusters.
 *
 * <p>InstanceManager only contains soft states, that is, it can repopulate its state through
 * scanning the jobs on the YARN clusters. It periodically scans through all applications to update
 * all state.</p>
 *
 */
public class InstanceManager implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceManager.class);
  private final long rescanInterval;
  private final Map<String, ClusterInfo> clusters;
  private final ScheduledExecutorService executor;
  private final InstanceStateUpdateListener listener;
  private final Configuration flinkConf = new Configuration();
  private boolean running;

  private final Runnable thunk = new Runnable() {
    @Override
    public void run() {
      try {
        scanAll();
      } catch (Throwable e) {
        LOG.warn("Failed to scan the applications {}", e);
      } finally {
        if (running) {
          executor.schedule(thunk, rescanInterval, TimeUnit.MILLISECONDS);
        }
      }
    }
  };

  private final AtomicReference<ConcurrentHashMap<UUID, InstanceInfo>> instances
      = new AtomicReference<>(new ConcurrentHashMap<>());

  @VisibleForTesting
  InstanceManager(
      Map<String, ClusterInfo> clusters,
      InstanceStateUpdateListener listener,
      ScheduledExecutorService executor,
      long rescanInterval) {
    this.clusters = Collections.unmodifiableMap(clusters);
    this.listener = listener;
    this.executor = executor;
    this.rescanInterval = rescanInterval;
  }

  @VisibleForTesting
  public static InstanceManager create(
      AthenaXConfiguration conf,
      InstanceStateUpdateListener listener,
      ScheduledExecutorService executor) {
    HashMap<String, ClusterInfo> c = new HashMap<>();
    for (Map.Entry<String, AthenaXConfiguration.YarnCluster> e : conf.clusters().entrySet()) {
      ClusterInfo ci = new ClusterInfo(e.getKey(), e.getValue().toYarnClusterConfiguration());
      c.put(e.getKey(), ci);
    }
    return new InstanceManager(c, listener, executor, conf.getExtraConfLong(INSTANCE_MANAGER_RESCAN_INTERVAL));
  }

  /**
   * Return the information of the job instance.
   */
  public InstanceStatus getInstanceStatus(UUID uuid) {
    InstanceInfo info = instances().get(uuid);
    if (info == null) {
      return null;
    }
    return info.status();
  }

  public InstanceState getInstanceState(UUID uuid) {
    InstanceStatus stat = getInstanceStatus(uuid);
    if (stat == null) {
      return null;
    }

    InstanceState state = new InstanceState()
        .state(InstanceState.StateEnum.fromValue(stat.getState().toString()));
    return state;
  }

  public void changeState(UUID uuid, InstanceState desiredState) throws IOException, YarnException {
    if (desiredState == null || desiredState.getState() != InstanceState.StateEnum.KILLED) {
      throw new UnsupportedOperationException();
    }

    InstanceInfo info = instances().get(uuid);
    if (info == null) {
      return;
    }

    ClusterInfo cluster = clusters.get(info.clusterName());
    Preconditions.checkNotNull(cluster);
    cluster.client().killApplication(info.appId());
  }

  public ConcurrentHashMap<UUID, InstanceInfo> instances() {
    return instances.get();
  }

  public void start() {
    running = true;
    executor.submit(thunk);
  }

  @Override
  public void close() throws Exception {
    running = false;
  }

  public void killYarnApplication(String clusterName, ApplicationId appId) throws IOException, YarnException {
    ClusterInfo cluster = clusters.get(clusterName);
    if (cluster == null) {
      throw new IllegalArgumentException("Invalid cluster name " + clusterName);
    }
    cluster.client().killApplication(appId);
  }

  public Map.Entry<UUID, ApplicationId> instantiate(
      JobDefinitionDesiredstate state,
      UUID jobUUID,
      JobCompilationResult job) throws Exception {
    String clusterName = state.getClusterId();
    ClusterInfo cluster = clusters.get(clusterName);
    if (cluster == null) {
      throw new IllegalArgumentException("Invalid cluster name " + clusterName);
    }

    JobDeployer deployer = new JobDeployer(cluster.conf(), cluster.client(), executor, flinkConf);
    ApplicationId appId = deployer.createApplication();
    UUID instanceUUID = UUID.randomUUID();
    InstanceMetadata md = new InstanceMetadata(instanceUUID, jobUUID);

    JobConf jobConf = new JobConf(
        appId,
        jobUUID.toString(),
        job.additionalJars(),
        state.getResource().getQueue(),
        state.getResource().getVCores(),
        state.getResource().getMemory(),
        md);

    LOG.info("Instantiating job {} at {}", jobUUID, clusterName);
    deployer.start(job.jobGraph(), jobConf);
    return new AbstractMap.SimpleImmutableEntry<>(instanceUUID, appId);
  }

  /**
   * Scan all clusters to recover the soft state.
   */
  @VisibleForTesting
  void scanAll() throws IOException, YarnException {
    ConcurrentHashMap<UUID, InstanceInfo> newInstances = new ConcurrentHashMap<>();
    for (ClusterInfo cluster : clusters.values()) {
      List<ApplicationReport> reports = cluster.client()
          .getApplications(Collections.singleton(ATHENAX_APPLICATION_TYPE));
      for (ApplicationReport report : reports) {
        InstanceInfo instance = Utils.extractInstanceInfo(cluster.name(), report);
        if (instance == null) {
          LOG.warn("Failed to retrieve instance info for {}:{}", cluster.name(), report.getApplicationId());
        } else {
          newInstances.put(instance.metadata().uuid(), instance);
        }
      }
    }
    LOG.info("Inspected {} active instances", newInstances.size());
    instances.set(newInstances);
    listener.onUpdatedInstances(newInstances);
  }
}
