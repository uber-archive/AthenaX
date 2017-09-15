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

package com.uber.athenax.backend.server.jobs;

import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.server.yarn.InstanceInfo;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.ACCEPTED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW_SAVING;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.RUNNING;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.SUBMITTED;

final class JobWatcherUtil {
  private static final EnumSet<YarnApplicationState> ALIVE_STATE =
      EnumSet.of(NEW, NEW_SAVING, SUBMITTED, ACCEPTED, RUNNING);

  private JobWatcherUtil() {
  }

  static final class StateView {
    // List of jobs
    private final Map<UUID, JobDefinition> jobs;
    // List of instances
    private final Map<UUID, InstanceInfo> instances;
    // Instance ID -> JobDefinition ID
    private final Map<UUID, UUID> instanceToJob;
    // JobDefinition ID -> List<Instance>
    private final Map<UUID, List<InstanceInfo>> jobInstances;

    private StateView(Map<UUID, JobDefinition> jobs,
                      Map<UUID, InstanceInfo> instances,
                      Map<UUID, UUID> instanceToJob,
                      Map<UUID, List<InstanceInfo>> jobInstances) {
      this.jobs = jobs;
      this.instances = instances;
      this.instanceToJob = instanceToJob;
      this.jobInstances = jobInstances;
    }

    Map<UUID, JobDefinition> jobs() {
      return jobs;
    }

    Map<UUID, InstanceInfo> instances() {
      return instances;
    }

    Map<UUID, UUID> instanceToJob() {
      return instanceToJob;
    }

    Map<UUID, List<InstanceInfo>> jobInstances() {
      return jobInstances;
    }
  }

  static HealthCheckReport computeHealthCheckReport(Map<UUID, JobDefinition> jobs,
                                                    Map<UUID, InstanceInfo> instances) {
    JobWatcherUtil.StateView v = JobWatcherUtil.computeState(jobs, instances);
    HealthCheckReport res = new HealthCheckReport();

    for (Map.Entry<UUID, List<InstanceInfo>> e : v.jobInstances.entrySet()) {
      JobDefinition job = v.jobs.get(e.getKey());

      // Spurious instances
      if (job == null) {
        res.spuriousInstances().addAll(e.getValue());
        continue;
      }

      Map<InstanceInfo, JobDefinitionDesiredstate> instanceState = new HashMap<>();
      e.getValue().forEach(x -> instanceState.put(x, JobWatcherUtil.computeActualState(x)));
      List<JobDefinitionDesiredstate> desiredState = new ArrayList<>(job.getDesiredState());
      List<InstanceInfo> healthyInstances = new ArrayList<>();

      for (Map.Entry<InstanceInfo, JobDefinitionDesiredstate> entry : instanceState.entrySet()) {
        if (desiredState.contains(entry.getValue())) {
          desiredState.remove(entry.getValue());
          healthyInstances.add(entry.getKey());
        }
      }

      List<InstanceInfo> allInstances = e.getValue();
      allInstances.removeAll(healthyInstances);
      res.instancesWithDifferentParameters().addAll(allInstances);

      if (!desiredState.isEmpty()) {
        res.instancesToStart().put(
            new ExtendedJobDefinition().uuid(e.getKey())
            .definition(job),
            desiredState);
      }
    }
    return res;
  }

  static StateView computeState(Map<UUID, JobDefinition> jobs, Map<UUID, InstanceInfo> instances) {

    // Instance ID -> JobDefinition ID
    HashMap<UUID, UUID> instanceToJob = new HashMap<>();
    HashMap<UUID, List<InstanceInfo>> jobInstances = new HashMap<>();

    for (Map.Entry<UUID, InstanceInfo> e : instances.entrySet()) {
      YarnApplicationState state = YarnApplicationState.valueOf(e.getValue().status().getState().toString());
      if (!isInstanceAlive(state)) {
        continue;
      }
      UUID jobId = e.getValue().metadata().jobDefinition();
      UUID instanceId = e.getKey();
      instanceToJob.put(instanceId, jobId);
      if (!jobInstances.containsKey(jobId)) {
        jobInstances.put(jobId, new ArrayList<>());
      }
      jobInstances.get(jobId).add(e.getValue());
    }
    jobs.keySet().stream().filter(x -> !jobInstances.containsKey(x))
        .forEach(x -> jobInstances.put(x, Collections.emptyList()));
    return new StateView(jobs, instances, instanceToJob, jobInstances);
  }

  static JobDefinitionDesiredstate computeActualState(InstanceInfo info) {
    JobDefinitionResource r = new JobDefinitionResource()
        .memory(info.status().getAllocatedMB())
        .vCores(info.status().getAllocatedVCores());
    JobDefinitionDesiredstate s = new JobDefinitionDesiredstate()
        .clusterId(info.clusterName())
        .resource(r);
    return s;
  }

  static HashMap<UUID, JobDefinition> listJobs(JobStore jobStore) throws IOException {
    List<ExtendedJobDefinition> jobs = jobStore.listAll();
    HashMap<UUID, JobDefinition> results = new HashMap<>();
    jobs.forEach(x -> results.put(x.getUuid(), x.getDefinition()));
    return results;
  }

  private static boolean isInstanceAlive(YarnApplicationState state) {
    return ALIVE_STATE.contains(state);
  }
}
