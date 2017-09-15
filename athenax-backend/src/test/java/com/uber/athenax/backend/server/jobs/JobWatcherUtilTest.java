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

import com.uber.athenax.backend.api.InstanceStatus;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.api.JobDefinitionResource;
import com.uber.athenax.backend.server.yarn.InstanceInfo;
import com.uber.athenax.backend.server.yarn.InstanceMetadata;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class JobWatcherUtilTest {

  @Test
  public void testComputeState() {
    HashMap<UUID, JobDefinition> jobMap = new HashMap<>();
    HashMap<UUID, InstanceInfo> instanceMap = new HashMap<>();
    mockState1(jobMap, instanceMap);

    JobWatcherUtil.StateView v = JobWatcherUtil.computeState(jobMap, instanceMap);

    HashMap<UUID, List<InstanceInfo>> jobInstances = new HashMap<>();
    HashMap<UUID, UUID> instanceToJob = new HashMap<>();

    for (Map.Entry<UUID, InstanceInfo> e : instanceMap.entrySet()) {
      UUID jobId = e.getValue().metadata().jobDefinition();
      instanceToJob.put(e.getKey(), jobId);
      List<InstanceInfo> li = jobInstances.getOrDefault(jobId, new ArrayList<>());
      li.add(e.getValue());
      jobInstances.put(jobId, li);
    }

    assertEquals(jobMap, v.jobs());
    assertEquals(instanceMap, v.instances());
    assertEquals(instanceToJob, v.instanceToJob());
    assertEquals(jobInstances, v.jobInstances());
  }

  @Test
  public void testKillingSpuriousInstances() {
    UUID instId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();

    InstanceInfo instance = mockInstanceInfo("foo", 1, 4096, jobId, InstanceStatus.StateEnum.ACCEPTED);

    HealthCheckReport report = JobWatcherUtil.computeHealthCheckReport(
        Collections.emptyMap(),
        Collections.singletonMap(instId, instance));
    assertEquals(1, report.spuriousInstances().size());
    instance = mockInstanceInfo("foo", 1, 4096, jobId, InstanceStatus.StateEnum.KILLED);
    report = JobWatcherUtil.computeHealthCheckReport(
        Collections.emptyMap(),
        Collections.singletonMap(instId, instance));
    assertTrue(report.spuriousInstances().isEmpty());
  }

  @Test
  public void testHealthyInstance() {
    UUID instId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();

    InstanceInfo instance = mockInstanceInfo("foo", 1, 4096, jobId, InstanceStatus.StateEnum.RUNNING);
    JobDefinition job = mockJobWithSingleInstance("foo", 1, 4096);

    HealthCheckReport report = JobWatcherUtil.computeHealthCheckReport(
        Collections.singletonMap(jobId, job), Collections.singletonMap(instId, instance));

    assertTrue(report.spuriousInstances().isEmpty());
    assertTrue(report.instancesWithDifferentParameters().isEmpty());
    assertTrue(report.instancesToStart().isEmpty());
  }

  @Test
  public void testMismatchedParameter() {
    UUID instId = UUID.randomUUID();
    UUID jobId = UUID.randomUUID();

    InstanceInfo instance = mockInstanceInfo("foo", 1, 4096, jobId, InstanceStatus.StateEnum.RUNNING);
    JobDefinition job = mockJobWithSingleInstance("foo", 1, 2048);

    HealthCheckReport report = JobWatcherUtil.computeHealthCheckReport(
        Collections.singletonMap(jobId, job), Collections.singletonMap(instId, instance));

    assertTrue(report.spuriousInstances().isEmpty());
    assertEquals(1, report.instancesWithDifferentParameters().size());
    assertEquals(1, report.instancesToStart().size());
  }

  @Test
  public void testStartJob() {
    UUID jobId = UUID.randomUUID();

    JobDefinition job = mockJobWithSingleInstance("foo", 1, 2048);

    HealthCheckReport report = JobWatcherUtil.computeHealthCheckReport(
        Collections.singletonMap(jobId, job), Collections.emptyMap());

    assertTrue(report.spuriousInstances().isEmpty());
    assertTrue(report.instancesWithDifferentParameters().isEmpty());
    assertEquals(1, report.instancesToStart().size());
  }

  private static JobDefinition mockJobWithSingleInstance(String clusterName, long vCore, long memory) {
    JobDefinitionDesiredstate desiredstate = mockDesiredState(clusterName, vCore, memory);
    JobDefinition job = mock(JobDefinition.class);
    doReturn(Collections.singletonList(desiredstate)).when(job).getDesiredState();
    return job;
  }

  private static InstanceInfo mockInstanceInfo(
      String clusterName,
      long vCore,
      long memory,
      UUID jobId,
      InstanceStatus.StateEnum state) {
    InstanceInfo instance = mock(InstanceInfo.class);
    InstanceMetadata md = mock(InstanceMetadata.class);
    doReturn(md).when(instance).metadata();
    doReturn(jobId).when(md).jobDefinition();

    InstanceStatus status = mock(InstanceStatus.class);
    doReturn(state).when(status).getState();
    doReturn(status).when(instance).status();

    doReturn(vCore).when(status).getAllocatedVCores();
    doReturn(memory).when(status).getAllocatedMB();
    doReturn(clusterName).when(instance).clusterName();
    return instance;
  }

  private static JobDefinitionDesiredstate mockDesiredState(String clusterName, long vCore, long memory) {
    return new JobDefinitionDesiredstate()
        .clusterId(clusterName)
        .resource(new JobDefinitionResource()
        .vCores(vCore)
        .memory(memory));
  }

  private static void mockState1(Map<UUID, JobDefinition> jobs, Map<UUID, InstanceInfo> instances) {
    InstanceMetadata[] md = new InstanceMetadata[] {
        mock(InstanceMetadata.class),
        mock(InstanceMetadata.class),
        mock(InstanceMetadata.class)
    };
    UUID[] instIds = new UUID[] {
        UUID.randomUUID(),
        UUID.randomUUID(),
        UUID.randomUUID()
    };

    UUID[] appId = new UUID[3];
    appId[0] = UUID.randomUUID();
    appId[1] = appId[0];
    appId[2] = UUID.randomUUID();

    jobs.put(appId[0], mock(JobDefinition.class));
    jobs.put(appId[2], mock(JobDefinition.class));

    InstanceInfo[] instanceArray = new InstanceInfo[] {
        mock(InstanceInfo.class),
        mock(InstanceInfo.class),
        mock(InstanceInfo.class)
    };
    for (int i = 0; i < instanceArray.length; ++i) {
      doReturn(md[i]).when(instanceArray[i]).metadata();
      InstanceStatus status = mock(InstanceStatus.class);
      doReturn(InstanceStatus.StateEnum.RUNNING).when(status).getState();
      doReturn(status).when(instanceArray[i]).status();
      doReturn(instIds[i]).when(md[i]).uuid();
      doReturn(appId[i]).when(md[i]).jobDefinition();
      instances.put(instIds[i], instanceArray[i]);
    }
  }
}
