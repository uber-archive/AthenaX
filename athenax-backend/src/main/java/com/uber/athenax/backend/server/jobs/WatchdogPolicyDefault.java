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

import com.google.common.collect.Iterables;
import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.yarn.InstanceInfo;
import com.uber.athenax.backend.server.yarn.InstanceManager;
import com.uber.athenax.vm.compiler.planner.JobCompilationResult;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WatchdogPolicyDefault implements WatchdogPolicy {
  private static final Logger LOG = LoggerFactory.getLogger(WatchdogPolicyDefault.class);
  private final InstanceManager instanceManager = ServerContext.INSTANCE.instanceManager();
  private final JobManager jobManager = ServerContext.INSTANCE.jobManager();

  @Override
  public void onHealthCheckReport(HealthCheckReport report) {
    for (InstanceInfo info : Iterables.concat(report.spuriousInstances(), report.instancesWithDifferentParameters())) {
      try {
        instanceManager.killYarnApplication(info.clusterName(), info.appId());
      } catch (ApplicationNotFoundException ignored) {
      } catch (IOException | YarnException e) {
        LOG.warn("Failed to kill application {}:{}, cause: {}", info.clusterName(), info.appId(), e);
      }
    }

    for (Map.Entry<ExtendedJobDefinition, List<JobDefinitionDesiredstate>> e : report.instancesToStart().entrySet()) {
      for (JobDefinitionDesiredstate s : e.getValue()) {
        final JobCompilationResult res;
        JobDefinition job = e.getKey().getDefinition();
        try {
          res = jobManager.compile(job, s);
          instanceManager.instantiate(s, e.getKey().getUuid(), res);
        } catch (Throwable ex) {
          LOG.warn("Failed to instantiate the query '{}' on {}", job.getQuery(), s.getClusterId(), ex);
        }
      }
    }
  }
}
