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
import com.uber.athenax.backend.api.JobDefinitionDesiredstate;
import com.uber.athenax.backend.server.yarn.InstanceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HealthCheckReport {
  private final List<InstanceInfo> spuriousInstances = new ArrayList<InstanceInfo>();
  private final List<InstanceInfo> instancesWithDifferentParameters = new ArrayList<>();
  private final Map<ExtendedJobDefinition, List<JobDefinitionDesiredstate>> instancesToStart = new HashMap<>();

  public List<InstanceInfo> spuriousInstances() {
    return spuriousInstances;
  }

  public List<InstanceInfo> instancesWithDifferentParameters() {
    return instancesWithDifferentParameters;
  }

  public Map<ExtendedJobDefinition, List<JobDefinitionDesiredstate>> instancesToStart() {
    return instancesToStart;
  }

}
