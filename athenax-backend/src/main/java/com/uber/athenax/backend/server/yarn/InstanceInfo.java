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

import com.uber.athenax.backend.api.InstanceStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class InstanceInfo {
  private final String clusterName;
  private final ApplicationId appId;
  private final InstanceMetadata metadata;
  private final InstanceStatus status;

  public InstanceInfo(String clusterName, ApplicationId appId,
               InstanceMetadata metadata, InstanceStatus status) {
    this.metadata = metadata;
    this.clusterName = clusterName;
    this.appId = appId;
    this.status = status;
  }

  public String clusterName() {
    return clusterName;
  }

  public ApplicationId appId() {
    return appId;
  }

  public InstanceMetadata metadata() {
    return metadata;
  }

  public InstanceStatus status() {
    return status;
  }
}
