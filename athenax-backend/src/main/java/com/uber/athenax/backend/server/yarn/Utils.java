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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;

import java.util.Optional;
import java.util.Set;

import static com.uber.athenax.backend.server.yarn.InstanceMetadata.YARN_TAG_PREFIX;

final class Utils {
  private Utils() {
  }

  /**
   * Extract the job definition UUID from the application tag of the YARN application.
   */
  @VisibleForTesting
  static InstanceMetadata getMetadata(Set<String> tags) {
    Optional<String> s = tags.stream().filter(x -> x.startsWith(YARN_TAG_PREFIX)).findFirst();
    if (!s.isPresent()) {
      return null;
    }
    return InstanceMetadata.deserialize(s.get());
  }

  static InstanceInfo extractInstanceInfo(String clusterName, ApplicationReport report) {
    InstanceMetadata md = getMetadata(report.getApplicationTags());
    if (md == null) {
      return null;
    }

    ApplicationResourceUsageReport usage = report.getApplicationResourceUsageReport();
    InstanceStatus stat = new InstanceStatus()
        .allocatedVCores((long) usage.getUsedResources().getVirtualCores())
        .allocatedMB((long) usage.getUsedResources().getMemory())
        .clusterId(clusterName)
        .applicationId(report.getApplicationId().toString())
        .startedTime(report.getStartTime())
        .runningContainers((long) usage.getNumUsedContainers())
        .trackingUrl(report.getTrackingUrl())
        .state(InstanceStatus.StateEnum.fromValue(report.getYarnApplicationState().toString()));
    return new InstanceInfo(clusterName, report.getApplicationId(), md, stat);
  }
}
