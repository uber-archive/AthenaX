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
import com.uber.athenax.backend.server.AthenaXExtraConfigOptions;
import com.uber.athenax.backend.server.InstanceStateUpdateListener;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;

import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InstanceManagerTest {

  @Test
  public void testChangeState() throws Exception {
    YarnClient client = mock(YarnClient.class);
    YarnClusterConfiguration conf = mock(YarnClusterConfiguration.class);
    ClusterInfo clusterInfo = new ClusterInfo("foo", conf, client);
    UUID app = UUID.randomUUID();
    ApplicationId yarnAppId = mock(ApplicationId.class);

    try (InstanceManager manager = new InstanceManager(
        Collections.singletonMap("foo", clusterInfo),
        mock(InstanceStateUpdateListener.class),
        mock(ScheduledExecutorService.class),
        AthenaXExtraConfigOptions.INSTANCE_MANAGER_RESCAN_INTERVAL.defaultValue())) {
      InstanceInfo instance = new InstanceInfo("foo", yarnAppId,
          mock(InstanceMetadata.class), mock(InstanceStatus.class));
      manager.instances().put(app, instance);
      manager.changeState(app, new InstanceState().state(InstanceState.StateEnum.KILLED));
      verify(client).killApplication(eq(yarnAppId));
    }
  }
}
