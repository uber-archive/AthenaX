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

package com.uber.athenax.vm.connectors.kafka;

import kafka.admin.AdminUtils;
import kafka.common.TopicExistsException;
import kafka.utils.ZkUtils;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.common.security.JaasUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.Properties;

public final class KafkaTestUtil {
  private static final int ZK_SESSION_TIMEOUT_MS = 10000;
  private static final int ZK_CONNECTION_TIMEOUT_MS = 10000;

  private KafkaTestUtil() {
  }

  static int getAvailablePort() {
    try {
      try (ServerSocket socket = new ServerSocket(0)) {
        return socket.getLocalPort();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to find available port to use", e);
    }
  }

  public static boolean createKafkaTopicIfNecessary(String brokerUri, int replFactor, int numPartitions, String topic)
      throws IOException {
    URI zkUri = URI.create(brokerUri);
    Preconditions.checkArgument("zk".equals(zkUri.getScheme()));
    String zkServerList = zkUri.getAuthority() + zkUri.getPath();

    ZkUtils zkUtils = ZkUtils.apply(zkServerList, ZK_SESSION_TIMEOUT_MS,
        ZK_CONNECTION_TIMEOUT_MS, JaasUtils.isZkSecurityEnabled());
    try {
      if (AdminUtils.topicExists(zkUtils, topic)) {
        return false;
      }

      try {
        AdminUtils.createTopic(zkUtils, topic, numPartitions, replFactor, new Properties());
      } catch (TopicExistsException ignored) {
        return false;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    } finally {
      if (zkUtils != null) {
        zkUtils.close();
      }
    }
    return true;
  }
}
