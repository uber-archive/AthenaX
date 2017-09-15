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

package com.uber.athenax.backend.server;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public final class AthenaXExtraConfigOptions {

  /**
   * The directory that the LevelDB JobStore uses to persist information.
   */
  public static final ConfigOption<String> JOBSTORE_LEVELDB_FILE =
      key("jobstore.leveldb.file").noDefaultValue();

  /**
   * The interval that the instance manager will rescan all running AthenaX application.
   */
  public static final ConfigOption<Long> INSTANCE_MANAGER_RESCAN_INTERVAL =
      key("instancemanager.rescan.interval").defaultValue(120 * 1000L);

  private AthenaXExtraConfigOptions() {
  }
}
