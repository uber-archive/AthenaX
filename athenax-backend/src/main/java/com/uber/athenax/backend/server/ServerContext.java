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

import com.uber.athenax.backend.server.jobs.JobManager;
import com.uber.athenax.backend.server.jobs.JobStore;
import com.uber.athenax.backend.server.jobs.WatchdogPolicy;
import com.uber.athenax.backend.server.yarn.InstanceManager;
import com.uber.athenax.vm.api.AthenaXTableCatalogProvider;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.InstantiationUtil.instantiate;

public final class ServerContext {
  public static final ServerContext INSTANCE = new ServerContext();

  private final long startTime;
  private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
  private InstanceManager instanceManager;
  private JobStore jobStore;
  private JobManager jobManager;
  private WatchdogPolicy watchdogPolicy;
  private AthenaXTableCatalogProvider catalogs;
  private AthenaXConfiguration conf;

  private ServerContext() {
    this.startTime = System.currentTimeMillis();
  }

  public void initialize(AthenaXConfiguration conf) throws ClassNotFoundException, IOException {
    this.conf = conf;
    this.jobStore = (JobStore) instantiate(Class.forName(conf.jobStoreImpl()));
    this.catalogs = (AthenaXTableCatalogProvider) instantiate(Class.forName(conf.catalogProvider()));
    this.jobManager = new JobManager(jobStore, catalogs);
    this.instanceManager = InstanceManager.create(conf, jobManager, executor);
    this.watchdogPolicy = (WatchdogPolicy) instantiate(Class.forName(conf.watchdogPolicyImpl()));
  }

  public WatchdogPolicy watchdogPolicy() {
    return watchdogPolicy;
  }

  public void start() throws IOException {
    jobStore.open(conf);
    instanceManager.start();
  }

  public long startTime() {
    return startTime;
  }

  public InstanceManager instanceManager() {
    return instanceManager;
  }

  public JobManager jobManager() {
    return jobManager;
  }

  public AthenaXTableCatalogProvider catalogs() {
    return catalogs;
  }

  public ScheduledExecutorService executor() {
    return executor;
  }

}
