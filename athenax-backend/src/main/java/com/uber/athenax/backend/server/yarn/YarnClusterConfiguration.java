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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Set;

/**
 * {@link YarnClusterConfiguration} consists of information of the YARN cluster.
 */
public class YarnClusterConfiguration {

  /**
   * The configuration used by YARN (i.e., <pre>yarn-site.xml</pre>).
   */
  private final YarnConfiguration conf;

  /**
   * The home directory of all AthenaX job where all the temporary files for each jobs are stored.
   */
  private final String homeDir;

  /**
   * The location of the Flink Uber jar.
   */
  private final Path flinkUberJar;

  /**
   * Additional resources to be localized for both JobManager and TaskManager.
   * They will NOT be added into the classpaths.
   */
  private final Set<Path> resourcesToLocalize;

  /**
   * JARs that will be localized and put into the classpaths for bot JobManager and TaskManager.
   */
  private final Set<Path> systemJars;

  public YarnClusterConfiguration(
      YarnConfiguration conf,
      String homeDir,
      Path flinkUberJar,
      Set<Path> resourcesToLocalize,
      Set<Path> systemJars) {
    this.conf = conf;
    this.homeDir = homeDir;
    this.flinkUberJar = flinkUberJar;
    this.resourcesToLocalize = resourcesToLocalize;
    this.systemJars = systemJars;
  }

  YarnConfiguration conf() {
    return conf;
  }

  public String homeDir() {
    return homeDir;
  }

  public Path flinkUberJar() {
    return flinkUberJar;
  }

  public Set<Path> resourcesToLocalize() {
    return resourcesToLocalize;
  }

  public Set<Path> systemJars() {
    return systemJars;
  }
}
