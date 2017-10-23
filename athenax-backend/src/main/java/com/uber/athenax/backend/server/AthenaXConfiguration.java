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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.uber.athenax.backend.server.jobs.LevelDBJobStore;
import com.uber.athenax.backend.server.jobs.WatchdogPolicyDefault;
import com.uber.athenax.backend.server.yarn.YarnClusterConfiguration;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AthenaXConfiguration {
  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class YarnCluster {
    /**
     * The configuration used by YARN (i.e., <pre>yarn-site.xml</pre>).
     */
    @JsonProperty("yarn.site.location")
    private final String yarnSite;

    /**
     * The home directory of all AthenaX job where all the temporary files for each jobs are stored.
     * In URI format
     */
    @JsonProperty("athenax.home.dir")
    private final String homeDir;

    /**
     * The location of the Flink Uber jar.
     */
    @JsonProperty("flink.uber.jar.location")
    private final String flinkUberJar;

    /**
     * Additional resources to be localized for both JobManager and TaskManager.
     * They will NOT be added into the classpaths.
     */
    @JsonProperty("localize.resources")
    private final Set<String> resourcesToLocalize;

    /**
     * JARs that will be localized and put into the classpaths for bot JobManager and TaskManager.
     */
    @JsonProperty("additional.jars")
    private final Set<String> additionalJars;

    public YarnCluster() {
      this.yarnSite = null;
      this.homeDir = null;
      this.flinkUberJar = null;
      this.resourcesToLocalize = Collections.emptySet();
      this.additionalJars = Collections.emptySet();
    }

    @VisibleForTesting
    Set<String> additionalJars() {
      return additionalJars;
    }

    public YarnClusterConfiguration toYarnClusterConfiguration() {
      Preconditions.checkNotNull(yarnSite, "yarn.site.location is not configured");
      Preconditions.checkNotNull(homeDir, "athenax.home.dir is not configured");
      Preconditions.checkNotNull(flinkUberJar, "flink.uber.jar.location is not configured");

      YarnConfiguration yarnConf = new YarnConfiguration();
      yarnConf.addResource(new Path(URI.create(yarnSite)));

      return new YarnClusterConfiguration(
          yarnConf, homeDir, new Path(flinkUberJar),
          resourcesToLocalize.stream().map(x -> new Path(URI.create(x))).collect(Collectors.toSet()),
          additionalJars.stream().map(x -> new Path(URI.create(x))).collect(Collectors.toSet()));
    }
  }

  /**
   * The name of the class that provides the catalog for all tables.
   */
  @JsonProperty("catalog.impl")
  private final String catalogProvider;

  /**
   * The name of the class that persists the data of jobs.
   */
  @JsonProperty("jobstore.impl")
  private final String jobStoreImpl;

  /**
   * The endpoint that the AthenaX master should listen to.
   */
  @JsonProperty("athenax.master.uri")
  private final String masterUri;

  @JsonProperty("watchdog.policy.impl")
  private final String watchdogPolicyImpl;

  @JsonProperty("clusters")
  private final Map<String, YarnCluster> clusters;

  // Extra configurations that can be used to customize the system.
  @JsonProperty("extras")
  private final Map<String, ?> extras;

  public AthenaXConfiguration() {
    this.catalogProvider = null;
    this.jobStoreImpl = LevelDBJobStore.class.getCanonicalName();
    this.watchdogPolicyImpl = WatchdogPolicyDefault.class.getCanonicalName();
    this.clusters = Collections.emptyMap();
    this.masterUri = null;
    this.extras = Collections.emptyMap();
  }

  public static AthenaXConfiguration load(File file) throws IOException {
    return MAPPER.readValue(file, AthenaXConfiguration.class);
  }

  public static AthenaXConfiguration loadContent(String content) throws IOException {
    return MAPPER.readValue(content, AthenaXConfiguration.class);
  }

  public String catalogProvider() {
    return catalogProvider;
  }

  public String jobStoreImpl() {
    return jobStoreImpl;
  }

  public String masterUri() {
    return masterUri;
  }

  public String watchdogPolicyImpl() {
    return watchdogPolicyImpl;
  }

  public Map<String, YarnCluster> clusters() {
    return clusters;
  }

  public Map<String, ?> extras() {
    return extras;
  }

  public long getExtraConfLong(ConfigOption<Long> config) {
    Object o = extras().get(config.key());
    if (config.hasDefaultValue()) {
      return o == null ? config.defaultValue() : Long.parseLong(o.toString());
    } else {
      throw new IllegalArgumentException("Unspecified config " + config.key());
    }
  }
}
