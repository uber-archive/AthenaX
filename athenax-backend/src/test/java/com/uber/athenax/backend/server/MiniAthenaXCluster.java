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

import com.uber.athenax.backend.server.yarn.YarnClusterConfiguration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FAILED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.FINISHED;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.KILLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.RM_ADDRESS;

public class MiniAthenaXCluster implements Closeable {
  private final File workDir;

  private final YarnConfiguration yarnConf = new YarnConfiguration();
  private final MiniYARNCluster yarnCluster;

  private YarnClusterConfiguration yarnClusterConf;

  public MiniAthenaXCluster(String name) {
    this.yarnCluster = new MiniYARNCluster(name, 1, 1, 1, 1);
    this.workDir = new File("target", name);
  }

  @Override
  public void close() throws IOException {
    yarnCluster.close();
    FileContext.getLocalFSFileContext().delete(new Path(workDir.getAbsolutePath()), true);
  }

  public void start() throws IOException, URISyntaxException {
    workDir.mkdirs();
    createDummyUberJar(new File(workDir, "flink.jar"));
    createEmptyFile(new File(workDir, "flink-conf.yaml"));
    this.yarnClusterConf = prepareYarnCluster();
  }

  private static void createDummyUberJar(File dest) throws IOException {
    try (JarOutputStream os = new JarOutputStream(new FileOutputStream(dest))) {
      os.setComment("Dummy Flink Uber jar");
    }
  }

  private static void createEmptyFile(File dest) throws IOException {
    new FileWriter(dest).close();
  }

  public static YarnApplicationState pollFinishedApplicationState(YarnClient client, ApplicationId appId)
      throws IOException, YarnException, InterruptedException {
    EnumSet<YarnApplicationState> finishedState = EnumSet.of(FINISHED, KILLED, FAILED);

    while (true) {
      ApplicationReport report = client.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      if (finishedState.contains(state)) {
        return state;
      } else {
        Thread.sleep(250);
      }
    }
  }

  public YarnConfiguration getYarnConfiguration() {
    return yarnConf;
  }

  public YarnClusterConfiguration getYarnClusterConf() {
    return yarnClusterConf;
  }

  private YarnClusterConfiguration prepareYarnCluster() throws IOException, URISyntaxException {
    yarnCluster.init(yarnConf);
    yarnCluster.start();
    yarnConf.set(RM_ADDRESS, yarnCluster.getResourceManager().getConfig().get(RM_ADDRESS));

    File yarnSite = new File(workDir, "yarn-site.xml");
    try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) {
      yarnConf.writeXml(pw);
    }

    Path flinkUberJar = new Path(new File(workDir, "flink.jar").toURI());
    Path flinkConfYaml = new Path(new File(workDir, "flink-conf.yaml").toURI());
    @SuppressWarnings("ConstantConditions")
    Path log4jPath = new Path(Thread.currentThread().getContextClassLoader().getResource("log4j.properties").toURI());

    Set<Path> resourcesToLocalize = new HashSet<>(Arrays.asList(flinkUberJar, flinkConfYaml, log4jPath));

    String home = workDir.toURI().toString();
    return new YarnClusterConfiguration(
        yarnConf,
        home,
        flinkUberJar,
        resourcesToLocalize,
        systemJars(yarnSite));
  }

  public File workDir() {
    return workDir;
  }

  public String generateYarnClusterConfContent(String clusterName) {
    StringBuffer sb = new StringBuffer();
    String parent = workDir.getAbsolutePath();
    sb.append("clusters:\n")
        .append(String.format("  %s:\n", clusterName))
        .append(String.format("    yarn.site.location: %s\n", new File(parent, "yarn-site.xml").toURI()))
        .append(String.format("    athenax.home.dir: %s\n", workDir.toURI()))
        .append(String.format("    flink.uber.jar.location: %s\n", new File(parent, "flink.jar").toURI()))
        .append("    localize.resources:\n");

    yarnClusterConf.resourcesToLocalize().forEach(x -> sb.append(String.format("      - %s\n", x.toUri())));

    sb.append("    additional.jars:\n");
    yarnClusterConf.systemJars().forEach(x -> sb.append(String.format("      - %s\n", x.toUri())));
    return sb.toString();
  }

  private static Set<Path> systemJars(File yarnSite) throws URISyntaxException {
    String[] jars = System.getProperty("java.class.path").split(Pattern.quote(File.pathSeparator));
    Set<Path> res = Arrays.stream(jars).map(File::new).filter(File::isFile)
        .map(x -> new Path(x.toURI())).collect(Collectors.toSet());
    res.add(new Path(yarnSite.toURI()));
    return res;
  }
}
