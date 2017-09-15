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

package com.uber.athenax.backend;

import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.ServerContext;
import com.uber.athenax.backend.server.WebServer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

public class AthenaXServer {
  private static final Options CLI_OPTIONS = new Options()
      .addOption(null, "conf", true, "The configuration file");

  private void start(AthenaXConfiguration conf) throws Exception {
    ServerContext.INSTANCE.initialize(conf);
    ServerContext.INSTANCE.start();
    try (WebServer server = new WebServer(URI.create(conf.masterUri()))) {
      server.start();
      Thread.currentThread().join();
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(CLI_OPTIONS, args);
    if (!line.hasOption("conf")) {
      System.err.println("No configuration file is specified");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("athenax-server", CLI_OPTIONS);
      System.exit(1);
    }

    try {
      String confFile = line.getOptionValue("conf");
      AthenaXConfiguration conf = AthenaXConfiguration.load(Paths.get(confFile).toFile());
      new AthenaXServer().start(conf);
    } catch (IOException | ClassNotFoundException e) {
      System.err.println("Failed to parse configuration.");
      throw e;
    }
  }
}
