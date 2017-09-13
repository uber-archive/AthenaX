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

package com.uber.athenax.vm.compiler.executor;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ContainedExecutor {

  public CompilationResult run(JobDescriptor job) throws IOException {
    // HACK: Start a socket to get the results back
    // Ideally it is simpler to use stdout / stderr / pipe,
    // but many loggers will interfere
    try (ServerSocket sock = new ServerSocket()) {
      sock.bind(new InetSocketAddress(InetAddress.getLocalHost(), 0));
      ProcessBuilder builder = new ProcessBuilder(buildMainArg(sock.getLocalPort()));
      Process proc = builder.start();
      try (OutputStream os = new BufferedOutputStream(proc.getOutputStream())) {
        os.write(job.serialize());
      }

      try (Socket client = sock.accept()) {
        try (ObjectInputStream is = new ObjectInputStream(client.getInputStream())) {
          return (CompilationResult) is.readObject();
        } catch (ClassNotFoundException e) {
          throw new IOException(e);
        }
      }
    }
  }

  private static List<String> buildMainArg(int port) {
    File jvm = new File(new File(System.getProperty("java.home"), "bin"), "java");
    ArrayList<String> ops = new ArrayList<>();
    ops.add(jvm.toString());
    ops.add("-classpath");
    ops.add(System.getProperty("java.class.path"));
    String javaLibPath = System.getProperty("java.library.path");
    if (javaLibPath != null) {
      ops.add("-Djava.library.path=" + javaLibPath);
    }
    ops.add(JobCompiler.class.getCanonicalName());
    ops.add(Integer.toString(port));
    return ops;
  }
}
