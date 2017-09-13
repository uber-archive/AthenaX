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

import org.apache.flink.runtime.jobgraph.JobGraph;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class CompilationResult implements Serializable {
  private JobGraph jobGraph;
  private Throwable remoteThrowable;

  public JobGraph jobGraph() {
    return jobGraph;
  }

  void jobGraph(JobGraph jobGraph) {
    this.jobGraph = jobGraph;
  }

  void remoteThrowable(Throwable e) {
    this.remoteThrowable = e;
  }

  public Throwable remoteThrowable() {
    return remoteThrowable;
  }

  byte[] serialize() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (ObjectOutputStream os = new ObjectOutputStream(bos)) {
      os.writeObject(this);
    } catch (IOException e) {
      return null;
    }
    return bos.toByteArray();
  }

}
