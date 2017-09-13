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

import com.uber.athenax.vm.api.AthenaXTableCatalog;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

public class JobDescriptor implements Serializable {
  private static final long serialVersionUID = -1;
  private final Map<String, String> userDefineFunctions;
  private final Map<String, AthenaXTableCatalog> inputs;
  private final AthenaXTableCatalog outputs;
  private final int parallelism;

  /**
   * Stripped down statement that can be recognized by Flink.
   */
  private final String sqlStatement;

  public JobDescriptor(Map<String, AthenaXTableCatalog> inputs,
                       Map<String, String> userDefineFunctions,
                       AthenaXTableCatalog outputs,
                       int parallelism, String sqlStatement) {
    this.userDefineFunctions = userDefineFunctions;
    this.inputs = inputs;
    this.outputs = outputs;
    this.parallelism = parallelism;
    this.sqlStatement = sqlStatement;
  }

  Map<String, String> udf() {
    return userDefineFunctions;
  }

  Map<String, AthenaXTableCatalog> inputs() {
    return inputs;
  }

  AthenaXTableCatalog outputs() {
    return outputs;
  }

  String sql() {
    return sqlStatement;
  }

  int parallelism() {
    return parallelism;
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
