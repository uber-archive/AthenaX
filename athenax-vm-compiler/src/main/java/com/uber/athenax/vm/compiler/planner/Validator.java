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

package com.uber.athenax.vm.compiler.planner;

import com.uber.athenax.vm.compiler.parser.SqlCreateFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * The validator types check the queries and extracts configurations.
 */
class Validator {
  private final PropertiesConfiguration conf = new PropertiesConfiguration();
  private final ArrayList<URI> additionalResources = new ArrayList<>();
  private final HashMap<String, String> userDefinedFunctions = new HashMap<>();

  private SqlSelect statement;

  Configuration options() {
    return conf;
  }

  ArrayList<URI> additionalResources() {
    return additionalResources;
  }

  Map<String, String> userDefinedFunctions() {
    return userDefinedFunctions;
  }

  void validateQuery(SqlNodeList query) {
    extract(query);
    validateExactlyOnceSelect(query);
  }

  /**
   * Extract options and jars from the queries.
   */
  @VisibleForTesting
  void extract(SqlNodeList query) {
    for (SqlNode n : query) {
      if (n instanceof SqlSetOption) {
        extract((SqlSetOption) n);
      } else if (n instanceof SqlCreateFunction) {
        extract((SqlCreateFunction) n);
      }
    }
  }

  private void extract(SqlCreateFunction node) {
    if (node.jarList() == null) {
      return;
    }

    for (SqlNode n : node.jarList()) {
      URI uri = URI.create(unwrapConstant(n));
      additionalResources.add(uri);
    }

    String funcName = node.dbName() != null ? unwrapConstant(node.dbName()) + "." + unwrapConstant(node.funcName())
        : unwrapConstant(node.funcName());
    String clazzName = unwrapConstant(node.className());
    userDefinedFunctions.put(funcName, clazzName);
  }

  private void extract(SqlSetOption node) {
    Object value = unwrapConstant(node.getValue());
    String property = node.getName().toString();

    Preconditions.checkArgument(!"SYSTEM".equals(node.getScope()),
        "cannot set properties at the system level");
    conf.setProperty(property, value);
  }

  /**
   * Validate there is only one SqlSelect construct in the lists of queries
   * and it is the last construct.
   */
  @VisibleForTesting
  void validateExactlyOnceSelect(SqlNodeList query) {
    Preconditions.checkArgument(query.size() > 0);
    SqlNode last = query.get(query.size() - 1);
    long n = StreamSupport.stream(query.spliterator(), false)
        .filter(x -> x instanceof SqlSelect)
        .count();
    Preconditions.checkArgument(n == 1 && last instanceof SqlSelect,
        "Only one top-level SELECT statement is allowed");
    statement = (SqlSelect) last;
  }

  SqlSelect statement() {
    return statement;
  }

  /**
   * Unwrap a constant in the AST as a Java Object.
   *
   * <p>The Calcite validator has folded all the constants by this point.
   * Thus the function expects either a SqlLiteral or a SqlIdentifier but not a SqlCall.</p>
   */
  private static String unwrapConstant(SqlNode value) {
    if (value == null) {
      return null;
    } else if (value instanceof SqlLiteral) {
      return ((SqlLiteral) value).toValue();
    } else if (value instanceof SqlIdentifier) {
      return value.toString();
    } else {
      throw new IllegalArgumentException("Invalid constant " + value);
    }
  }
}
