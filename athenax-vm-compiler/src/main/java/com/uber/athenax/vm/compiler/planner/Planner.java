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

import com.uber.athenax.vm.api.AthenaXTableCatalog;
import com.uber.athenax.vm.compiler.executor.CompilationResult;
import com.uber.athenax.vm.compiler.executor.ContainedExecutor;
import com.uber.athenax.vm.compiler.executor.JobDescriptor;
import com.uber.athenax.vm.compiler.parser.impl.ParseException;
import com.uber.athenax.vm.compiler.parser.impl.SqlParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.hadoop.fs.Path;

import java.io.StringReader;
import java.util.Map;
import java.util.stream.Collectors;

public class Planner {
  private static final int DEFAULT_IDENTIFIER_MAX_LENGTH = 128;

  private final Map<String, AthenaXTableCatalog> inputs;
  private final AthenaXTableCatalog outputs;

  public Planner(Map<String, AthenaXTableCatalog> inputs, AthenaXTableCatalog outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public JobCompilationResult sql(String sql, int parallelism) throws Throwable {
    SqlNodeList stmts = parse(sql);
    Validator validator = new Validator();
    validator.validateQuery(stmts);
    JobDescriptor job = new JobDescriptor(
        inputs,
        validator.userDefinedFunctions(),
        outputs,
        parallelism,
        validator.statement().toString());
    CompilationResult res = new ContainedExecutor().run(job);

    if (res.remoteThrowable() != null) {
      throw res.remoteThrowable();
    }
    return new JobCompilationResult(res.jobGraph(),
        validator.userDefinedFunctions().values().stream().map(Path::new).collect(Collectors.toList()));
  }

  @VisibleForTesting
  static SqlNodeList parse(String sql) throws ParseException {
    // Keep the SQL syntax consistent with Flink
    try (StringReader in = new StringReader(sql)) {
      SqlParserImpl impl = new SqlParserImpl(in);

      // back tick as the quote
      impl.switchTo("BTID");
      impl.setTabSize(1);
      impl.setQuotedCasing(Lex.JAVA.quotedCasing);
      impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
      impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);
      return impl.SqlStmtsEof();
    }
  }
}
