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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ProcessExecutorTest {

  @Test
  public void testCompile() throws IOException {
    RowTypeInfo schema = new RowTypeInfo(new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO}, new String[] {"id"});
    MockExternalCatalogTable inputTable = new MockExternalCatalogTable(schema, Collections.singletonList(Row.of(1)));
    MockExternalCatalogTable outputTable = new MockExternalCatalogTable(schema, new ArrayList<>());
    SingleLevelMemoryCatalog input = new SingleLevelMemoryCatalog("input",
        Collections.singletonMap("foo", inputTable));
    SingleLevelMemoryCatalog output = new SingleLevelMemoryCatalog("output",
        Collections.singletonMap("bar", outputTable));
    JobDescriptor job = new JobDescriptor(
        Collections.singletonMap("input", input),
        Collections.emptyMap(),
        output,
        1,
        "SELECT * FROM input.foo");
    CompilationResult res = new ContainedExecutor().run(job);
    assertNull(res.remoteThrowable());
    assertNotNull(res.jobGraph());
  }

  @Test
  public void testInvalidSql() throws IOException {
    RowTypeInfo schema = new RowTypeInfo(new TypeInformation[]{BasicTypeInfo.INT_TYPE_INFO}, new String[] {"id"});
    MockExternalCatalogTable inputTable = new MockExternalCatalogTable(schema, Collections.singletonList(Row.of(1)));
    MockExternalCatalogTable outputTable = new MockExternalCatalogTable(schema, new ArrayList<>());
    SingleLevelMemoryCatalog input = new SingleLevelMemoryCatalog("input",
        Collections.singletonMap("foo", inputTable));
    SingleLevelMemoryCatalog output = new SingleLevelMemoryCatalog("output",
        Collections.singletonMap("bar", outputTable));
    JobDescriptor job = new JobDescriptor(
        Collections.singletonMap("input", input),
        Collections.emptyMap(),
        output,
        1,
        "SELECT2 * FROM input.foo");
    CompilationResult res = new ContainedExecutor().run(job);
    assertNull(res.jobGraph());
    assertTrue(res.remoteThrowable() instanceof SqlParserException);
  }
}
