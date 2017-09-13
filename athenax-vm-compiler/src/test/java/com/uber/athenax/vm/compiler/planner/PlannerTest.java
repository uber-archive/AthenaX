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

import org.apache.calcite.sql.SqlNodeList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PlannerTest {

  @Test
  public void testCreateFunction() throws Exception {
    String sql = "CREATE FUNCTION foo AS 'com.uber.foo';";
    Planner.parse(sql);

    sql = "CREATE FUNCTION foo AS 'com.uber.foo' USING JAR 'mock://foo', JAR 'mock://bar';";
    Planner.parse(sql);
  }

  @Test
  public void testSetOption() throws Exception {
    String sql = "SET flink.enable.checkpoint=1;";
    Planner.parse(sql);
  }

  @Test
  public void testMultipleStatement() throws Exception {
    String sql = "SET flink.enable.checkpoint=1;";
    SqlNodeList list = Planner.parse(sql);
    assertEquals(1, list.size());

    sql = "SET flink.enable.checkpoint=1;\n"
        + "SELECT * FROM foo";
    list = Planner.parse(sql);
    assertEquals(2, list.size());

    sql = "SET flink.enable.checkpoint=1;\n"
        + "SELECT * FROM foo;";
    list = Planner.parse(sql);
    assertEquals(2, list.size());
  }
}
