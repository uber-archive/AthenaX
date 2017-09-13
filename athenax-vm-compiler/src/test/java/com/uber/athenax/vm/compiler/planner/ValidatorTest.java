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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.uber.athenax.vm.compiler.parser.impl.ParseException;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.configuration.Configuration;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ValidatorTest {
  @Test(expected = IllegalArgumentException.class)
  public void testMultiSqlInsert() throws IOException, ParseException {
    String sql = Joiner.on(";\n").join(
        "INSERT INTO foo (SELECT * FROM bar)",
        "INSERT INTO foo (SELECT * FROM bar)"
    );
    SqlNodeList nodes = Planner.parse(sql);
    Validator validator = new Validator();
    validator.validateExactlyOnceSelect(nodes);
  }

  @Test
  public void testWrapSelectIntoInsert() throws IOException, ParseException {
    String sql = Joiner.on(";\n").join(
        "SET foo = 1",
        "SELECT * FROM bar"
    );
    SqlNodeList nodes = Planner.parse(sql);
    Validator validator = new Validator();
    validator.validateExactlyOnceSelect(nodes);
  }

  @Test
  public void testSetOptions() throws IOException, ParseException {
    String sql = Joiner.on(";\n").join(
        "SET spam = 1",
        "SET foo = 'bar'",
        "RESET foo",
        "ALTER SESSION SET bar = OFF"
    );
    SqlNodeList nodes = Planner.parse(sql);
    Validator validator = new Validator();
    validator.extract(nodes);
    Configuration options = validator.options();
    assertEquals(1, options.getInt("spam"));
    assertNull(options.getString("foo", null));
    assertEquals("OFF", options.getString("bar"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSetSystemProperties() throws IOException, ParseException {
    String sql = "ALTER SYSTEM SET bar = OFF";
    SqlNodeList nodes = Planner.parse(sql);
    Validator validator = new Validator();
    validator.extract(nodes);
  }

  @Test
  public void testCreateFunction() throws IOException, ParseException {
    String sql = Joiner.on(";\n").join(
        "CREATE FUNCTION udf AS 'foo.udf'",
        "CREATE FUNCTION udf1 AS 'foo.udf' USING JAR 'mock://foo'",
        "CREATE FUNCTION udf2 AS 'foo.udf' USING JAR 'mock://foo', JAR 'mock://bar'"
    );
    SqlNodeList nodes = Planner.parse(sql);
    Validator validator = new Validator();
    validator.extract(nodes);
    assertEquals(ImmutableList.of(
        URI.create("mock://foo"),
        URI.create("mock://foo"),
        URI.create("mock://bar")
    ), ImmutableList.copyOf(validator.additionalResources()));
  }
}
