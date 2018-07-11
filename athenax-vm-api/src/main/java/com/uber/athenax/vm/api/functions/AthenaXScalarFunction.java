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

package com.uber.athenax.vm.api.functions;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * All user-defined functions (UDFs) in AthenaX needs to inherit from from this class.
 *
 * <p>The syntax of the UDFs closely resembles the ones in
 * <a href="https://cwiki.apache.org/confluence/display/Hive/HivePlugins">Hive</a>.
 * </p>
 *
 * <p>The subclasses need to:</p>
 * <ul>
 *   <li>Have a public default constructor.</li>
 *   <li>Implement at least once public <pre>eval()</pre> method that takes
 * zero or more parameters.</li>
 * </ul>
 *
 */
public abstract class AthenaXScalarFunction extends ScalarFunction {

  /**
   * <p>Setup method for user-defined function. It can be used for initialization work.</p>
   *
   * <p>By default, this method does nothing.</p>
   */
  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
  }

  /**
   * <p>Tear-down method for user-defined function. It can be used for clean up work.</p>
   *
   * <p>By default, this method does nothing.</p>
   */
  @Override
  public void close() throws Exception {
    super.close();
  }
}
