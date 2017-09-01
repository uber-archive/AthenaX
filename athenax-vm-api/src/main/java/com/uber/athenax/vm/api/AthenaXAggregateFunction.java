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

package com.uber.athenax.vm.api;

import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;

/**
 * Base class for User-Defined Aggregates.
 *
 * <p>The behavior of an [[AthenaXAggregateFunction]] can be defined by implementing a series of custom
 * methods. An [[AthenaXAggregateFunction]] needs at least three methods:
 *  - createAccumulator,
 *  - accumulate, and
 *  - getValue. </p>
 *
 * <p>All these methods muse be declared publicly, not static and named exactly as the names
 * mentioned above. The methods createAccumulator and getValue are defined in the
 * [[AthenaXAggregateFunction]] functions. </p>
 *
 * <p>Example: </p>
 *
 * <pre>
 *   public class SimpleAverageAccum extends Tuple2&lt;Long,Integer&gt; {
 *     public long sum = 0;
 *     public int count = 0;
 *   }
 *
 *   public class SimpleAverage extends AthenaXAggregateFunction&lt;Long, SimpleAverageAccum&gt; {
 *    public SimpleAverageAccum createAccumulator() {
 *      return new SimpleAverageAccum();
 *    }
 *
 *    public Long getValue(SimpleAverageAccum accumulator) {
 *      if (accumulator.count == 0) {
 *        return null;
 *      } else {
 *        return accumulator.sum / accumulator.count;
 *      }
 *    }
 *
 *    // overloaded accumulate method
 *    public void accumulate(SimpleAverageAccum accumulator, long iValue) {
 *      accumulator.sum += iValue;
 *      accumulator.count += 1;
 *    }
 *
 *    //Overloaded accumulate method
 *    public void accumulate(SimpleAverageAccum accumulator, int iValue) {
 *      accumulator.sum += iValue;
 *      accumulator.count += 1;
 *    }
 *  }
 *
 * </pre>
 *
 * @tparam T   the type of the aggregation result
 * @tparam ACC base class for aggregate Accumulator. The accumulator is used to keep the aggregated
 *             values which are needed to compute an aggregation result. AggregateFunction
 *             represents its state using accumulator, thereby the state of the AggregateFunction
 *             must be put into the accumulator.
 */
public abstract class AthenaXAggregateFunction<T, ACC> extends AggregateFunction<T, ACC> {
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
