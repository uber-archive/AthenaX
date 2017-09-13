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

import com.uber.athenax.vm.api.DataSinkProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

final class DataSinkRegistry {
  private static final Map<String, DataSinkProvider> PROVIDERS;

  static {
    HashMap<String, DataSinkProvider> providers = new HashMap<>();
    ServiceLoader<DataSinkProvider> loaders =
        ServiceLoader.load(DataSinkProvider.class);
    loaders.forEach(x -> providers.put(x.getType(), x));
    PROVIDERS = Collections.unmodifiableMap(providers);
  }

  private DataSinkRegistry() {
  }

  static DataSinkProvider getProvider(String type) {
    return PROVIDERS.get(type);
  }
}
