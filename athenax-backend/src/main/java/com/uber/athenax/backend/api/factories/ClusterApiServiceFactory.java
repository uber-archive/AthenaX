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

package com.uber.athenax.backend.api.factories;

import com.uber.athenax.backend.api.ClusterApiService;
import com.uber.athenax.backend.api.impl.ClusterApiServiceImpl;
import com.uber.athenax.backend.server.ServerContext;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen",
    date = "2017-09-19T15:16:54.206-07:00")
public final class ClusterApiServiceFactory {
  private static final ClusterApiService SERVICE = new ClusterApiServiceImpl(ServerContext.INSTANCE);

  private ClusterApiServiceFactory() {
  }

  public static ClusterApiService getClusterApi() {
    return SERVICE;
  }
}
