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

import com.uber.athenax.backend.api.JobsApiService;
import com.uber.athenax.backend.api.impl.JobsApiServiceImpl;
import com.uber.athenax.backend.server.ServerContext;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen",
    date = "2017-09-22T14:08:01.325-07:00")
public final class JobsApiServiceFactory {
  private static final JobsApiService SERVICE = new JobsApiServiceImpl(ServerContext.INSTANCE);

  private JobsApiServiceFactory() {
  }

  public static JobsApiService getJobsApi() {
    return SERVICE;
  }
}
