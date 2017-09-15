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

package com.uber.athenax.backend.api.impl;

import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.api.JobsApiService;
import com.uber.athenax.backend.api.NotFoundException;
import com.uber.athenax.backend.server.ServerContext;
import org.apache.http.HttpStatus;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen",
    date = "2017-09-22T14:43:25.370-07:00")
public class JobsApiServiceImpl extends JobsApiService {
  public static final int INVALID_REQUEST = HttpStatus.SC_INTERNAL_SERVER_ERROR;
  private final ServerContext ctx;

  public JobsApiServiceImpl(ServerContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response allocateNewJob(SecurityContext securityContext) throws NotFoundException {
    return Response.ok().entity(
        Collections.singletonMap("job-uuid", ctx.jobManager().newJobUUID())
    ).build();
  }

  @Override
  public Response getJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
      JobDefinition job = ctx.jobManager().getJob(jobUUID);
      if (job == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(job).build();
      }
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
  }

  @Override
  public Response listJob(SecurityContext securityContext) throws NotFoundException {
    try {
      return Response.ok(ctx.jobManager().listJobs()).build();
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
  }

  @Override
  public Response removeJob(UUID jobUUID, SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.jobManager().removeJob(jobUUID);
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
    return Response.ok().build();
  }

  @Override
  public Response updateJob(
      UUID jobUUID,
      JobDefinition body,
      SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.jobManager().updateJob(jobUUID, body);
    } catch (IOException e) {
      throw new NotFoundException(INVALID_REQUEST, e.getMessage());
    }
    return Response.ok().build();
  }
}
