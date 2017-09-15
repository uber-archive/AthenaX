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

import com.uber.athenax.backend.api.InstanceState;
import com.uber.athenax.backend.api.InstanceStatus;
import com.uber.athenax.backend.api.InstancesApiService;
import com.uber.athenax.backend.api.NotFoundException;
import com.uber.athenax.backend.server.ServerContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.http.HttpStatus;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.util.UUID;

@javax.annotation.Generated(
    value = "io.swagger.codegen.languages.JavaJerseyServerCodegen",
    date = "2017-09-19T15:16:54.206-07:00")
public class InstancesApiServiceImpl extends InstancesApiService {
  private final ServerContext ctx;

  public InstancesApiServiceImpl(ServerContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public Response changeInstanceState(
      UUID instanceUUID,
      InstanceState state,
      SecurityContext securityContext) throws NotFoundException {
    try {
      ctx.instanceManager().changeState(instanceUUID, state);
    } catch (YarnException | IOException e) {
      return Response.serverError().entity("Failed to kill the applications " + e).build();
    }
    return Response.ok().build();
  }

  @Override
  public Response getInstanceInfo(UUID instanceUUID, SecurityContext securityContext) throws NotFoundException {
    InstanceStatus stat = ctx.instanceManager().getInstanceStatus(instanceUUID);
    if (stat != null) {
      return Response.ok().entity(stat).build();
    } else {
      throw new NotFoundException(HttpStatus.SC_NOT_FOUND, "Instance not found");
    }
  }

  @Override
  public Response getInstanceState(UUID instanceUUID, SecurityContext securityContext) throws NotFoundException {
    InstanceState state = ctx.instanceManager().getInstanceState(instanceUUID);
    if (state != null) {
      return Response.ok().entity(state).build();
    } else {
      throw new NotFoundException(HttpStatus.SC_NOT_FOUND, "Instance not found");
    }
  }
}
