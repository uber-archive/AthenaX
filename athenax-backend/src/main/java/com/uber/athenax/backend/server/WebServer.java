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

package com.uber.athenax.backend.server;

import com.sun.jersey.api.container.grizzly2.GrizzlyServerFactory;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.uber.athenax.backend.api.ClusterApi;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.http.util.HttpStatus;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;

import java.io.IOException;
import java.net.URI;
import java.util.StringJoiner;

public class WebServer implements AutoCloseable {
  public static final String BASE_PATH = "/ws/v1";

  private static final String[] PACKAGES = new String[] {
      ClusterApi.class.getPackage().getName(),
      "com.fasterxml.jackson.jaxrs.json"
  };

  private final HttpServer server;

  public WebServer(URI endpoint) throws IOException {
    this.server = GrizzlyServerFactory.createHttpServer(endpoint, new HttpHandler() {

      @Override
      public void service(Request rqst, Response rspns) throws Exception {
        rspns.setStatus(HttpStatus.NOT_FOUND_404.getStatusCode(), "Not found");
        rspns.getWriter().write("404: not found");
      }
    });

    WebappContext context = new WebappContext("WebappContext", BASE_PATH);
    ServletRegistration registration = context.addServlet("ServletContainer", ServletContainer.class);
    registration.setInitParameter(ServletContainer.RESOURCE_CONFIG_CLASS,
        PackagesResourceConfig.class.getName());

    StringJoiner sj = new StringJoiner(",");
    for (String s : PACKAGES) {
      sj.add(s);
    }

    registration.setInitParameter(PackagesResourceConfig.PROPERTY_PACKAGES, sj.toString());
    registration.addMapping(BASE_PATH);
    context.deploy(server);
  }

  public void start() throws IOException {
    server.start();
  }

  @Override
  public void close() throws Exception {
    server.stop();
  }

  public int port() {
    return server.getListeners().iterator().next().getPort();
  }
}
