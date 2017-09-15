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

package com.uber.athenax.backend.server.yarn;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * Metadata that is recorded in the application tag of YARN.
 */
public class InstanceMetadata {
  static final String YARN_TAG_PREFIX = "athenax_md_";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @JsonProperty("i")
  private final UUID uuid;
  @JsonProperty("d")
  private final UUID definition;

  InstanceMetadata() {
    this.uuid = null;
    this.definition = null;
  }

  InstanceMetadata(UUID uuid, UUID definition) {
    this.uuid = uuid;
    this.definition = definition;
  }

  String serialize() {
    try {
      return YARN_TAG_PREFIX + MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof InstanceMetadata) {
      InstanceMetadata o = (InstanceMetadata) obj;
      return uuid.equals(o.uuid) && definition.equals(o.definition);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(uuid, definition);
  }

  public UUID uuid() {
    return uuid;
  }

  public UUID jobDefinition() {
    return definition;
  }

  static InstanceMetadata deserialize(String s) {
    Preconditions.checkArgument(s.startsWith(YARN_TAG_PREFIX));
    String json = s.substring(YARN_TAG_PREFIX.length());
    try {
      return MAPPER.readValue(json, InstanceMetadata.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
