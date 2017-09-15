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

package com.uber.athenax.backend.server.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.athenax.backend.api.ExtendedJobDefinition;
import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.server.AthenaXConfiguration;
import com.uber.athenax.backend.server.AthenaXExtraConfigOptions;
import org.apache.flink.util.Preconditions;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class LevelDBJobStore implements JobStore {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private DB db;

  @Override
  public void open(AthenaXConfiguration conf) throws IOException {
    String dbLocation = (String) conf.extras().get(AthenaXExtraConfigOptions.JOBSTORE_LEVELDB_FILE.key());
    Preconditions.checkNotNull(dbLocation, "Invalid location of the db");
    Options options = new Options();
    options.createIfMissing(true);
    db = factory.open(new File(dbLocation), options);
  }

  @Override
  public JobDefinition get(UUID uuid) throws IOException {
    byte[] data = db.get(uuid.toString().getBytes(UTF_8));
    if (data == null) {
      return null;
    }
    return MAPPER.readValue(data, JobDefinition.class);
  }

  @Override
  public void updateJob(UUID uuid, JobDefinition job) throws IOException {
    try (WriteBatch wb = db.createWriteBatch()) {
      wb.put(uuid.toString().getBytes(UTF_8), MAPPER.writeValueAsBytes(job));
      db.write(wb);
    }
  }

  @Override
  public void removeJob(UUID uuid) throws IOException {
    try (WriteBatch wb = db.createWriteBatch()) {
      wb.delete(uuid.toString().getBytes(UTF_8));
      db.write(wb);
    }
  }

  @Override
  public List<ExtendedJobDefinition> listAll() throws IOException {
    ArrayList<ExtendedJobDefinition> jobs = new ArrayList<>();
    try (DBIterator it = db.iterator()) {
      for (it.seekToFirst(); it.hasNext(); it.next()) {
        Map.Entry<byte[], byte[]> e = it.peekNext();
        UUID uuid = UUID.fromString(new String(e.getKey(), UTF_8));
        JobDefinition def = MAPPER.readValue(e.getValue(), JobDefinition.class);
        jobs.add(new ExtendedJobDefinition().uuid(uuid).definition(def));
      }
    }
    return jobs;
  }

  @Override
  public void close() throws IOException {
    if (db != null) {
      db.close();
      db = null;
    }
  }
}
