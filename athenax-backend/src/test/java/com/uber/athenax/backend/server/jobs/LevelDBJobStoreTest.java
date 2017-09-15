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

import com.uber.athenax.backend.api.JobDefinition;
import com.uber.athenax.backend.server.AthenaXConfiguration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static com.uber.athenax.backend.server.AthenaXExtraConfigOptions.JOBSTORE_LEVELDB_FILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LevelDBJobStoreTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testLevelDBStore() throws IOException {
    UUID jobUUID = UUID.randomUUID();
    JobDefinition def = new JobDefinition()
        .query("foo");
    AthenaXConfiguration conf = mock(AthenaXConfiguration.class);
    doReturn(Collections.singletonMap(JOBSTORE_LEVELDB_FILE.key(), folder.newFolder("db").getAbsolutePath()))
    .when(conf).extras();
    try (LevelDBJobStore db = new LevelDBJobStore()) {
      db.open(conf);
      assertTrue(db.listAll().isEmpty());
      db.updateJob(jobUUID, def);
      assertEquals(def.getQuery(), db.get(jobUUID).getQuery());
      assertEquals(1, db.listAll().size());
      db.removeJob(jobUUID);
      assertNull(db.get(jobUUID));
    }
  }
}
