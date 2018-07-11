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

package com.uber.athenax.vm.api.tables;

import java.util.List;
import java.util.Map;

/**
 * AthenaXTableCatalogProvider provides the catalogs for all clusters.
 */
public interface AthenaXTableCatalogProvider {
  /**
   * Return the catalogs for input tables for a specific cluster.
   *
   * @param cluster the name of the cluster
   * @return a map from catalog name to the catalog.
   */
  Map<String, AthenaXTableCatalog> getInputCatalog(String cluster);

  /**
   * Generate a output catalog for a specific cluster.
   *
   * @param cluster the name of the cluster
   * @param outputs customized strings that define the outputs.
   * @return a catalog that describes all the outputs.
   */
  AthenaXTableCatalog getOutputCatalog(String cluster, List<String> outputs);
}
