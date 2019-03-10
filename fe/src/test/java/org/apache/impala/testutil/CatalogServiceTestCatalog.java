// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.testutil;

import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.sentry.SentryAuthorizationConfig;
import org.apache.impala.authorization.sentry.SentryConfig;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TUniqueId;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Test class of the Catalog Server's catalog that exposes internal state that is useful
 * for testing.
 */
public class CatalogServiceTestCatalog extends CatalogServiceCatalog {
  public CatalogServiceTestCatalog(boolean loadInBackground, int numLoadingThreads,
      AuthorizationConfig authConfig, TUniqueId catalogServiceId,
      MetaStoreClientPool metaStoreClientPool) throws ImpalaException {
    super(loadInBackground, numLoadingThreads, authConfig, catalogServiceId, null,
        System.getProperty("java.io.tmpdir"), metaStoreClientPool);

    // Cache pools are typically loaded asynchronously, but as there is no fixed execution
    // order for tests, the cache pools are loaded synchronously before the tests are
    // executed.
    CachePoolReader rd = new CachePoolReader(false);
    rd.run();
  }

  public static CatalogServiceCatalog create() {
    return createWithAuth(new SentryAuthorizationConfig(new SentryConfig(null)));
  }

  /**
   * Creates a catalog server that reads authorization policy metadata from the
   * authorization config.
   */
  public static CatalogServiceCatalog createWithAuth(AuthorizationConfig config) {
    FeSupport.loadLibrary();
    CatalogServiceCatalog cs;
    try {
      cs = new CatalogServiceTestCatalog(false, 16, config, new TUniqueId(),
          new MetaStoreClientPool(0, 0));
      cs.reset();
    } catch (ImpalaException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    return cs;
  }

  /**
   * Creates a transient test catalog instance backed by an embedded HMS derby database on
   * the local filesystem. The derby database is created from scratch and has no table
   * metadata.
   */
  public static CatalogServiceCatalog createTransientTestCatalog() throws
      ImpalaException {
    FeSupport.loadLibrary();
    Path derbyPath = Paths.get(System.getProperty("java.io.tmpdir"),
        UUID.randomUUID().toString());
    CatalogServiceCatalog cs = new CatalogServiceTestCatalog(false, 16, null,
        new TUniqueId(), new EmbeddedMetastoreClientPool(0, derbyPath));
    cs.reset();
    return cs;
  }

  @Override
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }
}
