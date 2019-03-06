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

package org.apache.impala.authorization.ranger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.AuthorizationManager;
import org.apache.impala.authorization.AuthorizationPolicy;
import org.apache.impala.authorization.NoneAuthorizationFactory.NoneAuthorizationManager;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.FeCatalogManager;

/**
 * An implementation of {@link AuthorizationFactory} that uses Ranger.
 */
public class RangerAuthorizationFactory implements AuthorizationFactory {
  private final AuthorizationConfig authzConfig_;

  public RangerAuthorizationFactory(BackendConfig backendConfig) {
    Preconditions.checkNotNull(backendConfig);
    authzConfig_ = newAuthorizationConfig(backendConfig);
  }

  /**
   * This is for testing.
   */
  @VisibleForTesting
  public RangerAuthorizationFactory(AuthorizationConfig authzConfig) {
    Preconditions.checkNotNull(authzConfig);
    authzConfig_ = authzConfig;
  }

  @Override
  public AuthorizationConfig newAuthorizationConfig(BackendConfig backendConfig) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(
        backendConfig.getBackendCfg().getServer_name()));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(
        backendConfig.getRangerServiceType()));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(backendConfig.getRangerAppId()));
    return new RangerAuthorizationConfig(backendConfig.getRangerServiceType(),
        backendConfig.getRangerAppId(), backendConfig.getBackendCfg().getServer_name());
  }

  @Override
  public AuthorizationConfig getAuthorizationConfig() { return authzConfig_; }

  @Override
  public AuthorizationChecker newAuthorizationChecker(AuthorizationPolicy authzPolicy) {
    return new RangerAuthorizationChecker(authzConfig_);
  }

  @Override
  public AuthorizationManager newAuthorizationManager(FeCatalogManager catalog,
      AuthorizationChecker authzChecker) {
    // TODO: return an actual implementation of AuthorizationManager for Ranger.
    return new NoneAuthorizationManager();
  }

  @Override
  public AuthorizationManager newAuthorizationManager(CatalogServiceCatalog catalog) {
    // TODO: return an actual implementation of AuthorizationManager for Ranger.
    return new NoneAuthorizationManager();
  }
}
