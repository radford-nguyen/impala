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
import org.apache.impala.authorization.Authorizable;
import org.apache.impala.authorization.AuthorizationChecker;
import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.authorization.DefaultAuthorizableFactory;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.authorization.PrivilegeRequest;
import org.apache.impala.authorization.User;
import org.apache.impala.common.InternalException;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;

import java.util.EnumSet;
import java.util.HashSet;

/**
 * An implementation of {@link AuthorizationChecker} that uses Ranger.
 */
public class RangerAuthorizationChecker extends AuthorizationChecker {
  private final RangerImpalaPlugin plugin_;

  public RangerAuthorizationChecker(AuthorizationConfig authzConfig) {
    super(authzConfig);
    Preconditions.checkArgument(authzConfig instanceof RangerAuthorizationConfig);
    RangerAuthorizationConfig rangerConfig = (RangerAuthorizationConfig) authzConfig;
    plugin_ = new RangerImpalaPlugin(
        rangerConfig.getServiceType(), rangerConfig.getAppId());
    plugin_.init();
  }

  @Override
  protected boolean authorize(User user, PrivilegeRequest request)
      throws InternalException {
    Preconditions.checkNotNull(user);
    Preconditions.checkNotNull(request);
    RangerImpalaResourceBuilder builder = new RangerImpalaResourceBuilder();
    Authorizable authorizable = request.getAuthorizable();
    switch (authorizable.getType()) {
      case SERVER:
        builder.server(config_.getServerName());
        break;
      case DB:
        builder.server(config_.getServerName())
            .database(authorizable.getDbName());
        break;
      case TABLE:
        builder.server(config_.getServerName())
            .database(authorizable.getDbName())
            .table(authorizable.getTableName());
        break;
      case COLUMN:
        builder.server(config_.getServerName())
            .database(authorizable.getDbName());
        // Any column access is special in Ranger. For example if we want to check if
        // we have access to any column on a particular table, we need to build a resource
        // without the column resource.
        // For example:
        // access type: RangerPolicyEngine.ANY_ACCESS
        // resources: [server=server1, database=foo, table=bar]
        if (request.getPrivilege() != Privilege.ANY ||
          !DefaultAuthorizableFactory.ALL.equals(authorizable.getTableName())) {
          builder.table(authorizable.getTableName());
        }
        if (request.getPrivilege() != Privilege.ANY ||
          !DefaultAuthorizableFactory.ALL.equals(authorizable.getColumnName())) {
          builder.column(authorizable.getColumnName());
        }
        break;
      case FUNCTION:
        builder.server(config_.getServerName())
            .database(authorizable.getDbName())
            .function(authorizable.getFnName());
        break;
      case URI:
        builder.server(config_.getServerName())
            .uri(authorizable.getName());
        break;
      default:
        throw new IllegalArgumentException(String.format("Invalid authorizable type: %s",
            authorizable.getType()));
    }
    RangerAccessResourceImpl resource = builder.build();
    if (request.getPrivilege() == Privilege.VIEW_METADATA) {
      return authorizeAnyOf(plugin_, resource, user,
          request.getPrivilege().getImpliedPrivileges());
    }
    return authorize(plugin_, resource, user, request.getPrivilege());
  }

  private static boolean authorizeAnyOf(RangerImpalaPlugin plugin,
      RangerAccessResourceImpl resource, User user, EnumSet<Privilege> privileges)
      throws InternalException {
    for (Privilege privilege: privileges) {
      if (authorize(plugin, resource, user, privilege)) {
        return true;
      }
    }
    return false;
  }

  private static boolean authorize(RangerImpalaPlugin plugin,
      RangerAccessResourceImpl resource, User user, Privilege privilege)
      throws InternalException {
    RangerAccessRequest request = new RangerAccessRequestImpl(resource,
        privilege == Privilege.ANY ?
            RangerPolicyEngine.ANY_ACCESS :
            privilege.toString().toLowerCase(),
        user.getShortName(), new HashSet<>());
    RangerAccessResult result = plugin.isAccessAllowed(request);
    return result != null && result.getIsAllowed();
  }

  @VisibleForTesting
  public RangerImpalaPlugin getRangerImpalaPlugin() { return plugin_; }
}
