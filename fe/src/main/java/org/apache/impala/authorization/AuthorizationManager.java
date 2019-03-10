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

package org.apache.impala.authorization;

import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TCreateDropRoleParams;
import org.apache.impala.thrift.TDdlExecResponse;
import org.apache.impala.thrift.TGrantRevokePrivParams;
import org.apache.impala.thrift.TGrantRevokeRoleParams;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TShowGrantPrincipalParams;
import org.apache.impala.thrift.TShowRolesParams;
import org.apache.impala.thrift.TShowRolesResult;

/**
 * This interface provides functionalities to manage authorization, such as grant, revoke,
 * show grant, etc.
 *
 * The implementer of this interface may need to update the TDdlExecResponse passed.
 */
public interface AuthorizationManager {
  /**
   * Returns true if the given user is an admin user.
   */
  boolean isAdmin(User user) throws ImpalaException;

  /**
   * Creates a role.
   */
  void createRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Drops a role.
   */
  void dropRole(User requestingUser, TCreateDropRoleParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Gets all roles;
   */
  TShowRolesResult getRoles(TShowRolesParams params) throws ImpalaException;

  /**
   * Grants a role to a group.
   */
  void grantRoleToGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Revokes a role from a group.
   */
  void revokeRoleFromGroup(User requestingUser, TGrantRevokeRoleParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Grant a privilege to a role.
   */
  void grantPrivilegeToRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Revokes a privilege from a role.
   */
  void revokePrivilegeFromRole(User requestingUser, TGrantRevokePrivParams params,
      TDdlExecResponse response) throws ImpalaException;

  /**
   * Gets all privileges.
   */
  TResultSet getPrivileges(TShowGrantPrincipalParams params) throws ImpalaException;

  /**
   * Grants/revokes an owner privilege for the database, such as database creation,
   * removal, etc.
   */
  void updateDatabaseOwnerPrivilege(String serverName, String databaseName,
      String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException;

  /**
   * Grants/revokes an owner privilege for the table, such as table creation, removal,
   * rename, etc.
   */
  void updateTableOwnerPrivilege(String serverName, String databaseName, String tableName,
      String oldOwner, PrincipalType oldOwnerType, String newOwner,
      PrincipalType newOwnerType, TDdlExecResponse response) throws ImpalaException;
}
