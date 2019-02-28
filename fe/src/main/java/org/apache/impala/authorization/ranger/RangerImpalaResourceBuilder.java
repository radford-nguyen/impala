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

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

/**
 * A builder for creating {@link RangerAccessResourceImpl} instance.
 */
public class RangerImpalaResourceBuilder {
  public static final String SERVER = "server";
  public static final String DATABASE = "database";
  public static final String TABLE = "table";
  public static final String COLUMN = "column";
  public static final String FUNCTION = "function";
  public static final String URI = "uri";

  private final RangerAccessResourceImpl rangerAccessResource =
      new RangerAccessResourceImpl();

  public RangerImpalaResourceBuilder server(String serverName) {
    rangerAccessResource.setValue(SERVER, serverName);
    return this;
  }

  public RangerImpalaResourceBuilder database(String dbName) {
    rangerAccessResource.setValue(DATABASE, dbName);
    return this;
  }

  public RangerImpalaResourceBuilder table(String tableName) {
    rangerAccessResource.setValue(TABLE, tableName);
    return this;
  }

  public RangerImpalaResourceBuilder column(String columnName) {
    rangerAccessResource.setValue(COLUMN, columnName);
    return this;
  }

  public RangerImpalaResourceBuilder function(String fnName) {
    rangerAccessResource.setValue(FUNCTION, fnName);
    return this;
  }

  public RangerImpalaResourceBuilder uri(String uri) {
    rangerAccessResource.setValue(URI, uri);
    return this;
  }

  public RangerAccessResourceImpl build() { return rangerAccessResource; }
}
