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

package org.apache.impala.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.authorization.ranger.RangerAuthorizationFactory;
import org.apache.impala.authorization.sentry.SentryAuthorizationFactory;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TBackendGflags;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JniFrontendTest {

  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // since some test cases will need to modify the (static)
    // be flags, we need to save the original values so they
    // can be restored and not break other tests
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testCheckGroupsMappingProvider() throws ImpalaException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        JniBasedUnixGroupsMappingWithFallback.class.getName());
    assertTrue(JniFrontend.checkGroupsMappingProvider(conf).isEmpty());

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsMapping.class.getName(),
            JniBasedUnixGroupsMappingWithFallback.class.getName()));

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsNetgroupMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsNetgroupMapping.class.getName(),
            JniBasedUnixGroupsNetgroupMappingWithFallback.class.getName()));
  }

  private static BackendConfig authCfg(
      String authorization_factory_class, String authorization_provider) {

    final TBackendGflags stubCfg = new TBackendGflags();
    stubCfg.setAuthorization_factory_class(authorization_factory_class);
    stubCfg.setAuthorization_provider(authorization_provider);
    BackendConfig.create(stubCfg);
    return BackendConfig.INSTANCE;
  }

  private static BackendConfig authCfg(
      Class<? extends AuthorizationFactory> authorization_factory_class,
      String authorization_provider) {

    return authCfg(
        authorization_factory_class.getCanonicalName(),
        authorization_provider
    );
  }

  @Test
  public void testAuthorizationPolicySelection_onlyProviderSpecified() throws Exception {
    assertEquals(
        SentryAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(authCfg("", "sentry"))
    );

    assertEquals(
        RangerAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(authCfg("", "ranger"))
    );
  }

  @Test
  public void testAuthorizationPolicySelection_FactoryTakesPrecedenceOverProvider()
      throws Exception {
    assertEquals(
        SentryAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(
            authCfg(SentryAuthorizationFactory.class, "ranger")
        )
    );

    assertEquals(
        RangerAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(
            authCfg(RangerAuthorizationFactory.class, "sentry")
        )
    );

    assertEquals(
        SentryAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(
              authCfg(SentryAuthorizationFactory.class, "sentry")
        )
    );

    assertEquals(
        RangerAuthorizationFactory.class.getCanonicalName(),
        JniFrontend.authzFactoryClassNameFrom(
            authCfg(RangerAuthorizationFactory.class, "ranger")
        )
    );
  }

}