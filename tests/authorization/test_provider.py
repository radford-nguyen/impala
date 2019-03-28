# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Client tests for SQL statement authorization

import pytest
import time
from getpass import getuser

from tests.util.filesystem_utils import IS_LOCAL

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite,
																									 IMPALAD_ARGS,
																									 STATESTORED_ARGS,
																									 CATALOGD_ARGS,
																									 SENTRY_CONFIG,
																									 DEFAULT_CLUSTER_SIZE,
																									 CLUSTER_SIZE,
																									 IMPALA_LOG_DIR,
																									 DEFAULT_QUERY_OPTIONS
 

class TestProvider(ImpalaTestSuite):
  """
  Tests for internally-supported authorization provider selection with Impala.
  """
  @pytest.mark.execute_serially
  def test_invalid_provider_flag(self, unique_name):
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala "
                 "--authorization_provider=foobar"

    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala "
                  "--authorization_provider=foobar"

    cluster_args = [
				"--%s=%s " % (IMPALAD_ARGS, impalad_args),
				"--%s=%s " % (CATALOGD_ARGS, catalogd_args)
		]


    if SENTRY_CONFIG in method.func_dict:
      self._start_sentry_service(method.func_dict[SENTRY_CONFIG],
          method.func_dict.get(SENTRY_LOG_DIR))

    cluster_size = DEFAULT_CLUSTER_SIZE
    if CLUSTER_SIZE in method.func_dict:
      cluster_size = method.func_dict[CLUSTER_SIZE]

    # Start a clean new cluster before each test
    if IMPALA_LOG_DIR in method.func_dict:
      self._start_impala_cluster(cluster_args,
          default_query_options=method.func_dict.get(DEFAULT_QUERY_OPTIONS),
          impala_log_dir=method.func_dict[IMPALA_LOG_DIR], cluster_size=cluster_size)
    else:
      self._start_impala_cluster(cluster_args,
          default_query_options=method.func_dict.get(DEFAULT_QUERY_OPTIONS),
          cluster_size=cluster_size, num_coordinators=cluster_size,
          expected_num_executors=cluster_size)
    super(CustomClusterTestSuite, self).setup_class()

  @classmethod
  def setup_class(cls):
    # Explicit override of ImpalaTestSuite.setup_class(). For custom cluster, the
    # ImpalaTestSuite.setup_class() procedure needs to happen on a per-method basis.
    # IMPALA-3614: @SkipIfLocal.multiple_impalad workaround
    # IMPALA-2943 TODO: When pytest is upgraded, see if this explicit skip can be
    # removed in favor of the class-level SkipifLocal.multiple_impalad decorator.
    if IS_LOCAL:
      pytest.skip("multiple impalads needed")

  @classmethod
  def teardown_class(cls):
    # Explicit override of ImpalaTestSuite.teardown_class(). For custom cluster, the
    # ImpalaTestSuite.teardown_class() procedure needs to happen on a per-method basis.
    pass

  def teardown_method(self, method):
    super(TestProvider, self).teardown_class()

