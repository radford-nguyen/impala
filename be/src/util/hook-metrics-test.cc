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

#include "testutil/gtest-util.h"
#include "util/hook-metrics.h"

namespace impala {

TEST(QueryEventHookMetricContainerTest, UpdateTest) {
  string hook_name = "somehook.somemethod";
  MetricGroup metric_group("hook-metrics-test");

  // SUT == System Under Test
  QueryEventHookMetricContainer SUT(hook_name, &metric_group);

  EXPECT_EQ(0, SUT.num_submitted_counter_->GetValue());
  EXPECT_EQ(0, SUT.num_rejected_counter_->GetValue());
  EXPECT_EQ(0, SUT.num_exceptioned_counter_->GetValue());
  EXPECT_EQ(0, SUT.num_timedout_counter_->GetValue());
  EXPECT_EQ(0, SUT.hook_execution_duration_mean_ns_->GetValue());
  EXPECT_EQ(0, SUT.hook_queued_duration_mean_ns_->GetValue());

  const int64_t num_execution_rejections = 3;
  const int64_t num_execution_exceptions = 6;
  const int64_t num_execution_timeouts = 9;
  const int64_t num_execution_submissions = 11;
  const double execution_mean_time = 455;
  const double queued_mean_time = 6672;

  TQueryEventHookMetrics tmetrics;
  tmetrics.__set_num_execution_submissions(num_execution_submissions);
  tmetrics.__set_num_execution_rejections(num_execution_rejections);
  tmetrics.__set_num_execution_exceptions(num_execution_exceptions);
  tmetrics.__set_num_execution_timeouts(num_execution_timeouts);
  tmetrics.__set_execution_mean_time(execution_mean_time);
  tmetrics.__set_queued_mean_time(queued_mean_time);

  SUT.Update(tmetrics);

  EXPECT_EQ(num_execution_rejections, SUT.num_submitted_counter_->GetValue());
  EXPECT_EQ(num_execution_exceptions, SUT.num_rejected_counter_->GetValue());
  EXPECT_EQ(num_execution_timeouts, SUT.num_exceptioned_counter_->GetValue());
  EXPECT_EQ(num_execution_submissions, SUT.num_timedout_counter_->GetValue());
  EXPECT_EQ(execution_mean_time, SUT.hook_execution_duration_mean_ns_->GetValue());
  EXPECT_EQ(queued_mean_time, SUT.hook_queued_duration_mean_ns_->GetValue());
}

}
