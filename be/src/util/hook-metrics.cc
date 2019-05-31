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

#include "util/hook-metrics.h"
#include "common/logging.h"

using std::map;
using std::pair;
using std::string;

namespace {

/// Following metric names must match with the key in metrics.json

/// metric name for hook submissions counter
const char* NUM_SUBMITTED_METRIC_NAME =
    "query-event-hook.$0.execution-submissions";

/// metric name for hook rejections counter
const char* NUM_REJECTED_METRIC_NAME =
    "query-event-hook.$0.execution-rejections";

/// metric name for hook exceptions counter
const char* NUM_EXCEPTIONED_METRIC_NAME =
    "query-event-hook.$0.execution-exceptions";

/// metric name for hook timeouts counter
const char* NUM_TIMEDOUT_METRIC_NAME =
    "query-event-hook.$0.execution-timeouts";

/// metric name for hook execution mean time
const char* EXECUTION_DURATION_MEAN_METRIC_NAME =
    "query-event-hook.$0.mean-execution-time";

/// metric name for hook queued mean time
const char* QUEUED_DURATION_MEAN_METRIC_NAME =
    "query-event-hook.$0.mean-queued-time";
}

namespace impala {

QueryEventHookMetricContainer::QueryEventHookMetricContainer(
    string hook_name, MetricGroup* metric_group) {

  MetricGroup* hook_metric_group = metric_group->GetOrCreateChildGroup("hooks");

  num_submitted_counter_ = hook_metric_group->RegisterMetric(new IntCounter(
      MetricDefs::Get(NUM_SUBMITTED_METRIC_NAME, hook_name), 0));

  num_rejected_counter_ = hook_metric_group->RegisterMetric(new IntCounter(
      MetricDefs::Get(NUM_REJECTED_METRIC_NAME, hook_name), 0));

  num_exceptioned_counter_ = hook_metric_group->RegisterMetric(new IntCounter(
      MetricDefs::Get(NUM_EXCEPTIONED_METRIC_NAME, hook_name), 0));

  num_timedout_counter_ = hook_metric_group->RegisterMetric(new IntCounter(
      MetricDefs::Get(NUM_TIMEDOUT_METRIC_NAME, hook_name), 0));

  hook_execution_duration_mean_ns_ = hook_metric_group->RegisterMetric(new DoubleGauge(
      MetricDefs::Get(EXECUTION_DURATION_MEAN_METRIC_NAME, hook_name), 0));

  hook_queued_duration_mean_ns_ = hook_metric_group->RegisterMetric(new DoubleGauge(
      MetricDefs::Get(QUEUED_DURATION_MEAN_METRIC_NAME, hook_name), 0));
}

map<string, QueryEventHookMetricContainer> QueryEventHookMetrics::metric_containers_;

void QueryEventHookMetricContainer::Update(const TQueryEventHookMetrics& from) {
  num_submitted_counter_->SetValue(from.num_execution_submissions);
  num_rejected_counter_->SetValue(from.num_execution_rejections);
  num_exceptioned_counter_->SetValue(from.num_execution_exceptions);
  num_timedout_counter_->SetValue(from.num_execution_timeouts);
  hook_execution_duration_mean_ns_->SetValue(from.execution_mean_time);
  hook_queued_duration_mean_ns_->SetValue(from.queued_mean_time);
}

void QueryEventHookMetrics::Refresh(MetricGroup* metric_group,
                                    TGetQueryEventHookMetricsResult* response) {

  if (!response) {
    LOG(ERROR) << "Received null TGetQueryEventHookMetricsResult* when "
               << "trying to refresh QueryEventHook metrics";
    return;
  }

  if (!metric_group) {
    LOG(ERROR) << "Received null MetricGroup* when "
               << "trying to refresh QueryEventHook metrics";
    return;
  }

  for (auto hook_metrics = response->hook_metrics.begin();
      hook_metrics != response->hook_metrics.end();
      ++hook_metrics) {

    string hook_name = hook_metrics->first;
    TQueryEventHookMetrics metrics = hook_metrics->second;

    auto hook_metric_container = metric_containers_.find(hook_name);
    if (hook_metric_container == metric_containers_.end()) {
      // this *should* only happen on initialization because
      // metrics should always be refreshed from the frontend's metrics response,
      // which *should* always contain the same hook names
      // as they are defined at impala startup
      LOG(INFO) << "Initializing metrics for QueryEventHook name: " << hook_name;

      QueryEventHookMetricContainer& new_metrics =
          InitializeSingleHookMetrics(hook_name, metric_group);

      new_metrics.Update(metrics);
    } else {
      hook_metric_container->second.Update(metrics);
    }
  }
}

QueryEventHookMetricContainer& QueryEventHookMetrics::InitializeSingleHookMetrics(
    string hook_name,
    MetricGroup* metric_group) {

  DCHECK(metric_containers_.find(hook_name) == metric_containers_.end());

  QueryEventHookMetricContainer new_container(hook_name, metric_group);

  pair<map<string, QueryEventHookMetricContainer>::iterator, bool> ret =
      metric_containers_.insert(pair<string, QueryEventHookMetricContainer>(
          hook_name, new_container));
  return ret.first->second;
}

}
