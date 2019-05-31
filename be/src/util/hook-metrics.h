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

#ifndef IMPALA_UTIL_HOOK_METRICS_H
#define IMPALA_UTIL_HOOK_METRICS_H

#include "util/metrics.h"

namespace impala {

/// This class represents an instance of metrics collected for
/// particular QueryEventHook method.  QueryEventHooks are
/// user-provided runtime dependencies that run in the
/// frontend
class QueryEventHookMetricContainer {
public:
  QueryEventHookMetricContainer(string hook_name, MetricGroup* metric_group);

  /// Updates this container instance to reflect the info
  /// stored in `from` (typically sent from the frontend)
  void Update(const TQueryEventHookMetrics& from);

  /// Total number of times this hook has been submitted for execution
  IntCounter* num_submitted_counter_;

  /// Total number of times this hook method has been rejected
  IntCounter* num_rejected_counter_;

  /// Total number of times this hook method has exceptioned
  IntCounter* num_exceptioned_counter_;

  /// Total number of times this hook method has timed out and been cancelled
  IntCounter* num_timedout_counter_;

  /// Mean duration of this hook method's actual execution time
  DoubleGauge* hook_execution_duration_mean_ns_;

  /// Mean duration submitted hook has spent queued awaiting execution start
  DoubleGauge* hook_queued_duration_mean_ns_;

};

/// Provides static methods for updating QueryEventHook metrics
class QueryEventHookMetrics {
public:

  /// refresh all the metrics which are used to display on webui based on the given
  /// response this method should be called at regular intervals to update the metrics
  /// information on the webui
  static void Refresh(MetricGroup* metric_group,
                      TGetQueryEventHookMetricsResult* response);

private:

  /// A registry of all hook metrics ever encountered via Refresh, keyed
  /// by hook_name.  This exists to cache the metrics by hook_name so that
  /// we don't have to repeatedly look up each individual metric
  static std::map</* hook_name */ std::string,
                  QueryEventHookMetricContainer> metric_containers_;

  /// Initializes a QueryEventHookMetricContainer and adds it to the `metric_containers_`
  /// map.  This will fail a DCHECK if a container already exists for the
  /// given `hook_name`
  static QueryEventHookMetricContainer&
      InitializeSingleHookMetrics(std::string hook_name, MetricGroup* metric_group);
};


} // namespace impala

#endif
