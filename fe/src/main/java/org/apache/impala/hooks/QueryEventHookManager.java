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
package org.apache.impala.hooks;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Metrics;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link QueryEventHookManager} manages the registration and execution of
 * {@link QueryEventHook}s. Each manager instance may manage its own hooks,
 * though the expected use-case is to have 1 instance per process, usually
 * owned by the frontend. This class is not thread-safe.
 *
 * <h3>Hook Registration</h3>
 *
 * The hook implementation(s) to use at runtime are specified through the
 * backend config flag {@link TBackendGflags#query_event_hook_classes}
 * at Impala startup. See {@link #createFromConfig(BackendConfig)}.
 *
 * <h3>Hook Classloading</h3>
 *
 * Each hook implementation is loaded using `this` manager's classloader; no
 * classloader isolation is performed.  Individual hook implementations should
 * take care to properly handle any dependencies they bring in to avoid shadowing
 * existing dependencies on the Impala classpath.
 *
 * <h3>Hook Execution</h3>
 *
 * Hook initialization ({@link QueryEventHook#onImpalaStartup()} is
 * performed synchronously during {@link #createFromConfig(BackendConfig)}.
 * <p>
 * {@link QueryEventHook#onQueryComplete(QueryCompleteContext)} is performed
 * asynchronously during {@link #executeQueryCompleteHooks(QueryCompleteContext)}.
 * This execution is performed by a thread-pool executor, whose size is set at
 * compile-time.  This means that hooks may also execute concurrently.
 * </p>
 * <p>
 * For a more-detailed description of how
 * {@link QueryEventHook#onQueryComplete(QueryCompleteContext)} is executed, please
 * reference {@link FixedCapacityQueryHookExecutor}.
 * </p>
 *
 */
public class QueryEventHookManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryEventHookManager.class);

  // constants to share with the executor so that no mismatches
  // occur when looking up method or flag names
  //
  // it'd be nice if we could get the flag names from the be
  // defn so we don't have to manually sync if they change
  static final String ON_QUERY_COMPLETE = "onQueryComplete";
  static final String BE_HOOKS_FLAG = "query_event_hook_classes";
  static final String BE_HOOKS_THREADS_FLAG = "query_event_hook_nthreads";
  static final String BE_HOOKS_TIMEOUT_FLAG = "query_event_hook_timeout_s";
  static final String BE_HOOKS_DAEMONTHREADS_FLAG = "query_event_hook_use_daemon_threads";
  static final String BE_HOOKS_QUEUE_CAPACITY_FLAG = "query_event_hook_queue_capacity";

  private final List<QueryEventHook> hooks_;
  private final FixedCapacityQueryHookExecutor hookExecutor_;

  // TODO: (IMPALA-8914) pass metrics to backend for publishing
  private final Metrics hookMetrics_;

  /**
   * Static factory method to create a manager instance.  This will register
   * all {@link QueryEventHook}s specified by the backend config flag
   * {@code query_event_hook_classes} and then invoke their
   * {@link QueryEventHook#onImpalaStartup()} methods synchronously.
   *
   * @throws IllegalArgumentException if config is invalid
   * @throws InternalException if any hook could not be instantiated
   * @throws InternalException if any hook.onImpalaStartup() throws an exception
   */
  public static QueryEventHookManager createFromConfig(BackendConfig config)
      throws InternalException {

    final int hookTimeout_s = config.getQueryExecHookTimeout_s();
    final int nHookThreads = config.getNumQueryExecHookThreads();
    final String queryExecHookClasses = config.getQueryExecHookClasses();
    final boolean useDaemonThreads = config.getQueryEventHookDaemonThreads();
    final int queueCapacity = config.getQueryExecHookQueueCapacity();
    LOG.info("QueryEventHook config:");
    LOG.info("- {}={}", BE_HOOKS_THREADS_FLAG, nHookThreads);
    LOG.info("- {}={}", BE_HOOKS_FLAG, queryExecHookClasses);
    LOG.info("- {}={}", BE_HOOKS_TIMEOUT_FLAG, hookTimeout_s);
    LOG.info("- {}={}", BE_HOOKS_DAEMONTHREADS_FLAG, useDaemonThreads);
    LOG.info("- {}={}", BE_HOOKS_QUEUE_CAPACITY_FLAG, queueCapacity);

    final String[] hookClasses;
    if (StringUtils.isNotEmpty(queryExecHookClasses)) {
      hookClasses = queryExecHookClasses.split("\\s*,\\s*");
    } else {
      hookClasses = new String[0];
    }

    return new QueryEventHookManager(
        nHookThreads, hookClasses, hookTimeout_s, useDaemonThreads, queueCapacity);
  }

  /**
   * Instantiates a manager with a fixed-size thread-pool executor for
   * executing {@link QueryEventHook#onQueryComplete(QueryCompleteContext)}.
   *
   * @param nHookExecutorThreads
   * @param hookClasses
   * @param hookTimeout_s
   * @param useDaemonThreads
   *
   * @throws IllegalArgumentException if {@code nHookExecutorThreads <= 0}
   * @throws IllegalArgumentException if {@code hookTimeout_s <= 0}
   * @throws IllegalArgumentException if {@code queueCapacity <= 0}
   * @throws InternalException if any hookClass cannot be instantiated
   * @throws InternalException if any hookClass.onImpalaStartup throws an exception
   */
  private QueryEventHookManager(int nHookExecutorThreads, String[] hookClasses,
                                int hookTimeout_s, boolean useDaemonThreads,
                                int queueCapacity)
      throws InternalException {

    if (nHookExecutorThreads <= 0) {
      final String msg = String.format("# hook threads should be positive but was {}. " +
          "Please check your %s flag", hookTimeout_s, BE_HOOKS_THREADS_FLAG);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (hookTimeout_s <= 0) {
      final String msg = String.format("hook timeout should be positive but was {}. " +
          "Please check your %s flag", hookTimeout_s, BE_HOOKS_TIMEOUT_FLAG);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    if (queueCapacity <= 0) {
      final String msg = String.format("hook queue capacity should be >= 0 but was {}. " +
          "Please check your %s flag", queueCapacity, BE_HOOKS_QUEUE_CAPACITY_FLAG);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }

    this.hookMetrics_ = new Metrics();
    this.hookExecutor_ = new FixedCapacityQueryHookExecutor(
        nHookExecutorThreads, hookTimeout_s, TimeUnit.SECONDS,
        queueCapacity, useDaemonThreads, this.hookMetrics_);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> this.cleanUp()));

    final List<QueryEventHook> hooks = new ArrayList<>(hookClasses.length);
    this.hooks_ = Collections.unmodifiableList(hooks);

    for (String postExecHook : hookClasses) {
      final QueryEventHook hook;
      try {
        final Class<QueryEventHook> clsHook =
            (Class<QueryEventHook>) Class.forName(postExecHook);
        hook = clsHook.newInstance();
      } catch (InstantiationException
          | IllegalAccessException
          | ClassNotFoundException e) {
        final String msg = String.format(
            "Unable to instantiate query event hook class %s. Please check %s config",
            postExecHook, BE_HOOKS_FLAG);
        LOG.error(msg, e);
        throw new InternalException(msg, e);
      }

      hooks.add(hook);
    }

    for (QueryEventHook hook : hooks) {
      try {
        LOG.debug("Initiating hook.onImpalaStartup for {}", hook.getClass().getName());
        hook.onImpalaStartup();
      } catch (Exception e) {
        final String msg = String.format(
            "Exception during onImpalaStartup from QueryEventHook %s instance=%s",
            hook.getClass(), hook);
        LOG.error(msg, e);
        throw new InternalException(msg, e);
      }
    }
  }

  private void cleanUp() {
    hookExecutor_.shutdown();
  }

  /**
   * Returns an unmodifiable view of all the {@link QueryEventHook}s
   * registered at construction.
   *
   * @return unmodifiable view of all currently-registered hooks
   */
  public List<QueryEventHook> getHooks() {
    return hooks_;
  }

  /**
   * Hook method to be called after query execution.  This implementation
   * will execute all currently-registered {@link QueryEventHook}s
   * asynchronously, returning immediately.  Actual execution is governed by
   * {@link FixedCapacityQueryHookExecutor}, which you can reference for
   * more details.
   *
   * @param context
   */
  public void executeQueryCompleteHooks(QueryCompleteContext context) {
    _executeQueryCompleteHooks(context);
  }

  @VisibleForTesting
  List<Future<QueryEventHook>> _executeQueryCompleteHooks(
      QueryCompleteContext context) {
    LOG.debug("Query complete hook invoked with: {}", context);
    return hooks_.stream().map(hook -> {
      LOG.debug("Submitting onQueryComplete: {}", hook.getClass().getName());
      return hookExecutor_.submitOnQueryComplete(hook, context);
    }).collect(Collectors.toList());
  }

}
