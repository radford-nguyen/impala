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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.impala.common.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.impala.hooks.QueryEventHookManager.BE_HOOKS_TIMEOUT_FLAG;
import static org.apache.impala.hooks.QueryEventHookManager.ON_QUERY_COMPLETE;

/**
 * {@link FixedCapacityQueryHookExecutor} is an execution engine for
 * {@link QueryEventHook}s that utilizes a fixed-capacity thread pool pulling
 * tasks from a fixed-capacity work queue while also supporting a rudimentary
 * timeout mechanism for cancelling long-running hook tasks.
 *
 *
 * ### Rejections
 *
 * The work queue for submitted tasks awaiting a free thread for execution
 * is bounded and its capacity set at construction-time.  If any tasks are
 * submitted while the queue is full, they will be rejected and will not
 * ever execute.
 *
 * When rejection occurs, it will be logged and captured in the performance
 * metrics.  Other than the this and the returned {@link Future} from task
 * submission, there is no indication that rejection has taken place.
 *
 * Please see the documentation for
 * {@link #submitOnQueryComplete(QueryEventHook, QueryCompleteContext)}
 * regarding futures for more information/caveats on using the task future
 * to extract information.
 *
 *
 * ### Timeouts/Cancellation
 *
 * {@link FixedCapacityQueryHookExecutor} is configured with a hookTimeout
 * at construction-time.  The purpose of this timeout is to allow the executor
 * to cancel tasks that do not complete execution within the timeout and thus
 * keep the queue from filling up.
 *
 * The timeout value is measured from the start of task execution, so time spent
 * queued waiting to be executed does not count against this.
 *
 *
 * ### Metrics
 *
 * Performance metrics are collected in the {@link Metrics} object supplied
 * at construction-time.  The following metrics are collected:
 *
 * - `query-event-hook.${hookClass}.${method}.num-execution-rejections`
 *
 *     {@link Counter} indicating how many submitted tasks have been rejected
 *     due to a full work queue
 *
 * - `query-event-hook.${hookClass}.${method}.num-execution-exceptions`
 *
 *     {@link Counter} indicating how many tasks have thrown an exception
 *     during execution
 *
 * - `query-event-hook.${hookClass}.${method}.num-execution-timeouts`
 *
 *     {@link Counter} indicating how many tasks have been cancelled due to
 *     not completing within `hookTimeout_s` of submission.
 *
 * - `query-event-hook.${hookClass}.${method}.num-execution-submissions`
 *
 *     {@link Counter} indicating the number of times ${hookClass}.${method}
 *     has been submitted for execution.
 *
 * - `query-event-hook.${hookClass}.${method}.execution-time`
 *
 *     {@link Timer} indicating the amount of time ${hookClass}.${method}
 *     has taken to complete, whether normally or by error (e.g. timeout or exception).
 *     Because tasks can potentially be cancelled before beginning execution, the
 *     {@link Timer#getCount()} of this timer may be less than
 *     ${hookClass}.${method}.submissions
 *
 * - `query-event-hook.${hookClass}.${method}.queued-time`
 *
 *     {@link Timer} indicating the amount of time between hook task submission
 *     and hook task execution. Since a task may be cancelled before it even begins
 *     execution, {@link Timer#getCount()} of this timer may be less than
 *     ${hookClass}.${method}.submissions
 */
class FixedCapacityQueryHookExecutor extends ThreadPoolExecutor {
  private static final Logger LOG =
      LoggerFactory.getLogger(FixedCapacityQueryHookExecutor.class);

  private final long hookTimeout_;
  private final TimeUnit hookTimeoutUnit_;

  // use 2 executors, one for executing hook tasks and the other
  // for cancelling them if they have exceeded the configured timeout
  private final ScheduledThreadPoolExecutor timeoutMonitor_;
  private final ConcurrentMap<Runnable, ScheduledFuture> runningHooks = new ConcurrentHashMap<>();
  private final int queueCapacity_;
  private final Metrics metrics_;

  /**
   * Constructs a query hook executor with a fixed thread pool size,
   * fixed task queue capacity, and maximum timeout value for {@link QueryEventHook}
   * execution.  The task queue is used to hold tasks that are awaiting execution
   * because all threads are currently busy.
   *
   * @param nThreads size of thread pool to use for hook execution
   * @param hookTimeout hook timeout quantity
   * @param hookTimeoutUnit hook timeout unit
   * @param queueCapacity capacity of queue to hold hook tasks awaiting execution
   * @param useDaemonThreads set to true to use daemon threads for hook execution
   *                         (see {@link Thread#setDaemon(boolean)}
   *
   * @throws IllegalArgumentException if nThreads, hookTimeout_s, or queueCapacity < 1
   */
  FixedCapacityQueryHookExecutor(
      int nThreads,
      int hookTimeout,
      TimeUnit hookTimeoutUnit,
      int queueCapacity,
      boolean useDaemonThreads,
      Metrics hookMetrics) {

    // ArrayBlockingQueue constructor performs bounds-check on queueCapacity for us.
    // super constructor performs bounds-check on nThreads for us.
    super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(queueCapacity),
            new ThreadFactoryBuilder()
                    .setNameFormat("QueryEventHookExecutor-%d")
                    .setDaemon(useDaemonThreads)
                    .build()
    );

    this.metrics_ = Objects.requireNonNull(hookMetrics);

    Preconditions.checkArgument(hookTimeout >= 1,
        "hook timeout should be >= 1 but was " + hookTimeout);

    this.hookTimeout_ = hookTimeout;

    this.hookTimeoutUnit_ = Objects.requireNonNull(hookTimeoutUnit,
        "hookTimeoutUnit cannot be null");

    this.queueCapacity_ = queueCapacity;

    // This executor cancels any hook tasks that
    // have not completed within the configured timeout.
    this.timeoutMonitor_ =
        new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder()
                .setNameFormat("QueryEventHookMonitor-%d")
                .setDaemon(useDaemonThreads)
                .build()
            );

    this.timeoutMonitor_.setRemoveOnCancelPolicy(true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void shutdown() {
    timeoutMonitor_.shutdown();
    super.shutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Runnable> shutdownNow() {
    timeoutMonitor_.shutdownNow();
    return super.shutdownNow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (hookTimeout_ > 0) {
      final Runnable cancelTask = r instanceof OnQueryCompleteHookTask ?
              () -> ((OnQueryCompleteHookTask)r).cancel(t)
              :
              () -> t.interrupt();

      final ScheduledFuture<?> scheduled = timeoutMonitor_.schedule(cancelTask, hookTimeout_, hookTimeoutUnit_);
      runningHooks.put(r, scheduled);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    ScheduledFuture timeoutTask = runningHooks.remove(r);
    if (timeoutTask != null) {
      // don't interrupt the task that is cancelling the hook!
      timeoutTask.cancel(false);
    }
  }

  /**
   * Submits a future task to run
   * {@link QueryEventHook#onQueryComplete(QueryCompleteContext)} on the given hook
   * with the given context.  Performance metrics are captured and logged in the
   * {@link Metrics} object of `this` instance.
   *
   * ### Returned Future
   *
   * This method returns a {@link Future} for testing purposes.  This
   * is done from a package-private API because the future does not behave as
   * expected, due to the current implementation of using 2 executors.
   *
   * For example, cancelling the future would not actually cancel the
   * onQueryComplete task that was submitted.
   *
   * ### Task Cancellation
   *
   * If a submitted task does not complete execution within the configured hook
   * timeout of {@code this} instance, then the task will be cancelled.  When this
   * happens, the returned future will throw {@link ExecutionException} with the
   * cause as {@link TimeoutException}.  Performance metrics are still captured.
   *
   * ** The timeout period starts when the task is submitted, not when it begins
   * execution. **
   *
   * ### Error-Handling
   *
   * Performance metrics are still captured when the hook method exceptions.
   *
   * @param hook
   * @param context
   */
  Future<QueryEventHook> submitOnQueryComplete(
      QueryEventHook hook, QueryCompleteContext context) {
    Objects.requireNonNull(hook);
    Objects.requireNonNull(context);

    final String method = ON_QUERY_COMPLETE;

    getSubmissionCounter(hook, method).inc();

    final long submissionTime = System.nanoTime();

    final Future<QueryEventHook> f;
    try {
      f = submit(new OnQueryCompleteHookTask(hook, submissionTime, context));
    } catch (RejectedExecutionException e) {
      // We catch the exception instead of using a custom RejectedExecutionHandler
      // in the executor because this way we have access to the QueryCompleteContext
      // for logging.
      LOG.warn("QueryEventHook {}.{} execution rejected because the " +
              "task queue for this hook is full (at size={}).  The rejected " +
              "QueryCompleteContext was {}.  Executor activeCount currently at {}",
          hook.getClass().getName(), method,
          queueCapacity_,
          context,
          getActiveCount());

      getRejectionCounter(hook, method).inc();

      return failedFuture(e);
    }

    return f;
  }

  // Returns a future that is already-completed (failed) with the
  // given Throwable
  //
  // This method can be replaced with Java 9's CompletableFuture.failedFuture.
  private static <T> CompletableFuture<T> failedFuture(Throwable e) {
    CompletableFuture<T> f = new CompletableFuture<>();
    f.completeExceptionally(e);
    return f;
  }

  /**
   * Convenience method to prepend "query-event-hook" to a metric name.
   * This exists just to make it easier to change the naming scheme if
   * desired.
   */
  private static String mName(String... names) {
    return name("query-event-hook", names);
  }

  @VisibleForTesting
  Counter getExceptionCounter(QueryEventHook hook, String method) {
    return metrics_.getCounter(
        mName(hook.getClass().getName(), method, "num-execution-exceptions"));
  }

  @VisibleForTesting
  Counter getRejectionCounter(QueryEventHook hook, String method) {
    return metrics_.getCounter(
        mName(hook.getClass().getName(), method, "num-execution-rejections"));
  }

  @VisibleForTesting
  Counter getTimeoutCounter(QueryEventHook hook, String method) {
    return metrics_.getCounter(
        mName(hook.getClass().getName(), method, "num-execution-timeouts"));
  }

  @VisibleForTesting
  Counter getSubmissionCounter(QueryEventHook hook, String method) {
    return metrics_.getCounter(
        mName(hook.getClass().getName(), method, "num-execution-submissions"));
  }

  @VisibleForTesting
  Timer getExecutionTimer(QueryEventHook hook, String method) {
    return metrics_.getTimer(
        mName(hook.getClass().getName(), method, "execution-time"));
  }

  @VisibleForTesting
  Timer getQueuedTimer(QueryEventHook hook, String method) {
    return metrics_.getTimer(
        mName(hook.getClass().getName(), method, "queued-time"));
  }

  // Internal class that encapsulates a QueryEventHook.onQueryComplete task along
  // with the means to cancel it.
  //
  // This class basically exists so that, at submission-time, we can bundle together
  // information needed to collect/log metrics about hook execution and cancellation.
  private class OnQueryCompleteHookTask implements Callable<QueryEventHook> {
    private static final String HOOK_METHOD = ON_QUERY_COMPLETE;
    private final QueryEventHook hook;
    private final long submissionTime;
    private final QueryCompleteContext context;

    private OnQueryCompleteHookTask(QueryEventHook hook, long submissionTime, QueryCompleteContext context) {
      this.hook = hook;
      this.submissionTime = submissionTime;
      this.context = context;
    }

    // Executes the submitted hook task that this
    // object represents
    @Override
    public QueryEventHook call() throws Exception {
      final Timer execTimer = getExecutionTimer(hook, HOOK_METHOD);
      final Timer queuedTimer = getQueuedTimer(hook, HOOK_METHOD);
      queuedTimer.update(System.nanoTime() - submissionTime, TimeUnit.NANOSECONDS);
      return execTimer.time(() -> {
        try {
          hook.onQueryComplete(context);
          return hook;
        }
        catch (Throwable t) {
          getExceptionCounter(hook, HOOK_METHOD).inc();

          LOG.error("Exception thrown by QueryEventHook {}.{} method.  " +
                          "Hook instance {}. This exception is " +
                          "currently being ignored by Impala, " +
                          "but may cause subsequent problems in that hook's execution",
                  hook.getClass().getName(), HOOK_METHOD,
                  hook,
                  t);

          throw t;
        }
      });
    }

    // Cancels the running hook task that this
    // object represents.  This should really only be called from
    // the timeoutMonitor_ executor
    private void cancel(Thread t) {
      // cancel the hook task and log metrics/warning
      t.interrupt();

      getTimeoutCounter(hook, HOOK_METHOD).inc();

      LOG.warn("{}.{} task has not completed" +
                      "within {} {} of task submission and will be " +
                      "cancelled, whether or not it has begun execution.  You can check the " +
                      "configured timeout in your {} configuration property. " +
                      "The hook context being consumed was: {}",
              hook.getClass().getName(), HOOK_METHOD,
              hookTimeout_,
              hookTimeoutUnit_,
              BE_HOOKS_TIMEOUT_FLAG,
              context);
    }
  }

}
