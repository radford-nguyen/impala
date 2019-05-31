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

import org.apache.impala.common.Metrics;
import org.apache.impala.testutil.AlwaysErrorQueryEventHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test suite for {@link FixedCapacityQueryHookExecutor}
 */
public class FixedCapacityQueryHookExecutorTest {

  private static final QueryCompleteContext STUB_COMPLETE_CTX =
      new QueryCompleteContext("whatever");

  private final Metrics metrics_ = new Metrics();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testQueryCompleteTimeoutCancelsTask() throws Exception {

    final int nThreads = 1;
    final boolean daemon= true;
    final int hookTimeout = 20;

    final AtomicBoolean hookCompleted = new AtomicBoolean(false);
    final AtomicBoolean hookInterrupted = new AtomicBoolean(false);

    final QueryEventHook sleepingHook = new QueryEventHook() {
      @Override
      public void onImpalaStartup() {}
      @Override
      public void onQueryComplete(QueryCompleteContext context) {
        try {
          // Sleep much longer than `hookTimeout` to reasonably guarantee a timeout.
          TimeUnit.SECONDS.sleep(2);
          // Shouldn't reach here because of timeout.
          hookCompleted.set(true);
        } catch (InterruptedException e) {
          hookInterrupted.set(true);
        }
      }
    };

    final FixedCapacityQueryHookExecutor SUT =
        new FixedCapacityQueryHookExecutor(nThreads, hookTimeout, TimeUnit.MILLISECONDS,
            5, daemon, metrics_);

    Future<QueryEventHook> f = SUT.submitOnQueryComplete(sleepingHook, STUB_COMPLETE_CTX);

    assertOnQueryCompleteTimedOut(f, 1, TimeUnit.SECONDS);

    assertFalse(hookCompleted.get());
    assertTrue(hookInterrupted.get());
  }

  // Encapsulates logic that determines if a hook task
  // from submitOnQueryComplete() timed out.
  //
  // Kind of weird logic due to the implementation, so
  // it is wrapped in this method.
  private void assertOnQueryCompleteTimedOut(Future<QueryEventHook> hookFuture,
                                             long timeout,
                                             TimeUnit timeUnit) throws Exception {
    try {
      hookFuture.get(timeout, timeUnit);
      fail("ExecutionException expected but not thrown");
    }
    catch (ExecutionException expected) {
      if (!(expected.getCause() instanceof TimeoutException)) {
        fail("TimeoutException expected but not thrown");
      }
    }
  }

  @Test
  public void testFastHookCompletionDoesntTimeout() throws Exception {
    final int nThreads = 1;
    final boolean daemon = true;
    final int hookTimeout_s = 1;

    final QueryEventHook hook = new QueryEventHook() {
      @Override
      public void onImpalaStartup() {
      }

      @Override
      public void onQueryComplete(QueryCompleteContext context) {
        // NO-OP hook for fastest execution
      }
    };

    final FixedCapacityQueryHookExecutor SUT =
        new FixedCapacityQueryHookExecutor(nThreads, hookTimeout_s, TimeUnit.SECONDS,
            5, daemon, metrics_);

    for (int i = 0; i < 1000; i++) {
      final Future<QueryEventHook> f = SUT.submitOnQueryComplete(hook, STUB_COMPLETE_CTX);
      final QueryEventHook futureResponse = f.get(3, TimeUnit.MINUTES);
      assertSame(hook, futureResponse);
    }
  }

  @Test
  public void testHookQueueFullWillRejectSubmission() throws Throwable {
    final int nThreads = 1;
    final boolean daemon= true;
    final int queueSize = 3;
    final int timeout_s = 3;

    final QueryEventHook hook = new QueryEventHook() {
      @Override
      public void onImpalaStartup() {}
      @Override
      public void onQueryComplete(QueryCompleteContext context) {
        try {
          // Block "indefinitely" so that tasks fill up the queue.
          TimeUnit.MINUTES.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };

    final FixedCapacityQueryHookExecutor SUT =
        new FixedCapacityQueryHookExecutor(nThreads, timeout_s, TimeUnit.SECONDS,
            queueSize, daemon, metrics_);

    final List<Future<QueryEventHook>> fs = new ArrayList<>(queueSize + 2);

    // Submit queueSize+1 times because the first submission will always execute
    // immediately (and therefore not go into the queue).
    for (int i=0; i<queueSize+2; i++) {
      fs.add(SUT.submitOnQueryComplete(hook, STUB_COMPLETE_CTX));
    }

    // The first tasks should time out since we sleep "indefinitely"
    // but they were still accepted for execution.
    for (int i=0; i<queueSize+1; i++) {
      final Future<QueryEventHook> f = fs.get(i);
      try {
        f.get(1, TimeUnit.MILLISECONDS);
        fail("TimeoutException expected but not thrown");
      } catch (TimeoutException expected) {}
    }

    // The last task should immediately fail because it was
    // rejected or submission.
    expectedException.expect(RejectedExecutionException.class);
    final Future<QueryEventHook> f = fs.get(queueSize + 1);
    try {
      f.get(1, TimeUnit.MILLISECONDS);
    } catch (ExecutionException ee) {
      // Unwrap the executor exception and throw it.
      throw ee.getCause();
    }
  }

  @Test
  public void testExceptionStillCapturesMetrics() throws Exception {
    final int nThreads = 1;
    final boolean daemon= true;
    final int queueSize = 3;
    final int timeout_s = 3;
    final QueryEventHook hook = new AlwaysErrorQueryEventHook();
    final FixedCapacityQueryHookExecutor SUT =
        new FixedCapacityQueryHookExecutor(nThreads, timeout_s, TimeUnit.SECONDS,
            queueSize, daemon, metrics_);

    final int exceptionCount = 23;
    for (int i=0; i<exceptionCount; i++) {
      final Future<QueryEventHook> f = SUT.submitOnQueryComplete(hook, STUB_COMPLETE_CTX);

      try {
        f.get(3, TimeUnit.SECONDS);
        fail("ExecutionException expected but not thrown");
      } catch (ExecutionException expected) {}

      assertEquals(i+1,
          SUT.getExceptionCounter(hook, "onQueryComplete").getCount());
    }

    assertEquals(exceptionCount,
        SUT.getExecutionTimer(hook, "onQueryComplete").getCount());
  }

  @Test
  public void testTimeoutStillCapturesMetrics() throws Exception {
    final int nThreads = 1;
    final boolean daemon= true;
    final int queueSize = 3;
    final int timeout = 5;

    final QueryEventHook hook = new QueryEventHook() {
      @Override
      public void onImpalaStartup() {}

      @Override
      public void onQueryComplete(QueryCompleteContext context) {
        try {
          // Ensure a timeout by sleeping much longer than `timeout`
          TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {}
      }
    };

    final FixedCapacityQueryHookExecutor SUT =
        new FixedCapacityQueryHookExecutor(nThreads, timeout, TimeUnit.MILLISECONDS,
            queueSize, daemon, metrics_);

    for (int i=0; i<25; i++) {
      assertOnQueryCompleteTimedOut(
          SUT.submitOnQueryComplete(hook, STUB_COMPLETE_CTX),
          3, TimeUnit.SECONDS);

      assertEquals(i+1,
          SUT.getTimeoutCounter(hook, "onQueryComplete").getCount());

      assertEquals(i+1,
          SUT.getExecutionTimer(hook, "onQueryComplete").getCount());
    }
  }

}