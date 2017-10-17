/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.mock.request.ProcessRequestInternal;
import org.apache.reef.mock.runtime.MockAllocatedEvalautor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * basic mock tests.
 */
public final class BasicMockTests {

  private MockApplication mockApplication;

  private MockRuntime mockRuntime;

  private MockClock mockClock;

  @Before
  public void initialize() throws Exception {
    final Configuration conf = MockConfiguration.CONF
        .set(MockConfiguration.ON_DRIVER_STARTED, MockApplication.StartHandler.class)
        .set(MockConfiguration.ON_DRIVER_STOP, MockApplication.StopHandler.class)
        .set(MockConfiguration.ON_CONTEXT_ACTIVE, MockApplication.ActiveContextHandler.class)
        .set(MockConfiguration.ON_CONTEXT_CLOSED, MockApplication.ContextClosedHandler.class)
        .set(MockConfiguration.ON_CONTEXT_FAILED, MockApplication.FailedContextHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_ALLOCATED, MockApplication.AllocatedEvaluatorHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_COMPLETED, MockApplication.CompletedEvaluatorHandler.class)
        .set(MockConfiguration.ON_EVALUATOR_FAILED, MockApplication.FailedEvaluatorHandler.class)
        .set(MockConfiguration.ON_TASK_COMPLETED, MockApplication.CompletedTaskHandler.class)
        .set(MockConfiguration.ON_TASK_FAILED, MockApplication.FailedTaskHandler.class)
        .set(MockConfiguration.ON_TASK_RUNNING, MockApplication.RunningTaskHandler.class)
        .set(MockConfiguration.ON_TASK_SUSPENDED, MockApplication.SuspendedTaskHandler.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    this.mockApplication = injector.getInstance(MockApplication.class);
    this.mockRuntime = injector.getInstance(MockRuntime.class);
    this.mockClock = injector.getInstance(MockClock.class);

    this.mockClock.run();
  }

  @Test
  public void testSuccessRequests() throws Exception {
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    this.mockApplication.requestEvaluators(1);
    assertTrue("check for process event", this.mockRuntime.hasProcessRequest());
    final ProcessRequest allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    assertTrue("allocate evalautor request",
        allocateEvaluatorRequest.getType() == ProcessRequest.Type.ALLOCATE_EVALUATOR);
    final AllocatedEvaluator evaluator =
        ((ProcessRequestInternal<AllocatedEvaluator, Object>)allocateEvaluatorRequest)
            .getSuccessEvent();
    this.mockRuntime.succeed(allocateEvaluatorRequest);
    assertTrue("evaluator allocation succeeded",
        this.mockApplication.getAllocatedEvaluators().contains(evaluator));
    final ActiveContext rootContext = this.mockApplication.getContext(evaluator,
        MockAllocatedEvalautor.ROOT_CONTEXT_IDENTIFIER_PREFIX + evaluator.getId());
    assertTrue("root context", rootContext != null);


    // submit a task
    this.mockApplication.submitTask(rootContext, "test-task");
    assertTrue("create task queued", this.mockRuntime.hasProcessRequest());
    final ProcessRequest createTaskRequest = this.mockRuntime.getNextProcessRequest();
    assertTrue("create task request",
        createTaskRequest.getType() == ProcessRequest.Type.CREATE_TASK);
    final RunningTask task = (RunningTask) ((ProcessRequestInternal)createTaskRequest).getSuccessEvent();
    this.mockRuntime.succeed(createTaskRequest);
    assertTrue("task running", this.mockApplication.getRunningTasks().contains(task));

    // check task auto complete
    assertTrue("check for request", this.mockRuntime.hasProcessRequest());
    final ProcessRequestInternal completedTask =
        (ProcessRequestInternal) this.mockRuntime.getNextProcessRequest();
    assertTrue("complete task request",
        completedTask.getType() == ProcessRequest.Type.COMPLETE_TASK);
    this.mockRuntime.succeed(completedTask);
    assertTrue("no running tasks", this.mockApplication.getRunningTasks().size() == 0);

    // create a sub-context
    this.mockApplication.submitContext(rootContext, "child");
    assertTrue("check for request", this.mockRuntime.hasProcessRequest());
    final ProcessRequestInternal createContextRequest =
        (ProcessRequestInternal) this.mockRuntime.getNextProcessRequest();
    assertTrue("create context request",
        createContextRequest.getType() == ProcessRequest.Type.CREATE_CONTEXT);
    this.mockRuntime.succeed(createContextRequest);
    final ActiveContext context = this.mockApplication.getContext(evaluator, "child");
    assertTrue("child context", context.getParentId().get().equals(rootContext.getId()));
  }

  @Test
  public void testFailureRequests() throws Exception {
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    this.mockApplication.requestEvaluators(1);
    assertTrue("check for process event", this.mockRuntime.hasProcessRequest());
    ProcessRequest allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    this.mockRuntime.fail(allocateEvaluatorRequest);
    assertTrue("evaluator allocation failed",
        this.mockApplication.getFailedEvaluators().size() == 1);

    this.mockApplication.requestEvaluators(1);
    allocateEvaluatorRequest = this.mockRuntime.getNextProcessRequest();
    final AllocatedEvaluator evaluator =
        (AllocatedEvaluator)((ProcessRequestInternal)allocateEvaluatorRequest).getSuccessEvent();
    this.mockRuntime.succeed(allocateEvaluatorRequest);
    final ActiveContext rootContext = this.mockApplication
        .getContext(evaluator, MockAllocatedEvalautor.ROOT_CONTEXT_IDENTIFIER_PREFIX + evaluator.getId());


    // submit a task
    this.mockApplication.submitTask(rootContext, "test-task");
    assertTrue("create task queued", this.mockRuntime.hasProcessRequest());
    final ProcessRequest createTaskRequest = this.mockRuntime.getNextProcessRequest();
    assertTrue("create task request",
        createTaskRequest.getType() == ProcessRequest.Type.CREATE_TASK);
    this.mockRuntime.fail(createTaskRequest);
    assertTrue("task running", this.mockApplication.getFailedTasks().size() == 1);

    // create a sub-context
    this.mockApplication.submitContext(rootContext, "child");
    assertTrue("check for request", this.mockRuntime.hasProcessRequest());
    final ProcessRequestInternal createContextRequest =
        (ProcessRequestInternal) this.mockRuntime.getNextProcessRequest();
    this.mockRuntime.fail(createContextRequest);
    assertTrue("child context", this.mockApplication.getFailedContext().size() == 1);
  }

  @Test
  public void testMockFailures() {
    // make sure we're running
    assertTrue("mock application received start event", this.mockApplication.isRunning());

    // allocate an evaluator and get root context
    this.mockApplication.requestEvaluators(1);
    this.mockRuntime.succeed(this.mockRuntime.getNextProcessRequest());
    final AllocatedEvaluator evaluator = this.mockRuntime.getCurrentAllocatedEvaluators().iterator().next();
    final ActiveContext rootContext = this.mockApplication.getContext(evaluator,
        MockAllocatedEvalautor.ROOT_CONTEXT_IDENTIFIER_PREFIX + evaluator.getId());

    // create a child context off of root context
    this.mockApplication.submitContext(rootContext, "child");
    this.mockRuntime.succeed(this.mockRuntime.getNextProcessRequest());
    final ActiveContext childContext = this.mockApplication.getContext(evaluator, "child");

    // submit a task from child context
    this.mockApplication.submitTask(childContext, "test-task");
    final ProcessRequest createTaskRequest = this.mockRuntime.getNextProcessRequest();
    createTaskRequest.setAutoComplete(false); // keep it running
    this.mockRuntime.succeed(createTaskRequest);
    final RunningTask task = this.mockRuntime.getCurrentRunningTasks().iterator().next();

    // fail task
    this.mockRuntime.fail(task);
    assertTrue("task failed", this.mockApplication.getFailedTasks().size() == 1);

    // fail child context
    this.mockRuntime.fail(childContext);
    assertTrue("child context failed",
        this.mockApplication.getFailedContext().iterator().next().getId().equals(childContext.getId()));
    // evaluator should still be up
    assertTrue("check evaluator", this.mockApplication.getFailedEvaluators().size() == 0);

    // fail evaluator
    this.mockRuntime.fail(evaluator);
    assertTrue("evaluator failed", this.mockApplication.getFailedEvaluators().size() == 1);

    // both contexts should be failed
    assertTrue("root and child contexts failed",
        this.mockApplication.getFailedContext().size() == 2);
  }
}
