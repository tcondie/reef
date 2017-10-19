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

package org.apache.reef.mock.runtime;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextMessage;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.parameters.*;
import org.apache.reef.driver.task.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.mock.MockClock;
import org.apache.reef.mock.MockRuntime;
import org.apache.reef.mock.MockTaskReturnValueProvider;
import org.apache.reef.mock.ProcessRequest;
import org.apache.reef.mock.request.*;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.util.*;

/**
 * mock runtime driver.
 */
public final class MockRuntimeDriver implements MockRuntime {

  private final InjectionFuture<MockClock> clock;

  private final List<ProcessRequest> processRequestQueue = new ArrayList<>();

  private final Set<EventHandler<StartTime>> driverStartHandlers;

  private final Set<EventHandler<StopTime>> driverStopHandlers;

  private final Set<EventHandler<AllocatedEvaluator>> allocatedEvaluatorHandlers;

  private final Set<EventHandler<CompletedEvaluator>> completedEvaluatorHandlers;

  private final Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers;

  private final Set<EventHandler<TaskRunningHandlers>> taskRunningHandlers;

  private final Set<EventHandler<FailedTask>> taskFailedHandlers;

  private final Set<EventHandler<TaskMessage>> taskMessageHandlers;

  private final Set<EventHandler<CompletedTask>> taskCompletedHandlers;

  private final Set<EventHandler<SuspendedTask>> taskSuspendedHandlers;

  private final Set<EventHandler<ActiveContext>> contextActiveHandlers;

  private final Set<EventHandler<CloseContext>> contextClosedHandlers;

  private final Set<EventHandler<ContextMessage>> contextMessageHandlers;

  private final Set<EventHandler<FailedContext>> contextFailedHandlers;

  private final Map<String, MockAllocatedEvalautor> allocatedEvaluatorMap = new HashMap<>();

  private final Map<String, List<MockActiveContext>> allocatedContextsMap = new HashMap<>();

  private final Map<String, MockRunningTask> runningTasks = new HashMap<>();

  private final MockTaskReturnValueProvider taskReturnValueProvider;

  @Inject
  MockRuntimeDriver(
      final InjectionFuture<MockClock> clock,
      final MockTaskReturnValueProvider taskReturnValueProvider,
      @Parameter(DriverStartHandler.class) final Set<EventHandler<StartTime>> driverStartHandlers,
      @Parameter(Clock.StopHandler.class) final Set<EventHandler<StopTime>> driverStopHandlers,
      @Parameter(EvaluatorAllocatedHandlers.class) final Set<EventHandler<AllocatedEvaluator>>
          allocatedEvaluatorHandlers,
      @Parameter(EvaluatorCompletedHandlers.class) final Set<EventHandler<CompletedEvaluator>>
          completedEvaluatorHandlers,
      @Parameter(EvaluatorFailedHandlers.class) final Set<EventHandler<FailedEvaluator>> failedEvaluatorHandlers,
      @Parameter(TaskRunningHandlers.class) final Set<EventHandler<TaskRunningHandlers>> taskRunningHandlers,
      @Parameter(TaskFailedHandlers.class) final Set<EventHandler<FailedTask>> taskFailedHandlers,
      @Parameter(TaskMessageHandlers.class) final Set<EventHandler<TaskMessage>> taskMessageHandlers,
      @Parameter(TaskCompletedHandlers.class) final Set<EventHandler<CompletedTask>> taskCompletedHandlers,
      @Parameter(TaskSuspendedHandlers.class) final Set<EventHandler<SuspendedTask>> taskSuspendedHandlers,
      @Parameter(ContextActiveHandlers.class) final Set<EventHandler<ActiveContext>> contextActiveHandlers,
      @Parameter(ContextClosedHandlers.class) final Set<EventHandler<CloseContext>> contextClosedHandlers,
      @Parameter(ContextMessageHandlers.class) final Set<EventHandler<ContextMessage>> contextMessageHandlers,
      @Parameter(ContextFailedHandlers.class) final Set<EventHandler<FailedContext>> contextFailedHandlers) {
    this.clock = clock;
    this.taskReturnValueProvider = taskReturnValueProvider;
    this.driverStartHandlers = driverStartHandlers;
    this.driverStopHandlers = driverStopHandlers;
    this.allocatedEvaluatorHandlers = allocatedEvaluatorHandlers;
    this.completedEvaluatorHandlers = completedEvaluatorHandlers;
    this.failedEvaluatorHandlers = failedEvaluatorHandlers;
    this.taskRunningHandlers = taskRunningHandlers;
    this.taskFailedHandlers = taskFailedHandlers;
    this.taskMessageHandlers = taskMessageHandlers;
    this.taskCompletedHandlers = taskCompletedHandlers;
    this.taskSuspendedHandlers = taskSuspendedHandlers;
    this.contextActiveHandlers = contextActiveHandlers;
    this.contextClosedHandlers = contextClosedHandlers;
    this.contextMessageHandlers = contextMessageHandlers;
    this.contextFailedHandlers = contextFailedHandlers;
  }

  @Override
  public Collection<AllocatedEvaluator> getCurrentAllocatedEvaluators() {
    return new ArrayList<AllocatedEvaluator>(this.allocatedEvaluatorMap.values());
  }

  @Override
  public void fail(final AllocatedEvaluator evaluator) {
    if (this.allocatedEvaluatorMap.containsKey(evaluator.getId())) {
      FailedTask failedTask =  null;
      if (this.runningTasks.containsKey(evaluator.getId())) {
        final RunningTask task = this.runningTasks.remove(evaluator.getId());
        failedTask = new FailedTask(
            task.getId(),
            "mock",
            Optional.<String>empty(),
            Optional.<Throwable>empty(),
            Optional.<byte[]>empty(),
            Optional.<ActiveContext>of(task.getActiveContext()));
      }
      final List<FailedContext> failedContexts = new ArrayList<>();
      for (final MockActiveContext context : this.allocatedContextsMap.get(evaluator.getId())) {
        failedContexts.add(new MockFailedContext(context));
      }
      this.allocatedContextsMap.remove(evaluator.getId());

      post(this.failedEvaluatorHandlers, new MockFailedEvaluator(
          evaluator.getId(), failedContexts,
          failedTask == null ? Optional.<FailedTask>empty() : Optional.of(failedTask)));
    } else {
      throw new IllegalStateException("unknown evaluator " + evaluator);
    }
  }

  @Override
  public Collection<ActiveContext> getCurrentActiveContexts() {
    final List<ActiveContext> currentActiveContexts = new ArrayList<>();
    for (final List<MockActiveContext> contexts : this.allocatedContextsMap.values()) {
      currentActiveContexts.addAll(contexts);
    }
    return currentActiveContexts;
  }

  @Override
  public void fail(final ActiveContext context) {
    final MockAllocatedEvalautor evaluator = ((MockActiveContext) context).getEvaluator();
    post(this.contextFailedHandlers, new MockFailedContext((MockActiveContext) context));
    if (!((MockActiveContext) context).getParentContext().isPresent()) {
      // root context failure shuts evalautor down
      fail(evaluator);
    } else {
      this.allocatedContextsMap.get(evaluator.getId()).remove(context);
    }
  }

  @Override
  public Collection<RunningTask> getCurrentRunningTasks() {
    return new ArrayList<RunningTask>(this.runningTasks.values());
  }

  @Override
  public void fail(final RunningTask task) {
    final String evaluatorID = task.getActiveContext().getEvaluatorId();
    if (this.runningTasks.containsKey(evaluatorID) &&
        this.runningTasks.get(evaluatorID).equals(task)) {
      this.runningTasks.remove(evaluatorID);
      post(taskFailedHandlers, new FailedTask(
          task.getId(),
          "mock",
          Optional.<String>empty(),
          Optional.<Throwable>empty(),
          Optional.<byte[]>empty(),
          Optional.of(task.getActiveContext())));
    } else {
      throw new IllegalStateException("unknown running task " + task);
    }
  }

  @Override
  public void start() {
    post(this.driverStartHandlers, new StartTime(this.clock.get().getCurrentTime()));
  }

  @Override
  public void stop() {
    post(this.driverStopHandlers, new StopTime(this.clock.get().getCurrentTime()));
  }

  @Override
  public boolean hasProcessRequest() {
    return this.processRequestQueue.size() > 0;
  }

  @Override
  public ProcessRequest getNextProcessRequest() {
    if (this.processRequestQueue.size() > 0) {
      return this.processRequestQueue.remove(0);
    } else {
      return null;
    }
  }

  @Override
  public void succeed(final ProcessRequest pr) {
    final ProcessRequestInternal request = (ProcessRequestInternal) pr;
    switch (request.getType()) {
    case ALLOCATE_EVALUATOR:
      final MockAllocatedEvalautor allocatedEvalautor = ((AllocateEvaluator)request).getSuccessEvent();
      validateAndCreate(allocatedEvalautor);
      post(this.allocatedEvaluatorHandlers, allocatedEvalautor);
      post(this.contextActiveHandlers, allocatedEvalautor.getRootContext());
      break;
    case CLOSE_EVALUATOR:
      final CompletedEvaluator closedEvaluator = ((CloseEvaluator)request).getSuccessEvent();
      validateAndClose(closedEvaluator);
      post(this.completedEvaluatorHandlers, closedEvaluator);
      break;
    case CREATE_CONTEXT:
      final MockActiveContext createContext = ((CreateContext) request).getSuccessEvent();
      validateAndCreate(createContext);
      post(this.contextActiveHandlers, createContext);
      break;
    case CLOSE_CONTEXT:
      final MockClosedContext closeContext = ((CloseContext) request).getSuccessEvent();
      validateAndClose(closeContext);
      post(this.contextClosedHandlers, closeContext);
      break;
    case CREATE_TASK:
      final MockRunningTask createTask = ((CreateTask)request).getSuccessEvent();
      validateAndCreate(createTask);
      post(this.taskRunningHandlers, request.getSuccessEvent());
      break;
    case SUSPEND_TASK:
      final MockRunningTask suspendedTask = ((SuspendTask)request).getTask();
      validateAndClose(suspendedTask);
      post(this.taskSuspendedHandlers, request.getSuccessEvent());
      break;
    case CLOSE_TASK:
    case COMPLETE_TASK:
      final MockRunningTask completedTask = ((CompleteTask)request).getTask();
      validateAndClose(completedTask);
      post(this.taskCompletedHandlers, request.getSuccessEvent());
      break;
    case CREATE_CONTEXT_AND_TASK:
      final CreateContextAndTask createContextTask = (CreateContextAndTask) request;
      final Tuple<MockActiveContext, MockRunningTask> events = createContextTask.getSuccessEvent();
      validateAndCreate(events.getKey());
      post(this.contextActiveHandlers, events.getKey());
      validateAndCreate(events.getValue());
      post(this.taskRunningHandlers, events.getValue());
      break;
    case SEND_MESSAGE_DRIVER_TO_TASK:
      // ignore
      break;
    case SEND_MESSAGE_DRIVER_TO_CONTEXT:
      // ignore
      break;
    default:
      throw new IllegalStateException("unknown type");
    }

    if (request.doAutoComplete()) {
      add(request.getCompletionProcessRequest());
    }
  }

  @Override
  public void fail(final ProcessRequest pr) {
    final ProcessRequestInternal request = (ProcessRequestInternal) pr;
    switch (request.getType()) {
    case ALLOCATE_EVALUATOR:
      post(this.failedEvaluatorHandlers, request.getFailureEvent());
      break;
    case CLOSE_EVALUATOR:
      final CompletedEvaluator evaluator = ((CloseEvaluator)request).getSuccessEvent();
      validateAndClose(evaluator);
      post(this.failedEvaluatorHandlers, request.getFailureEvent());
      break;
    case CREATE_CONTEXT:
      post(this.contextFailedHandlers, request.getFailureEvent());
      break;
    case CLOSE_CONTEXT:
      final MockClosedContext context = ((CloseContext)request).getSuccessEvent();
      validateAndClose(context);
      if (context.getParentContext() == null) {
        add(new CloseEvaluator(context.getMockActiveContext().getEvaluator()));
      }
      post(this.contextFailedHandlers, request.getFailureEvent());
      break;
    case CREATE_TASK:
      post(this.taskFailedHandlers, request.getFailureEvent());
      break;
    case SUSPEND_TASK:
      validateAndClose(((SuspendTask)request).getTask());
      post(this.taskFailedHandlers, request.getFailureEvent());
      break;
    case CLOSE_TASK:
    case COMPLETE_TASK:
      validateAndClose(((CloseTask)request).getTask());
      post(this.taskFailedHandlers, request.getFailureEvent());
      break;
    case CREATE_CONTEXT_AND_TASK:
      final CreateContextAndTask createContextTask = (CreateContextAndTask) request;
      final Tuple<MockFailedContext, FailedTask> events = createContextTask.getFailureEvent();
      post(this.taskFailedHandlers, events.getValue());
      post(this.contextFailedHandlers, events.getKey());
      break;
    case SEND_MESSAGE_DRIVER_TO_TASK:
      // ignore
      break;
    case SEND_MESSAGE_DRIVER_TO_CONTEXT:
      // ignore
      break;
    default:
      throw new IllegalStateException("unknown type");
    }
  }

  MockTaskReturnValueProvider getTaskReturnValueProvider() {
    return this.taskReturnValueProvider;
  }
  /**
   * Used by mock REEF entities (e.g., AllocatedEvaluator, RunningTask) to inject
   * process requests from initiated actions e.g., RunningTask.close().
   * @param request to inject
   */
  void add(final ProcessRequest request) {
    this.processRequestQueue.add(request);
  }

  private <T> void post(final Set<EventHandler<T>> handlers, final Object event) {
    for (final EventHandler<T> handler : handlers) {
      handler.onNext((T) event);
    }
  }

  private void validateAndCreate(final MockActiveContext context) {
    if (!this.allocatedEvaluatorMap.containsKey(context.getEvaluatorId())) {
      throw new IllegalStateException("unknown evaluator id " + context.getEvaluatorId());
    } else if (!this.allocatedContextsMap.containsKey(context.getEvaluatorId())) {
      this.allocatedContextsMap.put(context.getEvaluatorId(), new ArrayList<MockActiveContext>());
    }
    this.allocatedContextsMap.get(context.getEvaluatorId()).add(context);
  }

  private void validateAndClose(final MockClosedContext context) {
    if (!this.allocatedContextsMap.containsKey(context.getEvaluatorId())) {
      throw new IllegalStateException("unknown evaluator id " + context.getEvaluatorId());
    }
    final List<MockActiveContext> contexts = this.allocatedContextsMap.get(context.getEvaluatorId());
    if (!contexts.get(contexts.size() - 1).equals(context.getMockActiveContext())) {
      throw new IllegalStateException("closing context that is not on the top of the stack");
    }
    contexts.remove(context.getMockActiveContext());
  }

  private void validateAndCreate(final MockRunningTask task) {
    if (this.runningTasks.containsKey(task.evaluatorID())) {
      throw new IllegalStateException("task already running on evaluator " +
          task.evaluatorID());
    }
    this.runningTasks.put(task.evaluatorID(), task);
  }

  private void validateAndClose(final MockRunningTask task) {
    if (!this.runningTasks.containsKey(task.getActiveContext().getEvaluatorId())) {
      throw new IllegalStateException("no task running on evaluator");
    }
    this.runningTasks.remove(task.getActiveContext().getEvaluatorId());
  }

  private void validateAndCreate(final MockAllocatedEvalautor evalutor) {
    if (this.allocatedEvaluatorMap.containsKey(evalutor.getId())) {
      throw new IllegalStateException("evaluator id " + evalutor.getId() + " already exists");
    }
    this.allocatedEvaluatorMap.put(evalutor.getId(), evalutor);
    this.allocatedContextsMap.put(evalutor.getId(), new ArrayList<MockActiveContext>());
    this.allocatedContextsMap.get(evalutor.getId()).add(evalutor.getRootContext());
  }

  private void validateAndClose(final CompletedEvaluator evalautor) {
    if (!this.allocatedEvaluatorMap.containsKey(evalautor.getId())) {
      throw new IllegalStateException("unknown evaluator id " + evalautor.getId());
    }
    this.allocatedEvaluatorMap.remove(evalautor.getId());
  }
}
