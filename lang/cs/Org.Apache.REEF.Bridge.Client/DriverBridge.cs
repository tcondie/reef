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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.DotNet;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Client.DotNet
{
    internal sealed class DriverBridge
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(DriverBridge));

        // Control event dispatchers 

        private readonly DispatchEventHandler<IDriverStarted> _driverStartedDispatcher;

        private readonly DispatchEventHandler<IDriverStopped> _driverStoppedDispatcher;

        // Evaluator event dispatchers

        private readonly DispatchEventHandler<IAllocatedEvaluator> _allocatedEvaluatorDispatcher;

        private readonly DispatchEventHandler<IFailedEvaluator> _failedEvaluatorDispatcher;

        private readonly DispatchEventHandler<ICompletedEvaluator> _completedEvaluatorDispatcher;

        // Context event dispatchers

        private readonly DispatchEventHandler<IActiveContext> _activeContextDispatcher;

        private readonly DispatchEventHandler<IClosedContext> _closedContextDispatcher;

        private readonly DispatchEventHandler<IFailedContext> _failedContextDispatcher;

        private readonly DispatchEventHandler<IContextMessage> _contextMessageDispatcher;

        // Task event dispatchers

        private readonly DispatchEventHandler<ITaskMessage> _taskMessageDispatcher;

        private readonly DispatchEventHandler<IFailedTask> _failedTaskDispatcher;

        private readonly DispatchEventHandler<IRunningTask> _runningTaskDispatcher;

        private readonly DispatchEventHandler<ICompletedTask> _completedTaskDispatcher;

        // Driver restart event dispatchers

        private readonly DispatchEventHandler<IDriverRestarted> _driverRestartedDispatcher;

        private readonly DispatchEventHandler<IActiveContext> _driverRestartActiveContextDispatcher;

        private readonly DispatchEventHandler<IRunningTask> _driverRestartRunningTaskDispatcher;

        private readonly DispatchEventHandler<IDriverRestartCompleted> _driverRestartCompletedDispatcher;

        private readonly DispatchEventHandler<IFailedEvaluator> _driverRestartFailedEvaluatorDispatcher;

        private int _activeDispatchCounter = 0;

        public bool IsIdle => _activeDispatchCounter == 0;

        [Inject]
        DriverBridge(
            // Runtime events
            [Parameter(Value = typeof(Parameters.DriverStartedHandlers))]
            ISet<IObserver<IDriverStarted>> driverStartHandlers,
            [Parameter(Value = typeof(Parameters.DriverStopHandlers))]
            ISet<IObserver<IDriverStopped>> driverStopHandlers,
            // Evaluator events
            [Parameter(Value = typeof(Parameters.AllocatedEvaluatorHandlers))]
            ISet<IObserver<IAllocatedEvaluator>> allocatedEvaluatorHandlers,
            [Parameter(Value = typeof(Parameters.FailedEvaluatorHandlers))]
            ISet<IObserver<IFailedEvaluator>> failedEvaluatorHandlers,
            [Parameter(Value = typeof(Parameters.CompletedEvaluatorHandlers))]
            ISet<IObserver<ICompletedEvaluator>> completedEvaluatorHandlers,
            // Context events
            [Parameter(Value = typeof(Parameters.ActiveContextHandlers))]
            ISet<IObserver<IActiveContext>> activeContextHandlers,
            [Parameter(Value = typeof(Parameters.ClosedContextHandlers))]
            ISet<IObserver<IClosedContext>> closedContextHandlers,
            [Parameter(Value = typeof(Parameters.FailedContextHandlers))]
            ISet<IObserver<IFailedContext>> failedContextHandlers,
            [Parameter(Value = typeof(Parameters.ContextMessageHandlers))]
            ISet<IObserver<IContextMessage>> contextMessageHandlers,
            // Task events
            [Parameter(Value = typeof(Parameters.TaskMessageHandlers))]
            ISet<IObserver<ITaskMessage>> taskMessageHandlers,
            [Parameter(Value = typeof(Parameters.FailedTaskHandlers))]
            ISet<IObserver<IFailedTask>> failedTaskHandlers,
            [Parameter(Value = typeof(Parameters.RunningTaskHandlers))]
            ISet<IObserver<IRunningTask>> runningTaskHandlers,
            [Parameter(Value = typeof(Parameters.CompletedTaskHandlers))]
            ISet<IObserver<ICompletedTask>> completedTaskHandlers,
            // Driver restart events
            [Parameter(Value = typeof(Parameters.DriverRestartedHandlers))]
            ISet<IObserver<IDriverRestarted>> driverRestartedHandlers,
            [Parameter(Value = typeof(Parameters.DriverRestartActiveContextHandlers))]
            ISet<IObserver<IActiveContext>> driverRestartActiveContextHandlers,
            [Parameter(Value = typeof(Parameters.DriverRestartRunningTaskHandlers))]
            ISet<IObserver<IRunningTask>> driverRestartRunningTaskHandlers,
            [Parameter(Value = typeof(Parameters.DriverRestartCompletedHandlers))]
            ISet<IObserver<IDriverRestartCompleted>> driverRestartCompletedHandlers,
            [Parameter(Value = typeof(Parameters.DriverRestartFailedEvaluatorHandlers))]
            ISet<IObserver<IFailedEvaluator>> driverRestartFailedEvaluatorHandlers,
            // Misc.
            [Parameter(Value = typeof(Parameters.TraceListeners))]
            ISet<TraceListener> traceListeners,
            [Parameter(Value = typeof(EvaluatorConfigurationProviders))]
            ISet<IConfigurationProvider> configurationProviders,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.TraceLevel))]
            string traceLevel,
            IConfigurationSerializer configurationSerializer)
        {
            _driverStartedDispatcher = new DispatchEventHandler<IDriverStarted>(driverStartHandlers);
            _driverStoppedDispatcher = new DispatchEventHandler<IDriverStopped>(driverStopHandlers);
            _allocatedEvaluatorDispatcher = new DispatchEventHandler<IAllocatedEvaluator>(allocatedEvaluatorHandlers);
            _failedEvaluatorDispatcher = new DispatchEventHandler<IFailedEvaluator>(failedEvaluatorHandlers);
            _completedEvaluatorDispatcher = new DispatchEventHandler<ICompletedEvaluator>(completedEvaluatorHandlers);
            _activeContextDispatcher = new DispatchEventHandler<IActiveContext>(activeContextHandlers);
            _closedContextDispatcher = new DispatchEventHandler<IClosedContext>(closedContextHandlers);
            _failedContextDispatcher = new DispatchEventHandler<IFailedContext>(failedContextHandlers);
            _contextMessageDispatcher = new DispatchEventHandler<IContextMessage>(contextMessageHandlers);
            _taskMessageDispatcher = new DispatchEventHandler<ITaskMessage>(taskMessageHandlers);
            _failedTaskDispatcher = new DispatchEventHandler<IFailedTask>(failedTaskHandlers);
            _runningTaskDispatcher = new DispatchEventHandler<IRunningTask>(runningTaskHandlers);
            _completedTaskDispatcher = new DispatchEventHandler<ICompletedTask>(completedTaskHandlers);
            _driverRestartedDispatcher = new DispatchEventHandler<IDriverRestarted>(driverRestartedHandlers);
            _driverRestartActiveContextDispatcher = new DispatchEventHandler<IActiveContext>(driverRestartActiveContextHandlers);
            _driverRestartRunningTaskDispatcher = new DispatchEventHandler<IRunningTask>(driverRestartRunningTaskHandlers);
            _driverRestartCompletedDispatcher = new DispatchEventHandler<IDriverRestartCompleted>(driverRestartCompletedHandlers);
            _driverRestartFailedEvaluatorDispatcher = new DispatchEventHandler<IFailedEvaluator>(driverRestartFailedEvaluatorHandlers);

            foreach (TraceListener listener in traceListeners)
            {
                Logger.AddTraceListener(listener);
            }
            _logger.Log(Level.Info, "Constructing DriverBridge");

            Level level;
            if (!Enum.TryParse(traceLevel.ToString(CultureInfo.InvariantCulture), out level))
            {
                _logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, 
                    "Invalid trace level {0} provided, will by default use verbose level", traceLevel));
            }
            else
            {
                Logger.SetCustomLevel(level);
            }
        }

        public async Task DispatchDriverRestartFailedEvaluatorEvent(IFailedEvaluator failedEvaluatorEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverRestartFailedEvaluatorDispatcher.OnNext(failedEvaluatorEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchDriverRestartCompletedEvent(IDriverRestartCompleted driverRestartCompletedEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverRestartCompletedDispatcher.OnNext(driverRestartCompletedEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchDriverRestartRunningTaskEvent(IRunningTask runningTaskEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverRestartRunningTaskDispatcher.OnNext(runningTaskEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchDriverRestartActiveContextEvent(IActiveContext activeContextEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverRestartActiveContextDispatcher.OnNext(activeContextEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchDriverRestartedEvent(IDriverRestarted driverRestartedEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverRestartedDispatcher.OnNext(driverRestartedEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchCompletedTaskEvent(ICompletedTask completedTaskEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _completedTaskDispatcher.OnNext(completedTaskEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchRunningTaskEvent(IRunningTask runningTaskEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _runningTaskDispatcher.OnNext(runningTaskEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchFailedTaskEvent(IFailedTask failedTaskEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _failedTaskDispatcher.OnNext(failedTaskEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchTaskMessageEvent(ITaskMessage taskMessageEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _taskMessageDispatcher.OnNext(taskMessageEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchContextMessageEvent(IContextMessage contextMessageEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _contextMessageDispatcher.OnNext(contextMessageEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchFailedContextEvent(IFailedContext failedContextEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _failedContextDispatcher.OnNext(failedContextEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchClosedContextEvent(IClosedContext closedContextEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _closedContextDispatcher.OnNext(closedContextEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchActiveContextEvent(IActiveContext activeContextEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _activeContextDispatcher.OnNext(activeContextEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchCompletedEvaluatorEvent(ICompletedEvaluator completedEvaluatorEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _completedEvaluatorDispatcher.OnNext(completedEvaluatorEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchFailedEvaluatorEvent(IFailedEvaluator failedEvaluatorEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _failedEvaluatorDispatcher.OnNext(failedEvaluatorEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchAllocatedEvaluatorEventAsync(IAllocatedEvaluator allocatedEvaluatorEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _allocatedEvaluatorDispatcher.OnNext(allocatedEvaluatorEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchStartEventAsync(IDriverStarted startEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverStartedDispatcher.OnNext(startEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }

        public async Task DispatchStopEvent(IDriverStopped stopEvent)
        {
            Interlocked.Increment(ref _activeDispatchCounter);
            await Task.Run(() => _driverStoppedDispatcher.OnNext(stopEvent));
            Interlocked.Decrement(ref _activeDispatchCounter);
        }
    }
}
