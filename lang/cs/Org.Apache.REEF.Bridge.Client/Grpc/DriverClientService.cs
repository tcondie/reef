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
using System.CodeDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Client.DotNet.Events;
using Org.Apache.REEF.Bridge.Proto;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Runtime;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using EvaluatorDescriptorImpl = Org.Apache.REEF.Bridge.Client.DotNet.Events.EvaluatorDescriptorImpl;
using Void = Org.Apache.REEF.Bridge.Proto.Void;

namespace Org.Apache.REEF.Bridge.Client.DotNet.Grpc
{
    internal class DriverClientService : DriverClient.DriverClientBase, IDriverClientService
    {
        private static readonly Logger _log = Logger.GetLogger(typeof(DriverClientService));

        private static readonly Void _void = new Void();

        private readonly Server _server;

        private readonly int _serverPort;

        private readonly DriverBridge _driverBridge;

        private readonly BridgeClock _bridgeClock;

        private readonly DriverServiceClient _driverServiceClient;

        private readonly IDictionary<string, BridgeActiveContext> _activeContexts = new Dictionary<string, BridgeActiveContext>();

        [Inject]
        private DriverClientService(
            DriverBridge driverBridge,
            BridgeClock bridgeClock,
            DriverServiceClient driverServiceClient, 
            TcpPortProvider tcpPortProvider)
        {
            _driverBridge = driverBridge;
            _bridgeClock = bridgeClock;
            _driverServiceClient = driverServiceClient;
            bool serverStarted = false;
            foreach (int port in tcpPortProvider)
            {
                try
                {
                    _serverPort = port;
                    _server = new Server
                    {
                        Services = {DriverClient.BindService(this)},
                        Ports = {new ServerPort("localhost", port, ServerCredentials.Insecure)}
                    };
                    _server.Start();
                    serverStarted = true;
                }
                catch (IOException e)
                {
                    _log.Log(Level.Info, "Unable to bind to port {0}", port);
                }
            }
            if (!serverStarted)
            {
                _log.Log(Level.Error, "Unable to start gRPC server");
                throw new IOException("Unable to start server");
            }
        }

#pragma IDriverClientService

        public void start()
        {
            _driverServiceClient.RegisterDriverClientService("localhost", _serverPort);
        }

        public void awaitTermination()
        {
            _server.ShutdownTask.GetAwaiter().GetResult();
        }

#pragma DriverClientBase

        public override Task<Void> AlarmTrigger(AlarmTriggerInfo request, ServerCallContext context)
        {
            _bridgeClock.OnNext(new BridgeClock.AlarmInfo
            {
                AlarmId = request.AlarmId,
                Timestamp = request.Timestamp
            });
            return Task.FromResult(_void);
        }

        public override Task<IdleStatus> IdlenessCheckHandler(Void request, ServerCallContext context)
        {
            return Task.FromResult(new IdleStatus
            {
               IsIdle = _driverBridge.IsIdle && _bridgeClock.IsIdle,
               Reason = "Driver client"
            });
        }

        public override Task<Void> StartHandler(StartTimeInfo request, ServerCallContext context)
        {
            _driverBridge.DispatchStartEventAsync(new BridgeDriverStarted
            {
                StartTime = new DateTime(request.StartTime)
            });
            return Task.FromResult(_void);
        }

        public override Task<Void> StopHandler(StopTimeInfo request, ServerCallContext context)
        {
            _driverBridge.DispatchStopEvent(new BridgeDriverStopped
            {
                StopTime = new DateTime(request.StopTime)
            });
            return Task.FromResult(_void);
        }

#pragma Context Handlers

        public override Task<Void> ActiveContextHandler(ContextInfo request, ServerCallContext context)
        {
            var activeContext = new BridgeActiveContext(_driverServiceClient)
            {
                Id = request.ContextId,
                EvaluatorId = request.EvaluatorId,
                ParentId = Optional<string>.OfNullable(request.ParentId.Length == 0 ? null : request.ParentId),
                EvaluatorDescriptor = Create(request.EvaluatorDescriptorInfo)
            };
            _activeContexts[activeContext.Id] = activeContext;
            _driverBridge.DispatchActiveContextEvent(activeContext);
            return Task.FromResult(_void);
        }

        public override Task<Void> ClosedContextHandler(ContextInfo request, ServerCallContext context)
        {
            if (_activeContexts.ContainsKey(request.ContextId))
            {
                var activeContext = _activeContexts[request.ContextId];
                _activeContexts.Remove(request.ContextId);
                var parentContext = _activeContexts[activeContext.ParentId.Value];
                _driverBridge.DispatchClosedContextEvent(new BridgeClosedContext()
                {
                    Id = activeContext.Id,
                    EvaluatorId = activeContext.EvaluatorId,
                    ParentId = Optional<string>.Of(parentContext.Id),
                    ParentContext = parentContext,
                    EvaluatorDescriptor = Create(request.EvaluatorDescriptorInfo)
                });
            }
            else
            {
                _log.Log(Level.Error, "Unknown context {0}", request.ContextId);
            }
            return Task.FromResult(_void);
        }

        public override Task<Void> ContextMessageHandler(ContextMessageInfo request, ServerCallContext context)
        {
            _driverBridge.DispatchContextMessageEvent(new BridgeContextMessage()
            {
                Id = request.ContextId,
                MessageSourceId = request.MessageSourceId,
                Message = request.Payload.ToByteArray()
            });
            return Task.FromResult(_void);
        }

        public override Task<Void> FailedContextHandler(ContextInfo request, ServerCallContext context)
        {
            if (_activeContexts.ContainsKey(request.ContextId))
            {
                var activeContext = _activeContexts[request.ContextId];
                _activeContexts.Remove(request.ContextId);
                var parentContext = activeContext.ParentId.IsPresent() ? _activeContexts[activeContext.ParentId.Value] : null;
                _driverBridge.DispatchFailedContextEvent(new BridgeFailedContext()
                {
                    Id = activeContext.Id,
                    EvaluatorId = activeContext.EvaluatorId,
                    ParentId = Optional<string>.OfNullable(parentContext == null ? null : parentContext.Id),
                    ParentContext = Optional<IActiveContext>.OfNullable(parentContext),
                    EvaluatorDescriptor = Create(request.EvaluatorDescriptorInfo)
                });
            }
            else
            {
                _log.Log(Level.Error, "unknown failed context {0}", request.ContextId);
            }
            return Task.FromResult(_void);
        }

        // Evaluator handlers

        public override Task<Void> AllocatedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            return base.AllocatedEvaluatorHandler(request, context);
        }

        public override Task<Void> CompletedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            return base.CompletedEvaluatorHandler(request, context);
        }

        public override Task<Void> FailedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            return base.FailedEvaluatorHandler(request, context);
        }

        // Task handlers

        public override Task<Void> RunningTaskHandler(TaskInfo request, ServerCallContext context)
        {
            return base.RunningTaskHandler(request, context);
        }

        public override Task<Void> CompletedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            return base.CompletedTaskHandler(request, context);
        }

        public override Task<Void> FailedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            return base.FailedTaskHandler(request, context);
        }

        public override Task<Void> TaskMessageHandler(TaskMessageInfo request, ServerCallContext context)
        {
            return base.TaskMessageHandler(request, context);
        }

        public override Task<Void> SuspendedTaskHandler(TaskInfo request, ServerCallContext context)
        {
            return base.SuspendedTaskHandler(request, context);
        }

        // Client handlers

        public override Task<Void> ClientCloseHandler(Void request, ServerCallContext context)
        {
            return base.ClientCloseHandler(request, context);
        }

        public override Task<Void> ClientCloseWithMessageHandler(ClientMessageInfo request, ServerCallContext context)
        {
            return base.ClientCloseWithMessageHandler(request, context);
        }

        public override Task<Void> ClientMessageHandler(ClientMessageInfo request, ServerCallContext context)
        {
            return base.ClientMessageHandler(request, context);
        }

#pragma Driver Restart Handlers

        public override Task<Void> DriverRestartHandler(DriverRestartInfo request, ServerCallContext context)
        {
            return base.DriverRestartHandler(request, context);
        }

        public override Task<Void> DriverRestartActiveContextHandler(ContextInfo request, ServerCallContext context)
        {
            return base.DriverRestartActiveContextHandler(request, context);
        }

        public override Task<Void> DriverRestartRunningTaskHandler(TaskInfo request, ServerCallContext context)
        {
            return base.DriverRestartRunningTaskHandler(request, context);
        }

        public override Task<Void> DriverRestartCompletedHandler(DriverRestartCompletedInfo request, ServerCallContext context)
        {
            return base.DriverRestartCompletedHandler(request, context);
        }

        public override Task<Void> DriverRestartFailedEvaluatorHandler(EvaluatorInfo request, ServerCallContext context)
        {
            return base.DriverRestartFailedEvaluatorHandler(request, context);
        }

#pragma Private helpers

        private IEvaluatorDescriptor Create(EvaluatorDescriptorInfo descriptorInfo)
        {
            RuntimeName runtimeName = RuntimeName.Local;
            if (!string.IsNullOrWhiteSpace(descriptorInfo.RuntimeName) && !Enum.TryParse(descriptorInfo.RuntimeName, true, out runtimeName))
            {
                throw new ArgumentException("Unknown runtime name received " + descriptorInfo.RuntimeName);
            }
            return new EvaluatorDescriptorImpl
            {
                NodeDescriptor = null,
                EvaluatorType = EvaluatorType.CLR,
                Memory = descriptorInfo.Memory,
                VirtualCore = descriptorInfo.Cores,
                RuntimeName = runtimeName
            };
        }
    }
}
