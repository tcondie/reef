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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Proto;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Bridge.Client.DotNet.Grpc
{
    internal class DriverServiceClient : IDriverServiceClient
    {
        private readonly DriverService.DriverServiceClient _driverServiceStub;

        [Inject]
        private DriverServiceClient(
            [Parameter(Value = typeof(Parameters.DriverServicePort))] int driverServicePort)
        {
            Channel driverServiceChannel = new Channel("localhost:" + driverServicePort, ChannelCredentials.Insecure);
            _driverServiceStub = new DriverService.DriverServiceClient(driverServiceChannel);
        }

        public void RegisterDriverClientService(string host, int port)
        {
            _driverServiceStub.RegisterDriverClient(new DriverClientRegistration
            {
                Host = host,
                Port = port
            });
        }

        public void OnShutdown()
        {
            throw new NotImplementedException();
        }

        public void OnShutdown(Exception ex)
        {
            throw new NotImplementedException();
        }

        public void OnSetAlarm(string alarmId, long timeoutMS)
        {
            throw new NotImplementedException();
        }

        public void OnEvaluatorRequest(IEvaluatorRequest evaluatorRequest)
        {
            throw new NotImplementedException();
        }

        public void OnEvaluatorClose(string evalautorId)
        {
            throw new NotImplementedException();
        }

        public void OnEvaluatorSubmit(string evaluatorId, IConfiguration contextConfiguration, IConfiguration taskConfiguration,
            List<FileInfo> addFileList, List<FileInfo> addLibraryList)
        {
            throw new NotImplementedException();
        }

        public void OnContextClose(string contextId)
        {
            throw new NotImplementedException();
        }

        public void OnContextSubmitContext(string contextId, IConfiguration contextConfiguration)
        {
            throw new NotImplementedException();
        }

        public void OnContextSubmitTask(string contextId, IConfiguration taskConfiguration)
        {
            throw new NotImplementedException();
        }

        public void OnContextMessage(string contextId, byte[] message)
        {
            throw new NotImplementedException();
        }

        public void OnTaskClose(string taskId, byte[] message)
        {
            throw new NotImplementedException();
        }

        public void OnTaskMessage(string taskId, byte[] message)
        {
            throw new NotImplementedException();
        }
    }
}
