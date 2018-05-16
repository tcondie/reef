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
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Bridge.Client.DotNet
{
    interface IDriverServiceClient
    {
        /// <summary>
        /// Initiate shutdown.
        /// </summary>
        void OnShutdown();

        /// <summary>
        /// Initiate shutdown with exception.
        /// </summary>
        /// <param name="ex">exception</param>
        void OnShutdown(Exception ex);

        // Clock Operations

        void OnSetAlarm(string alarmId, long timeoutMS);

        // Evaluator Operations

        void OnEvaluatorRequest(IEvaluatorRequest evaluatorRequest);

        void OnEvaluatorClose(string evalautorId);

        void OnEvaluatorSubmit(
            string evaluatorId,
            IConfiguration contextConfiguration,
            IConfiguration taskConfiguration,
            List<FileInfo> addFileList,
            List<FileInfo> addLibraryList);


        // Context operations 

        void OnContextClose(string contextId);

        void OnContextSubmitContext(string contextId, IConfiguration contextConfiguration);

        void OnContextSubmitTask(string contextId, IConfiguration taskConfiguration);

        void OnContextMessage(string contextId, byte[] message);

        // Task operations

        void OnTaskClose(string taskId, byte[] message);

        void OnTaskMessage(string taskId, byte[] message);
    }
}
