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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;


namespace Org.Apache.REEF.Examples.HelloREEF
{
    public sealed class HelloDriver :
        IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<ICompletedTask>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloDriver));
        private readonly IEvaluatorRequestor _evaluatorRequestor;

        /// <summary>
        /// Job driver constructed by Tang.
        /// </summary>
        /// <param name="evaluatorRequestor"></param>
        [Inject]
        private HelloDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _evaluatorRequestor = evaluatorRequestor;
        }

        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, "HelloDriver started at {0}", driverStarted.StartTime);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetMegabytes(64).Build());
        }

        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
             Logger.Log(Level.Info, "Evaluator allocated: {0}", allocatedEvaluator);

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloTask")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();

            Logger.Log(Level.Verbose, "Submit task: {0}", taskConfiguration);
            allocatedEvaluator.SubmitTask(taskConfiguration);
        }

        public void OnNext(ICompletedTask completedTask)
        {
            Logger.Log(Level.Info, "HelloREEF task completed {0}", completedTask);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }
}
