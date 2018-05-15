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
using System.Threading;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Client
{
    class CSharpDriverClientLauncher
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CSharpDriverClientLauncher));
        private static readonly bool AreAssertionsEnabled =
            #if DEBUG
               true
            #else
               false
            #endif
            ;

        /// <summary>
        /// Launches a REEF client process (Driver or Evaluator)
        /// </summary>
        /// <param name="args">Command-line arguments</param>
        static void Main(string[] args)
        {
            bool doDebug = true;
            while (doDebug)
            {
                Thread.Sleep(1000);
            }
            Console.WriteLine("C# CLIENT IS RUNNING");
            Logger.Log(Level.Info, "Entering CSharpDriverClientLauncher.main()");
            Logger.Log(Level.Verbose, "CSharpDriverClientLauncher started with user name [{0}]", Environment.UserName);
            Logger.Log(Level.Verbose, "CSharpDriverClientLauncher started. Assertions are {0} in this process.",
                AreAssertionsEnabled ? "ENABLED" : "DISABLED");
        }
    }
}
