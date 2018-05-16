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
using System.Diagnostics;
using System.Threading;
using Grpc.Core;
using Org.Apache.REEF.Bridge.Proto;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Bridge.Client
{
    class CSharpDriverClientLauncher
    {
        private static readonly ITcpPortProvider tcpPortProvider= GetTcpProvider(9900, 9940);
        private static readonly Logger Logger = Logger.GetLogger(typeof(CSharpDriverClientLauncher));
        private static TextWriterTraceListener listener = new TextWriterTraceListener("Client.log");
        private static DriverServiceClient client;

        private static readonly bool AreAssertionsEnabled =
            #if DEBUG
               true
            #else
               false
            #endif
            ;

        private static ITcpPortProvider GetTcpProvider(int portRangeStart, int portRangeEnd)
        {
            var configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation<ITcpPortProvider, TcpPortProvider>()
                .BindIntNamedParam<TcpPortRangeStart>(portRangeStart.ToString())
                .BindIntNamedParam<TcpPortRangeCount>((portRangeEnd - portRangeStart + 1).ToString())
                .Build();
            return TangFactory.GetTang().NewInjector(configuration).GetInstance<ITcpPortProvider>();
        }

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

            ITcpPortProvider tcpPortProvider = GetTcpProvider(9900, 9940);
            Server server = null;

            int localServerPort = 0;
            foreach(int port in tcpPortProvider)
            {
                try
                {
                    server = new Server
                    {
                        Services = { DriverClient.BindService(new DriverClientService()) },
                        Ports = { new ServerPort("localhost", port, ServerCredentials.Insecure) }
                    };
                    localServerPort = port;
                    Logger.Log(Level.Info, "Driver client started on port {0}", localServerPort);
                    break;
                }
                catch (Exception e)
                {
                    Logger.Log(Level.Info, "Unable to connect to bind to port {0}", port);
                }
            }

            Int32 driverPort = 0;
            driverPort = Int32.Parse(args[0]);
            client = new DriverServiceClient(driverPort);
            client.RegisterDriverClientsService("localhost", localServerPort);

            listener.Flush();
        }
    }
}
