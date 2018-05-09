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
using System.IO;
using System.Reflection;
using System.Text;
using Google.Protobuf;

using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Bridge.Proto;

namespace Org.Apache.REEF.Bridge.Client
{
    /// <summary>
    /// Proxy class that launches the Java driver service launcher.
    /// </summary>
    public class DriverServiceLauncher 
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverServiceLauncher));
        private static string configName = "DriverServiceLauncher.cfg";

        /// <summary>
        /// Finds the jar file that contains the bridge server
        /// </summary>
        /// <returns>A string which contains the name of the bridge server jar file/returns>
        static string FindBridgeServerJarFile()
        {
            string jarfile = null;

            Logger.Log(Level.Info, "Current working directory: [{0}]", Directory.GetCurrentDirectory());
            string searchDirectory = ".";
            string nameTemplate = "reef-bridge-proto-java-*-SNAPSHOT-jar-with-dependencies.jar";
            string[] files = Directory.GetFiles(searchDirectory, nameTemplate, SearchOption.AllDirectories);
            if (files != null && files.Length > 0)
            {
                jarfile = Path.GetFullPath(files[0]);
                Logger.Log(Level.Info, "Found bridge server jar file: [{0}]", Path.GetFullPath(files[0]));
            }
            else
            {
                throw new FileNotFoundException("Unable to find bridge server jar file");
            }
            return jarfile;
        }

        /// <summary>
        /// Collects libraries in the applications start directory tree.
        /// </summary>
        static void AddLibraryFiles(DriverClientConfiguration config)
        {
            string searchDirectory = ".";
            string nameTemplate = "*.dll";
            string[] files = Directory.GetFiles(searchDirectory, nameTemplate, SearchOption.AllDirectories);
            if (files != null && files.Length > 0)
            {
                foreach(string name in files)
                {
                    config.GlobalFiles.Add(name);
                    config.LocalFiles.Add(name);
                    Logger.Log(Level.Info, "Adding library file: [{0}]", name);
                }
            }
            else
            {
                throw new FileNotFoundException("Unable to find bridge library files");
            }
        }

        /// <summary>
        /// Launch the the bridge service with the given driver client configuration.
        /// </summary>
        /// <param name="config">Configuration for launching the driver client from the driver server</param>
        public static void Submit(DriverClientConfiguration config)
        {
            AddLibraryFiles(config);
            string jarfile = FindBridgeServerJarFile();

            // Write the driver service launcher configuration to a file.
            using (StreamWriter writer = new StreamWriter(configName))
            {
                JsonFormatter formatter = JsonFormatter.Default;
                formatter.Format(config, writer);
            }

            // Build the JVM command line arguments.
            bool waitForDebugger = true;
            string arguments =
                // Pause for debugger attach.
                ((waitForDebugger) ? "-agentlib:jdwp=transport=dt_socket,server=y,address=8000,suspend=y" : "")
                + " -cp " + jarfile
                // Execution entry point
                + " org.apache.reef.bridge.client.DriverServiceLauncher "
                + configName;
            Logger.Log(Level.Info, "Driver Service Launcher arguments: [{0}]", arguments);

            // Setup the JVM process.
            ProcessStartInfo rProcInfo = new ProcessStartInfo()
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
                FileName = "java.exe",
                Arguments = arguments
            };

            // Use string builders to capture the standard out and error.
            StringBuilder stdOutBuilder = new StringBuilder();
            stdOutBuilder.AppendLine("[StdOut]: ");
            StringBuilder stdErrBuilder = new StringBuilder();
            stdErrBuilder.AppendLine("[StdErr]: ");

            // Start the bridge server launcher.
            using (Process rProc = Process.Start(rProcInfo))
            {
                // Read the standard out and error on separate threads
                // simultaneously to avoid deadlock when rProc fills one
                // of the buffers and waits for this process to read it.
                // Use fully qualified namespace to avoid conflict
                // with Task class defined in visual studio.
                var stdOutTask = System.Threading.Tasks.Task.Run(
                  () => stdOutBuilder.Append(rProc.StandardOutput.ReadToEnd()));
                var stdErrTask = System.Threading.Tasks.Task.Run(
                  () => stdErrBuilder.Append(rProc.StandardError.ReadToEnd()));

                rProc.WaitForExit();
                if (rProc.ExitCode != 0)
                {
                    Logger.Log(Level.Error, "Exec of DriverServiceLauncher failed");
                    //throw new Exception("DriverServiceLauncher failed");
                }

                // Wait for std out and error readers.
                stdOutTask.Wait();
                stdErrTask.Wait();
            }
            Logger.Log(Level.Info, stdOutBuilder.ToString());
            Logger.Log(Level.Info, stdErrBuilder.ToString());
        }

        private DriverServiceLauncher()
        {
        }
    }
}
