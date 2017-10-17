﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Network;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using Timer = System.Timers.Timer;

namespace Org.Apache.REEF.Tests.Functional
{
    public class ReefFunctionalTest : IDisposable
    {
        private readonly static Logger Logger = Logger.GetLogger(typeof(ReefFunctionalTest));

        protected const string DriverStdout = "driver.stdout";
        protected const string DriverStderr = "driver.stderr";
        protected const string EvaluatorStdout = "evaluator.stdout";
        protected const string CmdFile = "run.cmd";
        protected const string BinFolder = ".";

        protected const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        private const string Local = "local";
        private const string YARN = "yarn";
        private const int SleepTime = 1000;
        private const string PortRangeStart = "8900";
        private const string PortRangeCount = "1000";

        private const string StorageAccountKeyEnvironmentVariable = "REEFTestStorageAccountKey";
        private const string StorageAccountNameEnvironmentVariable = "REEFTestStorageAccountName";
        private bool _testSuccess = false;
        private bool _onLocalRuntime = false;

        private readonly bool _enableRealtimeLogUpload = false;

        protected string TestId { get; set; }

        protected Timer TestTimer { get; set; }

        protected Task TimerTask { get; set; }

        protected bool TestSuccess 
        {
            get { return _testSuccess; }
            set { _testSuccess = value; }
        }

        protected bool IsOnLocalRuntime
        {
            get { return _onLocalRuntime; }
            set { _onLocalRuntime = value; }
        }

        public ReefFunctionalTest()
        {
            Init();
        }

        public void Init()
        {
            TestId = Guid.NewGuid().ToString("N").Substring(0, 8);
            Logger.Log(Level.Info, "Running test " + TestId + ". If failed AND log uploaded is enabled, log can be find in " + Path.Combine(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), TestId));
            if (_enableRealtimeLogUpload)
            {
                TimerTask = new Task(() =>
                {
                    TestTimer = new Timer()
                    {
                        Interval = 1000,
                        Enabled = true,
                        AutoReset = true
                    };
                    TestTimer.Elapsed += PeriodicUploadLog;
                    TestTimer.Start();
                });
                TimerTask.Start(); 
            }
            
            ValidationUtilities.ValidateEnvVariable("JAVA_HOME");

            if (!Directory.Exists(BinFolder))
            {
                throw new InvalidOperationException(BinFolder + " not found in current directory, cannot init test");
            }
        }

        protected void CleanUp(string testFolder = DefaultRuntimeFolder)
        {
            Logger.Log(Level.Verbose, "Cleaning up test.");

            if (_enableRealtimeLogUpload)
            {
                if (TimerTask != null)
                {
                    TestTimer.Stop();
                    TimerTask.Dispose();
                    TimerTask = null;
                }

                // Wait for file upload task to complete
                Thread.Sleep(500);
            }

            string dir = Path.Combine(Directory.GetCurrentDirectory(), testFolder);
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, true);
                }
            }
            catch (IOException)
            {
                // do not fail if clean up is unsuccessful
            }
            catch (UnauthorizedAccessException)
            {
                // do not fail if clean up is unsuccessful
            }
        }

        public void Dispose() 
        {
            CleanUp();
        }

        protected void ValidateSuccessForLocalRuntime(int? numberOfContextsToClose, int numberOfTasksToFail = 0, int numberOfEvaluatorsToFail = 0, string testFolder = DefaultRuntimeFolder, int retryCount = 60, string expectedCancellationMessage = null)
        {
            const string successIndication = "EXIT: ActiveContextClr2Java::Close";
            const string failedTaskIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedTaskHandlerOnNext";
            const string failedEvaluatorIndication = "Java_org_apache_reef_javabridge_NativeInterop_clrSystemFailedEvaluatorHandlerOnNext";
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, retryCount);

            Logger.Log(Level.Verbose, "Lines read from log file : " + lines.Count());
            string[] successIndicators = lines.Where(s => s.Contains(successIndication)).ToArray();
            string[] failedTaskIndicators = lines.Where(s => s.Contains(failedTaskIndication)).ToArray();
            string[] failedEvaluatorIndicators = lines.Where(s => s.Contains(failedEvaluatorIndication)).ToArray();
            Assert.True(!numberOfContextsToClose.HasValue || numberOfContextsToClose == successIndicators.Length,
                "Expected number of contexts to close (" + numberOfContextsToClose + ") differs from actual number of success indicators (" + successIndicators.Length + ")");
            Assert.True(numberOfTasksToFail == failedTaskIndicators.Length,
                "Expected number of tasks to fail (" + numberOfTasksToFail + ") differs from actual number of failed task indicators (" + failedTaskIndicators.Length + ")");
            Assert.True(numberOfEvaluatorsToFail == failedEvaluatorIndicators.Length,
                "Expected number of evaluators to fail (" + numberOfEvaluatorsToFail + ") differs from actual number of failed evaluator indicators (" + failedEvaluatorIndicators.Length + ")");

            if (!string.IsNullOrWhiteSpace(expectedCancellationMessage))
            {
                Assert.True(lines.Any(line => line.Contains(expectedCancellationMessage)), "Did not find job cancellation message in log file. Expected message: " + expectedCancellationMessage);
            }
        }

        /// <summary>
        /// Get message counts from lines given
        /// </summary>
        /// <param name="lines"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        protected int GetMessageCount(string[] lines, string message)
        {
            return lines.Where(s => s.Contains(message)).ToArray().Length;
        }

        /// <summary>
        /// See <see cref="ValidateMessageSuccessfullyLogged"/> for detail. This function is <see cref="ValidateMessageSuccessfullyLogged"/>
        /// for the driver log.
        /// </summary>
        protected void ValidateMessageSuccessfullyLoggedForDriver(string message, string testFolder, int numberOfOccurrences = 1)
        {
            var msgs = new List<string> { message };
            ValidateMessageSuccessfullyLogged(msgs, "driver", DriverStdout, testFolder, numberOfOccurrences);
        }

        /// <summary>
        /// See <see cref="ValidateMessageSuccessfullyLogged"/> for detail. This function is <see cref="ValidateMessageSuccessfullyLogged"/>
        /// for the driver log.
        /// </summary>
        protected void ValidateMessagesSuccessfullyLoggedForDriver(
            IEnumerable<string> messages,
            string testFolder,
            int numberOfOccurrences = 1)
        {
            var msgs = new List<string>(messages);
            ValidateMessageSuccessfullyLogged(msgs, "driver", DriverStdout, testFolder, numberOfOccurrences);
        }

        /// <summary>
        /// Validates that each of the message provided in the <see cref="messages"/> parameter occurs 
        /// some number of times.
        /// If <see cref="numberOfOccurrences"/> is greater than or equal to 0, validates that each of the message in 
        /// <see cref="messages"/> occur <see cref="numberOfOccurrences"/> times.
        /// If <see cref="numberOfOccurrences"/> is less than 0, validates that each of the message in <see cref="messages"/>
        /// occur at least once.
        /// </summary>
        protected void ValidateMessageSuccessfullyLogged(
            IEnumerable<string> messages, string subfolder, string fileName, string testFolder, int numberOfOccurrences = 1)
        {
            string[] lines = ReadLogFile(fileName, subfolder, testFolder);
            foreach (string message in messages)
            {
                string[] successIndicators = lines.Where(s => s.Contains(message)).ToArray();
                if (numberOfOccurrences > 0)
                {
                    Assert.True(numberOfOccurrences == successIndicators.Count(), 
                        "Expected number of message \"" + message + "\" occurrences " + numberOfOccurrences + " differs from actual " + successIndicators.Count());
                }
                else if (numberOfOccurrences == 0)
                {
                    Assert.True(0 == successIndicators.Count(),
                        "Message \"" + message + "\" not expected to occur but occurs " + successIndicators.Count() + " times");
                }
                else
                {
                    Assert.True(successIndicators.Count() > 0, "Message \"" + message + "\" expected to occur, but did not.");
                }
            }
        }

        protected void PeriodicUploadLog(object source, ElapsedEventArgs e)
        {
            try
            {
                UploadDriverLog();
            }
            catch (Exception)
            {
                // log not available yet, ignore it
            }
        }

        internal string[] ReadLogFile(string logFileName, string subfolder = "driver", string testFolder = DefaultRuntimeFolder, int retryCount = 60)
        {
            string fileName = string.Empty;
            string[] lines = null;
            for (int i = 0; i < retryCount; i++)
            {
                try
                {
                    fileName = GetLogFileName(logFileName, subfolder, testFolder);
                    lines = File.ReadAllLines(fileName);
                    break;
                }
                catch (Exception e)
                {
                    if (i == retryCount - 1)
                    {
                        // log only last exception before failure
                        Logger.Log(Level.Verbose, e.ToString());
                    }
                    if (i < retryCount - 1)
                    {
                        Thread.Sleep(SleepTime);
                    }
                }
            }
            Assert.True(lines != null, "Cannot read from log file " + fileName);
            return lines;
        }

        protected string GetLogFileName(string logFileName, string subfolder = "driver", string testFolder = DefaultRuntimeFolder)
        {
            string driverContainerDirectory = Directory.GetDirectories(Path.Combine(Directory.GetCurrentDirectory(), testFolder), subfolder, SearchOption.AllDirectories).SingleOrDefault();
            Logger.Log(Level.Verbose, "GetLogFileName, driverContainerDirectory:" + driverContainerDirectory);

            if (string.IsNullOrWhiteSpace(driverContainerDirectory))
            {
                throw new InvalidOperationException("Cannot find driver container directory: " + driverContainerDirectory);
            }
            string logFile = Path.Combine(driverContainerDirectory, logFileName);
            if (!File.Exists(logFile))
            {
                throw new InvalidOperationException("Log file not found: " + logFile);
            }
            return logFile;
        }

        private void UploadDriverLog()
        {
            string driverStdout = GetLogFileName(DriverStdout);
            string driverStderr = GetLogFileName(DriverStderr);
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(GetStorageConnectionString());
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));   
            container.CreateIfNotExistsAsync().Wait();

            CloudBlockBlob blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdOut"));
            FileStream fs = new FileStream(driverStdout, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStreamAsync(fs).Wait();
            fs.Close();

            blob = container.GetBlockBlobReference(Path.Combine(TestId, "driverStdErr"));
            fs = new FileStream(driverStderr, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            blob.UploadFromStreamAsync(fs).Wait();
            fs.Close();
        }

        /// <summary>
        /// Assembles the storage account connection string from the environment.
        /// </summary>
        /// <returns>the storage account connection string assembled from the environment.</returns>
        /// <exception cref="Exception">If the environment variables aren't set.</exception>
        private static string GetStorageConnectionString()
        {
            var accountName = GetEnvironmentVariable(StorageAccountNameEnvironmentVariable,
                "Please set " + StorageAccountNameEnvironmentVariable + " to the storage account name to be used for the tests");

            var accountKey = GetEnvironmentVariable(StorageAccountKeyEnvironmentVariable,
                "Please set " + StorageAccountKeyEnvironmentVariable + " to the key of the storage account to be used for the tests");

            var result = @"DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey;
            return result;
        }

        /// <summary>
        /// Fetch the value of the given environment variable
        /// </summary>
        /// <param name="variableName"></param>
        /// <param name="errorMessageIfNotAvailable"></param>
        /// <returns>the value of the given environment variable</returns>
        /// <exception cref="Exception">
        /// If the environment variables is not set. The message is taken from
        /// errorMessageIfNotAvailable
        /// </exception>
        private static string GetEnvironmentVariable(string variableName, string errorMessageIfNotAvailable)
        {
            var result = Environment.GetEnvironmentVariable(variableName);
            if (string.IsNullOrWhiteSpace(result))
            {
                Exceptions.Throw(new Exception(errorMessageIfNotAvailable), Logger);
            }
            return result;
        }

        protected void TestRun(IConfiguration driverConfig, Type globalAssemblyType, int numberOfEvaluator, string jobIdentifier = "myDriver", string runOnYarn = "local", string runtimeFolder = DefaultRuntimeFolder)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(runOnYarn, numberOfEvaluator, runtimeFolder));
            var reefClient = injector.GetInstance<IREEFClient>();
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var jobSubmission = jobRequestBuilder
                .AddDriverConfiguration(driverConfig)
                .AddGlobalAssemblyForType(globalAssemblyType)
                .SetJobIdentifier(jobIdentifier)
                .Build();

            reefClient.SubmitAndGetJobStatus(jobSubmission);
        }

        private IConfiguration GetRuntimeConfiguration(string runOnYarn, int numberOfEvaluator, string runtimeFolder)
        {
            switch (runOnYarn)
            {
                case Local:
                    var dir = Path.Combine(".", runtimeFolder);
                    var localClientConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfEvaluator.ToString())
                        .Set(LocalRuntimeClientConfiguration.RuntimeFolder, dir)
                        .Build();
                    return Configurations.Merge(localClientConfig, GetTcpConnectionConfiguration());
                case YARN:
                    var yarnClientConfig = YARNClientConfiguration.ConfigurationModule.Build();
                    var tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                       .Set(TcpPortConfigurationModule.PortRangeStart, PortRangeStart)
                       .Set(TcpPortConfigurationModule.PortRangeCount, PortRangeCount)
                       .Build();
                    return Configurations.Merge(yarnClientConfig, tcpPortConfig, GetTcpConnectionConfiguration());
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }

        protected virtual IConfiguration GetTcpConnectionConfiguration()
        {
            return TcpClientConfigurationModule.ConfigurationModule
                .Set(TcpClientConfigurationModule.MaxConnectionRetry, "150")
                .Set(TcpClientConfigurationModule.SleepTime, "1000")
                .Build();
        }
    }
}