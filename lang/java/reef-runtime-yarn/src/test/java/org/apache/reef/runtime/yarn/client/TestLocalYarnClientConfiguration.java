/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.runtime.yarn.client;

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.util.EnvironmentUtils;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.junit.Test;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tests the local yarn client configuration.
 */
public class TestLocalYarnClientConfiguration {

  private static final Logger LOG = Logger.getLogger(TestLocalYarnClientConfiguration.class.getName());

  @Test
  public void testLocalYarnClientConfiguration() throws Exception {
    final Configuration runtimeConf = LocalYarnClientConfiguration.CONF
        .set(LocalYarnClientConfiguration.ROOT_FOLDER, "C:\\Users\\tcondie\\")
        .build();

    final Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "test")
        .set(DriverConfiguration.GLOBAL_LIBRARIES, EnvironmentUtils.getClassLocation(TestDriver.class))
        .set(DriverConfiguration.ON_DRIVER_STARTED, TestDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, TestDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.DRIVER_JOB_SUBMISSION_DIRECTORY, "/tmp")
        .build();
    final LauncherStatus status = DriverLauncher.getLauncher(runtimeConf).run(driverConf, 0);
    LOG.log(Level.INFO, "Launch status: " + status.toString());
  }

  @Unit
  private static final class TestDriver {

    final class StartHandler implements EventHandler<StartTime> {
      @Override
      public void onNext(final StartTime value) {
      }
    }

    final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
      @Override
      public void onNext(final AllocatedEvaluator value) {
      }
    }
  }
}
