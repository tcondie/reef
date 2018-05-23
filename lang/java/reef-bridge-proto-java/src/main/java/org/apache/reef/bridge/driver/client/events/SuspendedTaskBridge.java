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

package org.apache.reef.bridge.driver.client.events;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.SuspendedTask;

/**
 * Suspended task bridge.
 */
public final class SuspendedTaskBridge implements SuspendedTask {

  private final String taskId;

  private final ActiveContext context;

  private final byte[] result;

  public SuspendedTaskBridge(final String taskId, final ActiveContext context, final byte[] result) {
    this.taskId = taskId;
    this.context = context;
    this.result = result;
  }

  @Override
  public ActiveContext getActiveContext() {
    return this.context;
  }

  @Override
  public byte[] get() {
    return this.result;
  }

  @Override
  public String getId() {
    return this.taskId;
  }
}
