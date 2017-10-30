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
package org.apache.reef.runtime.yarn.util;

import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.reef.annotations.audience.Private;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that creates the various records in the YARN API.
 */
@Private
public final class YarnTypes {

  // TODO[REEF-537]: Remove once the hadoop version is updated.
  public static final String MIN_VERSION_KEEP_CONTAINERS_AVAILABLE = "2.4.0";
  private static final Logger LOG = Logger.getLogger(YarnTypes.class.getName());
  private YarnTypes() {
  }

  /**
   * @return a ContainerLaunchContext with the given commands and LocalResources.
   */
  public static ContainerLaunchContext getContainerLaunchContext(
      final List<String> commands,
      final Map<String, LocalResource> localResources,
      final byte[] securityTokenBuffer) {
    return getContainerLaunchContext(commands, localResources, securityTokenBuffer, null);
  }

  /**
   * Gets a LaunchContext and sets the environment variable
   * {@link YarnUtilities#REEF_YARN_APPLICATION_ID_ENV_VAR} for REEF Evaluators.
   * @return a ContainerLaunchContext with the given commands and LocalResources.
   */
  public static ContainerLaunchContext getContainerLaunchContext(
      final List<String> commands,
      final Map<String, LocalResource> localResources,
      final byte[] securityTokenBuffer,
      final ApplicationId applicationId) {
    final ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
    context.setLocalResources(localResources);
    context.setCommands(commands);
    final Map<String, String> envMap = new HashMap<>();
    if (applicationId != null) {
      envMap.put(YarnUtilities.REEF_YARN_APPLICATION_ID_ENV_VAR, applicationId.toString());
    }

    String sparkYarnMode = "SPARK_YARN_MODE";
    String containerAdlToken = "CONTAINER_ADL_TOKEN";
    String sessionId = "SESSION_ID";
    String orchestratorType = "ORCHESTRATOR_TYPE";

    for (String key : new String[] {sparkYarnMode, containerAdlToken, sessionId, orchestratorType}) {
      if (System.getenv(key) != null) {
        envMap.put(key, System.getenv(key));
      }
    }

    try {
      String sessionManagerHostKey = "SESSION_MANAGER_HOST";
      String sessionManagerHost = InetAddress.getLocalHost().getHostAddress();
      String sessionManagerPortKey = "SESSION_MANAGER_PORT";
      String sessionManagerPort = "2048";
      envMap.put(sessionManagerHostKey, sessionManagerHost);
      envMap.put(sessionManagerPortKey, sessionManagerPort);
    } catch(UnknownHostException e) {
      LOG.log(Level.WARNING, "Unable to publish sessionManagerHost to session manager endpoint.");
    }

    context.setEnvironment(envMap);
    if (securityTokenBuffer != null) {
      context.setTokens(ByteBuffer.wrap(securityTokenBuffer));
      LOG.log(Level.INFO, "Added tokens to container launch context");
    }
    return context;
  }

  public static boolean isAtOrAfterVersion(final String version) {
    final String hadoopVersion = VersionInfo.getVersion();

    if (hadoopVersion == null || hadoopVersion.length() < version.length()) {
      throw new RuntimeException("unsupported or incomplete hadoop version number provided for comparison: " +
          hadoopVersion);
    }

    return hadoopVersion.substring(0, version.length()).compareTo(version) >= 0;
  }
}
