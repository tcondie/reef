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

import org.apache.hadoop.fs.Path;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.DriverJobSubmissionDirectory;
import org.apache.reef.runtime.common.client.DriverConfigurationProvider;
import org.apache.reef.runtime.common.client.api.JobSubmissionEvent;
import org.apache.reef.runtime.common.client.api.JobSubmissionHandler;
import org.apache.reef.runtime.common.files.JobJarMaker;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.runtime.yarn.client.parameters.RootFolder;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

@Private
@ClientSide
final class LocalYarnJobSubmissionHandler implements JobSubmissionHandler {

  private static final Logger LOG = Logger.getLogger(
      org.apache.reef.runtime.yarn.client.YarnJobSubmissionHandler.class.getName());

  private final String rootFolder;
  private final JobJarMaker jobJarMaker;
  private final REEFFileNames fileNames;
  private final DriverConfigurationProvider driverConfigurationProvider;

  private String applicationId;

  @Inject
  LocalYarnJobSubmissionHandler(
      @Parameter(RootFolder.class) final String rootFolder,
      final JobJarMaker jobJarMaker,
      final REEFFileNames fileNames,
      final DriverConfigurationProvider driverConfigurationProvider) throws IOException {

    this.rootFolder = rootFolder;
    this.jobJarMaker = jobJarMaker;
    this.fileNames = fileNames;
    this.driverConfigurationProvider = driverConfigurationProvider;
  }

  @Override
  public void close() {
  }

  @Override
  public void onNext(final JobSubmissionEvent jobSubmissionEvent) {

    final String id = jobSubmissionEvent.getIdentifier();
    LOG.log(Level.FINEST, "Submitting{0} job: {1}",
        new Object[] {jobSubmissionEvent});

    try {
      final File jobFolder = new File(new File(this.rootFolder), this.fileNames.getREEFFolderName());

      if (!jobFolder.exists() && !jobFolder.mkdirs()) {
        LOG.log(Level.WARNING, "Failed to create [{0}]", jobFolder.getAbsolutePath());
      }
      LOG.log(Level.FINE, "Assembling submission JAR for the Driver.");
      final Path jobSubmissionDirectory = getUserBoundJobSubmissionDirectory(jobSubmissionEvent.getConfiguration());
      final Configuration driverConfiguration = makeDriverConfiguration(jobSubmissionEvent, jobSubmissionDirectory);
      final File jobSubmissionFile = this.jobJarMaker.createJobSubmissionJAR(jobSubmissionEvent, driverConfiguration);
      LOG.log(Level.INFO, "Moving " + jobSubmissionFile.getName() + " to final path " + jobFolder.toPath());
      final File destination = new File(jobFolder, jobSubmissionFile.getName());
      Files.move(jobSubmissionFile.toPath(), destination.toPath());

      LOG.log(Level.FINEST, "Generated{0} job with ID {1} to job folder {2}", new String[] {
          this.applicationId, destination.toString()});
    } catch (final IOException e) {
      throw new RuntimeException("Unable to submit Driver to YARN: " + id, e);
    }
  }

  /**
   * Get the RM application ID.
   * Return null if the application has not been submitted yet, or was submitted unsuccessfully.
   * @return string application ID or null if no app has been submitted yet.
   */
  @Override
  public String getApplicationId() {
    return this.applicationId;
  }

  /**
   * Assembles the Driver configuration.
   */
  private Configuration makeDriverConfiguration(
      final JobSubmissionEvent jobSubmissionEvent,
      final Path jobFolderPath) throws IOException {

    return this.driverConfigurationProvider.getDriverConfiguration(
        jobFolderPath.toUri(),
        jobSubmissionEvent.getRemoteId(),
        jobSubmissionEvent.getIdentifier(),
        jobSubmissionEvent.getConfiguration());
  }


  private static Path getUserBoundJobSubmissionDirectory(final Configuration configuration) {
    try {
      return new Path(Tang.Factory.getTang().newInjector(configuration)
          .getNamedInstance(DriverJobSubmissionDirectory.class));
    } catch (final InjectionException ex) {
      throw new RuntimeException(DriverJobSubmissionDirectory.class.getName() + " not set!", ex);
    }

  }

}
