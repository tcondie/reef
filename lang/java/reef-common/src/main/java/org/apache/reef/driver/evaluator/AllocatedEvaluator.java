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
package org.apache.reef.driver.evaluator;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Public;
import org.apache.reef.driver.ContextAndTaskSubmittable;
import org.apache.reef.driver.ContextSubmittable;
import org.apache.reef.driver.TaskSubmittable;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.runtime.common.files.FileType;
import org.apache.reef.tang.Configuration;

import java.io.File;
import java.net.URL;

/**
 * Represents an Evaluator that is allocated, but is not running yet.
 */
@Public
@DriverSide
@Provided
public interface AllocatedEvaluator
    extends AutoCloseable, Identifiable, ContextSubmittable, ContextAndTaskSubmittable, TaskSubmittable {

  /**
   * Puts the given file into the working directory of the Evaluator.
   *
   * @param file the file to be copied
   */
  void addFile(final File file);

  /**
   * Puts the given file into the working directory of the Evaluator and adds it to its classpath.
   *
   * @param file the file to be copied
   */
  void addLibrary(final File file);

  /**
   * Puts the given file into the working directory of the evaluator under the provided name localName.
   * @param file the location of the file
   * @param type the type of the file
   * @param localName the name that the file will be localized as
   */
  void addRemoteFile(final URL file, FileType type, String localName);

  /**
   * @return the evaluator descriptor of this evaluator.
   */
  EvaluatorDescriptor getEvaluatorDescriptor();

  /**
   * Specify the process to be instantiated for the Evaluator.
   * Defaults to an EvaluatorProcess instantiated by the binded ProcessFactory.
   *
   * @param process
   */
  void setProcess(final EvaluatorProcess process);

  /**
   * Releases the allocated evaluator back to the resource manager.
   */
  @Override
  void close();

  /**
   * Submits the given Task for execution.
   * <p>
   * This generates a ContextConfiguration for the root context with a generated ID derived from the EvaluatorId.
   *
   * @param taskConfiguration the Configuration. See TaskConfiguration for details.
   */
  @Override
  void submitTask(final Configuration taskConfiguration);

  @Override
  void submitContext(final Configuration contextConfiguration);

  @Override
  void submitContextAndService(final Configuration contextConfiguration,
                               final Configuration serviceConfiguration);

  @Override
  void submitContextAndTask(final Configuration contextConfiguration,
                            final Configuration taskConfiguration);

  @Override
  void submitContextAndServiceAndTask(final Configuration contextConfiguration,
                                      final Configuration serviceConfiguration,
                                      final Configuration taskConfiguration);
}
