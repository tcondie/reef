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
package org.apache.reef.runtime.common.files;

import org.apache.reef.util.BuilderUtils;

import java.net.URL;

/**
 * Default POJO implementation of RemoteResource.
 * Use newBuilder to construct an instance.
 */
public final class RemoteResourceImpl implements RemoteResource {
  private final FileType type;
  private final String name;
  private final URL resourceLocation;

  private RemoteResourceImpl(final Builder builder) {
    this.type = BuilderUtils.notNull(builder.type);
    this.name = BuilderUtils.notNull(builder.name);
    this.resourceLocation = BuilderUtils.notNull(builder.resourceLocation);
  }

  @Override
  public FileType getType() {
    return type;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public URL getResourceLocation() {
    return resourceLocation;
  }

  @Override
  public String toString() {
    return String.format("RemoteResource: {%s:%s=%s}", this.name, this.type, this.resourceLocation);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to create RemoteResource instances.
   */
  public static final class Builder implements org.apache.reef.util.Builder<RemoteResourceImpl> {
    private FileType type;
    private String name;
    private URL resourceLocation;

    /**
     * @see RemoteResource#getType()
     */
    public Builder setType(final FileType type) {
      this.type = type;
      return this;
    }

    /**
     * @see RemoteResource#getName()
     */
    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    /**
     * @see RemoteResource#getResourceLocation()
     */
    public Builder setResourceLocation(final URL resourceLocation) {
      this.resourceLocation = resourceLocation;
      return this;
    }

    @Override
    public RemoteResourceImpl build() {
      return new RemoteResourceImpl(this);
    }
  }
}
