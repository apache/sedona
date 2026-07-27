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
package org.apache.sedona.core.wrapper;

import java.io.Serializable;

/** Stable row identity used by query-local planar KNN execution. */
public final class KnnGeometryMetadata implements Serializable {
  private final long uniqueId;
  private final Object originalUserData;

  public KnnGeometryMetadata(long uniqueId, Object originalUserData) {
    this.uniqueId = uniqueId;
    this.originalUserData = originalUserData;
  }

  /**
   * Adds stable identity without nesting metadata when an execution path is prepared more than
   * once.
   */
  public static KnnGeometryMetadata wrap(long uniqueId, Object userData) {
    if (userData instanceof KnnGeometryMetadata) {
      return (KnnGeometryMetadata) userData;
    }
    return new KnnGeometryMetadata(uniqueId, userData);
  }

  public long getUniqueId() {
    return uniqueId;
  }

  /** Returns the row payload, tolerating metadata nested by an older or external caller. */
  public Object getOriginalUserData() {
    Object userData = originalUserData;
    while (userData instanceof KnnGeometryMetadata) {
      userData = ((KnnGeometryMetadata) userData).originalUserData;
    }
    return userData;
  }
}
