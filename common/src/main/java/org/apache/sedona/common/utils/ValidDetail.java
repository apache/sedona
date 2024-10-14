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
package org.apache.sedona.common.utils;

import java.util.Objects;
import org.locationtech.jts.geom.Geometry;

public class ValidDetail {
  public final boolean valid;
  public final String reason;
  public final Geometry location;

  public ValidDetail(boolean valid, String reason, Geometry location) {
    this.valid = valid;
    this.reason = reason;
    this.location = location;
  }

  public boolean equals(ValidDetail other) {
    return this.valid == other.valid
        && Objects.equals(this.reason, other.reason)
        && Objects.equals(this.location, other.location);
  }
}
