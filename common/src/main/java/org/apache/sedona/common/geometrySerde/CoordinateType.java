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
package org.apache.sedona.common.geometrySerde;

enum CoordinateType {
  XY(1, 2, false, false),
  XYZ(2, 3, true, false),
  XYM(3, 3, false, true),
  XYZM(4, 4, true, true);

  public final int value;
  public final int ordinates;
  public final boolean hasZ;
  public final boolean hasM;
  public final int bytes;

  CoordinateType(int value, int ordinates, boolean hasZ, boolean hasM) {
    this.value = value;
    this.ordinates = ordinates;
    this.hasZ = hasZ;
    this.hasM = hasM;
    this.bytes = 8 * ordinates;
  }

  public static CoordinateType valueOf(int value) {
    switch (value) {
      case 1:
        return XY;
      case 2:
        return XYZ;
      case 3:
        return XYM;
      case 4:
        return XYZM;
      default:
        throw new IllegalArgumentException("Invalid coordinate type value: " + value);
    }
  }
}
