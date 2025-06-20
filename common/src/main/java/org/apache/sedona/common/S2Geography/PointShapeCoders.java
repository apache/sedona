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
package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.S2Coder;
import com.google.common.geometry.S2Point;
import java.lang.reflect.Field;

public class PointShapeCoders {
  public static final S2Coder<S2Point.Shape> FAST;
  public static final S2Coder<S2Point.Shape> COMPACT;

  static {
    try {
      // Locate the nested Coder class
      Class<?> coderClass = Class.forName("com.google.common.geometry.S2Point$Shape$Coder");

      // Grab the private static fields
      Field fastField = coderClass.getDeclaredField("FAST");
      fastField.setAccessible(true);
      FAST = (S2Coder<S2Point.Shape>) fastField.get(null);

      Field compactField = coderClass.getDeclaredField("COMPACT");
      compactField.setAccessible(true);
      COMPACT = (S2Coder<S2Point.Shape>) compactField.get(null);
    } catch (Exception e) {
      throw new RuntimeException("Unable to access S2Point.Shape.Coder fields", e);
    }
  }

  private PointShapeCoders() {}
}
