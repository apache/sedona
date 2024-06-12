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
package org.apache.sedona.snowflake.snowsql.ddl;

import java.lang.reflect.Parameter;

public class ArgSpecBuilder {
  /**
   * Build argument spec for a function. Format: argName1 argType1, argName2 argType2, ...
   *
   * @param argTypesRaw
   * @param argNames
   * @return
   */
  public static String args(Parameter[] argTypesRaw, String[] argNames, String[] argTypesCustom) {
    StringBuilder argTypesBuilder = new StringBuilder();
    for (int it = 0; it < argTypesRaw.length; it++) {
      argTypesBuilder.append(
          String.format(
              "%s %s",
              argNames[it],
              // Use the argTypes array if types are manually specified, otherwise use Reflection to
              // get the type name
              Constants.snowflakeTypeMap.get(
                  argTypesCustom.length == 0
                      ? argTypesRaw[it].getType().getTypeName()
                      : argTypesCustom[it])));
      if (it + 1 != argTypesRaw.length) {
        argTypesBuilder.append(", ");
      }
    }
    String argSpec = argTypesBuilder.toString();
    return argSpec;
  }

  /**
   * Build argument spec for a function. Format: argType1, argType2, ...
   *
   * @param argTypesRaw
   * @return
   */
  public static String argTypes(Parameter[] argTypesRaw, String[] argTypesCustom) {
    StringBuilder argTypesBuilder = new StringBuilder();
    for (int it = 0; it < argTypesRaw.length; it++) {
      argTypesBuilder.append(
          String.format(
              "%s",
              // Use the argTypes array if types are manually specified, otherwise use Reflection to
              // get the type name
              Constants.snowflakeTypeMap.get(
                  argTypesCustom.length == 0
                      ? argTypesRaw[it].getType().getTypeName()
                      : argTypesCustom[it])));
      if (it + 1 != argTypesRaw.length) {
        argTypesBuilder.append(", ");
      }
    }
    String argSpec = argTypesBuilder.toString();
    return argSpec;
  }
}
