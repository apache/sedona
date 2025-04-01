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
package org.apache.sedona.sql.datasources.osmpbf.model;

public enum RelationType {
  NODE,
  WAY,
  RELATION;

  public static String fromValue(int number) {
    switch (number) {
      case 0:
        return NODE.toString();
      case 1:
        return WAY.toString();
      case 2:
        return RELATION.toString();
      default:
        throw new IllegalArgumentException("Unknown relation type: " + number);
    }
  }
}
