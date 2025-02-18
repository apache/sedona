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

import java.util.HashMap;

public class OSMEntity {
  private long id;
  private String kind;

  private HashMap<String, String> tags;
  private Double latitude;
  private Double longitude;
  private long[] refs;
  private String[] refRoles;
  private String[] refTypes;

  public OSMEntity(
      long id, double latitude, double longitude, HashMap<String, String> tags, String kind) {
    this.id = id;
    this.kind = kind;
    this.tags = tags;
    this.latitude = latitude;
    this.longitude = longitude;
  }

  public OSMEntity(
      Long id,
      HashMap<String, String> tags,
      String relation,
      long[] refs,
      String[] refTypes,
      String[] refRoles) {
    this.id = id;
    this.tags = tags;
    this.kind = relation;
    this.refs = refs;
    this.refTypes = refTypes;
    this.refRoles = refRoles;
  }

  public OSMEntity(Long id, HashMap<String, String> tags, String relation, long[] refs) {
    this.id = id;
    this.tags = tags;
    this.kind = relation;
    this.refs = refs;
  }

  public String getKind() {
    return kind;
  }

  public HashMap<String, String> getTags() {
    return tags;
  }

  public Double getLatitude() {
    return latitude;
  }

  public Double getLongitude() {
    return longitude;
  }

  public long[] getRefs() {
    return refs;
  }

  public String[] getRefRoles() {
    return refRoles;
  }

  public String[] getRefTypes() {
    return refTypes;
  }

  public long getId() {
    return id;
  }
}
