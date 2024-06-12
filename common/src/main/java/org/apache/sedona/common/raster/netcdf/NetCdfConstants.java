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
package org.apache.sedona.common.raster.netcdf;

public class NetCdfConstants {
  public static final String VALID_MIN = "valid_min";
  public static final String VALID_MAX = "valid_max";
  public static final String MISSING_VALUE = "missing_value";
  public static final String VALID_RANGE = "valid_range";
  public static final String SCALE_FACTOR = "scale_factor";
  public static final String ADD_OFFSET = "add_offset";
  public static final String LONG_NAME = "long_name";
  public static final String UNITS = "units";
  public static final String GLOBAL_ATTR_PREFIX = "GLOBAL_ATTRIBUTES";
  public static final String VAR_ATTR_PREFIX = "VAR_";
  public static final String BAND_ATTR_PREFIX = "BAND_";

  public static final String INSUFFICIENT_DIMENSIONS_VARIABLE =
      "Provided variable has less than two dimensions";

  public static final String NON_NUMERIC_VALUE =
      "An attribute expected to have numeric values does not have numeric value";

  public static final String INVALID_LON_NAME = "Provided longitude variable short name is invalid";

  public static final String INVALID_LAT_NAME = "Provided latitude variable short name is invalid";

  public static final String INVALID_LAT_SHAPE = "Shape of latitude variable is supposed to be 1";

  public static final String INVALID_LON_SHAPE = "Shape of longitude variable is supposed to be 1";

  public static final String INVALID_RECORD_NAME = "Provided record variable short name is invalid";

  public static final String COORD_VARIABLE_NOT_FOUND =
      "Corresponding coordinate variable not found for dimension %s";

  public static final String COORD_IDX_NOT_FOUND =
      "Given record variable does not contain one of the latitude or longitude dimensions as the participating dimensions";
}
