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
package org.apache.sedona.flink.confluent;

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.apache.sedona.common.utils.FormatUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class Constructors {

  public static Geometry getGeometryByType(
      String geom, String inputDelimiter, GeometryType geometryType) throws ParseException {
    FileDataSplitter delimiter =
        inputDelimiter == null
            ? FileDataSplitter.CSV
            : FileDataSplitter.getFileDataSplitter(inputDelimiter);
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(delimiter, false, geometryType);
    return formatUtils.readGeometry(geom);
  }

  public static Geometry getGeometryByFileData(String wktString, FileDataSplitter dataSplitter)
      throws ParseException {
    FormatUtils<Geometry> formatUtils = new FormatUtils<>(dataSplitter, false);
    return formatUtils.readGeometry(wktString);
  }
}
