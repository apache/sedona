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

package org.apache.sedona.core.formatMapper;

import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;

class RddReader
{
    public static SpatialRDD<Geometry> createSpatialRDD(JavaRDD rawTextRDD, FormatMapper<Geometry> formatMapper)
    {
        SpatialRDD spatialRDD = new SpatialRDD<Geometry>();
        spatialRDD.rawSpatialRDD = rawTextRDD.mapPartitions(formatMapper);
        spatialRDD.fieldNames = formatMapper.readPropertyNames(rawTextRDD.take(1).get(0).toString());
        return spatialRDD;
    }
}
