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

package org.apache.sedona.python.wrapper.adapters

import org.apache.sedona.python.wrapper.translation.{
  FlatPairRddConverter,
  GeometryRddConverter,
  GeometrySeqToPythonConverter,
  ListPairRddConverter,
  PythonRDDToJavaConverter
}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.locationtech.jts.geom.Geometry

import scala.jdk.CollectionConverters._

object PythonConverter extends GeomSerializer {

  def translateSpatialRDDToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] =
    GeometryRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translateSpatialPairRDDToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] =
    FlatPairRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translateSpatialPairRDDWithListToPython(spatialRDD: JavaPairRDD[Geometry, java.util.List[Geometry]]): JavaRDD[Array[Byte]] =
    ListPairRddConverter(spatialRDD, geometrySerializer).translateToPython

  def translatePythonRDDToJava(pythonRDD: JavaRDD[Array[Byte]]): JavaRDD[Geometry] =
    PythonRDDToJavaConverter(pythonRDD, geometrySerializer).translateToJava

  def translateGeometrySeqToPython(spatialData: java.util.List[Geometry]): Array[Array[Byte]] =
    GeometrySeqToPythonConverter(spatialData.asScala.toSeq, geometrySerializer).translateToPython
}
