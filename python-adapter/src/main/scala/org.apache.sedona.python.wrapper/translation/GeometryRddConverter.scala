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

package org.apache.sedona.python.wrapper.translation

import org.apache.sedona.python.wrapper.utils.implicits._
import org.apache.spark.api.java.JavaRDD
import org.locationtech.jts.geom.Geometry

private[python] case class GeometryRddConverter[T <: Geometry](spatialRDD: JavaRDD[T],
                                                               geometrySerializer: PythonGeometrySerializer) extends RDDToPythonConverter {

  override def translateToPython: JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](geom => {
      val typeBuffer = 0.toByteArray()
      val sizeBuffer = 0.toByteArray()
      typeBuffer ++ geometrySerializer.serialize(geom) ++ sizeBuffer
    }).toJavaRDD()
  }

}
