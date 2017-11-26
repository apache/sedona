/**
  * FILE: UdfRegistrator
  * PATH: org.datasyslab.geosparksql.UDF.UdfRegistrator
  * Copyright (c) GeoSpark Development Team
  *
  * MIT License
  *
  * Permission is hereby granted, free of charge, to any person obtaining a copy
  * of this software and associated documentation files (the "Software"), to deal
  * in the Software without restriction, including without limitation the rights
  * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  * copies of the Software, and to permit persons to whom the Software is
  * furnished to do so, subject to the following conditions:
  *
  * The above copyright notice and this permission notice shall be included in all
  * copies or substantial portions of the Software.
  *
  * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  * SOFTWARE.
  */
package org.datasyslab.geosparksql.UDF

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.geosparksql.GeometryWrapper
import org.datasyslab.geospark.enums.{FileDataSplitter, GeometryType}
import org.datasyslab.geospark.formatMapper.FormatMapper

class UdfRegistrator {
  def resigterFormatMapper(sparkSession: SparkSession): Unit =
  {
    sparkSession.udf.register("ST_Point", (x: Double, y:Double)=>{
      val geometryFactory = new GeometryFactory()
      new GeometryWrapper(geometryFactory.createPoint(new Coordinate(x,y)))
    })
    sparkSession.udf.register("ST_PointWithId", (x: Double, y:Double, uniqueId: String)=>{
      val geometryFactory = new GeometryFactory()
      val geometry = geometryFactory.createPoint(new Coordinate(x,y))
      geometry.setUserData(uniqueId)
      new GeometryWrapper(geometry)
    })
    sparkSession.udf.register("ST_GeomFromText", (geomString: String, fileFormat:String)=>{
      new GeometryWrapper(geomString,FileDataSplitter.getFileDataSplitter(fileFormat))
    })
    sparkSession.udf.register("ST_GeomFromTextToType", (geomString: String, fileFormat:String, geometryType: String)=>{
      new GeometryWrapper(geomString,FileDataSplitter.getFileDataSplitter(fileFormat),GeometryType.getGeometryType(geometryType))
    })
    sparkSession.udf.register("ST_GeomFromTextWithId", (geomString: String, fileFormat:String, uniqueId: String)=>{
      var fileDataSplitter = FileDataSplitter.getFileDataSplitter(fileFormat)
      var formatMapper = new FormatMapper(fileDataSplitter, false)
      var geometry = formatMapper.readGeometry(geomString)
      geometry.setUserData(uniqueId)
      new GeometryWrapper(geometry)
    })
    sparkSession.udf.register("ST_GeomFromTextToTypeWithId", (geomString: String, fileFormat:String, geometryType: String, uniqueId: String)=>{
      var formatMapper = new FormatMapper(FileDataSplitter.getFileDataSplitter(fileFormat), false, GeometryType.getGeometryType(geometryType))
      var geometry = formatMapper.readGeometry(geomString)
      geometry.setUserData(uniqueId)
      new GeometryWrapper(geometry)
    })
  }

  def registerAll(sparkSession: SparkSession): Unit =
  {
    resigterFormatMapper(sparkSession)
  }
}
