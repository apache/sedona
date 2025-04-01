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
package org.apache.spark.sql.sedona_sql.expressions

import org.apache.sedona.sql.utils.GeometrySerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, GeometryFactory, Point}
import org.apache.sedona.common.geometryObjects.Geography

object implicits {

  implicit class InputExpressionEnhancer(inputExpression: Expression) {
    def toGeometry(input: InternalRow): Geometry = {
      if (inputExpression.isInstanceOf[SerdeAware]) {
        inputExpression
          .asInstanceOf[SerdeAware]
          .evalWithoutSerialization(input)
          .asInstanceOf[Geometry]
      } else {
        inputExpression.eval(input).asInstanceOf[Array[Byte]] match {
          case binary: Array[Byte] => GeometrySerializer.deserialize(binary)
          case _ => null
        }
      }
    }

    def toGeometryArray(input: InternalRow): Array[Geometry] = {
      inputExpression match {
        case aware: SerdeAware =>
          aware.evalWithoutSerialization(input).asInstanceOf[Array[Geometry]]
        case _ =>
          inputExpression.eval(input).asInstanceOf[ArrayData] match {
            case arrayData: ArrayData =>
              val length = arrayData.numElements()
              val geometries = new Array[Geometry](length)
              for (i <- 0 until length) {
                geometries(i) = arrayData.getBinary(i).toGeometry
              }
              geometries
            case _ => null
          }
      }
    }

    def toGeography(input: InternalRow): Geography = {
      inputExpression match {
        case serdeAware: SerdeAware =>
          serdeAware.evalWithoutSerialization(input).asInstanceOf[Geography]
        case _ =>
          inputExpression.eval(input).asInstanceOf[Array[Byte]] match {
            case binary: Array[Byte] => new Geography(GeometrySerializer.deserialize(binary))
            case _ => null
          }
      }
    }

    def toDoubleList(input: InternalRow): java.util.List[java.lang.Double] = {
      inputExpression match {
        case aware: SerdeAware =>
          aware.evalWithoutSerialization(input).asInstanceOf[java.util.List[java.lang.Double]]
        case _ =>
          inputExpression.eval(input).asInstanceOf[ArrayData] match {
            case arrayData: ArrayData =>
              val length = arrayData.numElements()
              val doubleList = new java.util.ArrayList[java.lang.Double]()
              for (i <- 0 until length) {
                doubleList.add(arrayData.getDouble(i))
              }
              doubleList.asInstanceOf[java.util.List[java.lang.Double]]
            case _ => null
          }
      }
    }

    def toGeometryList(input: InternalRow): java.util.List[Geometry] = {
      inputExpression match {
        case aware: SerdeAware =>
          aware.evalWithoutSerialization(input).asInstanceOf[java.util.List[Geometry]]
        case _ =>
          inputExpression.eval(input).asInstanceOf[ArrayData] match {
            case arrayData: ArrayData =>
              val length = arrayData.numElements()
              val geometries = new java.util.ArrayList[Geometry]()
              for (i <- 0 until length) {
                geometries.add(arrayData.getBinary(i).toGeometry)
              }
              geometries.asInstanceOf[java.util.List[Geometry]]
            case _ => null
          }
      }
    }

    def toInt(input: InternalRow): Int = {
      inputExpression.eval(input).asInstanceOf[Int]
    }

    def asString(input: InternalRow): String = {
      inputExpression.eval(input).asInstanceOf[UTF8String] match {
        case s: UTF8String => s.toString
        case _ => null
      }
    }
  }

  implicit class SequenceEnhancer[T](seq: Seq[T]) {
    def validateLength(length: Int, message: Option[String] = None): Unit = {
      message match {
        case None => assert(length == seq.length, s"Expression should be $length long")
        case Some(x) => assert(length == seq.length, message)
      }
    }

    def betweenLength(a: Int, b: Int): Unit = {
      val length = seq.length
      assert(length >= a && length <= b)
    }
  }

  implicit class ArrayDataEnhancer(arrayData: Array[Byte]) {
    def toGeometry: Geometry = {
      arrayData match {
        case binary: Array[Byte] => GeometrySerializer.deserialize(binary)
        case _ => null
      }
    }
  }

  implicit class GeometryEnhancer(geom: Geometry) {
    private val geometryFactory = new GeometryFactory()

    def toGenericArrayData: Array[Byte] = GeometrySerializer.serialize(geom)

    def getPoints: Array[Point] =
      geom.getCoordinates.map(coordinate => geometryFactory.createPoint(coordinate))

    def isNonEmpty: Boolean = geom != null && !geom.isEmpty
  }

  implicit class GeographyEnhancer(geog: Geography) {

    def toGenericArrayData: Array[Byte] = GeometrySerializer.serialize(geog.getGeometry)
  }
}
