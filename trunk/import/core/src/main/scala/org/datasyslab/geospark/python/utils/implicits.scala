package org.datasyslab.geospark.python.utils

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.util

import com.vividsolutions.jts.geom.Geometry

object implicits {

  implicit class IntImplicit(value: Int){
    def toByteArray(byteOrder: ByteOrder = ByteOrder.LITTLE_ENDIAN): Array[Byte] = {
      val typeBuffer = ByteBuffer
        .allocate(4)
        .order(byteOrder)
      typeBuffer.putInt(value)
      typeBuffer.array()
    }
  }

  implicit class DoubleImplicit(value: Double){
    def toByteArray(byteOrder: ByteOrder = ByteOrder.LITTLE_ENDIAN): Array[Byte] = {
      val typeBuffer = ByteBuffer
        .allocate(8)
        .order(byteOrder)
      typeBuffer.putDouble(value)
      typeBuffer.array()
    }
  }

  implicit class GeometryEnhancer(geometry: Geometry){
    def userDataToUtf8ByteArray: Array[Byte] =
      geometry.getUserData.asInstanceOf[String]
        .getBytes(StandardCharsets.UTF_8)
  }

  implicit class ListConverter[T](elements: List[T]){
    def toJavaHashSet: java.util.HashSet[T] = {
      val javaHashSet = new util.HashSet[T]()
      elements.foreach(javaHashSet.add)
      javaHashSet
    }
  }

}
