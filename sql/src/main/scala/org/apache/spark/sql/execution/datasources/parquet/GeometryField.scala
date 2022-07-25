package org.apache.spark.sql.execution.datasources.parquet

object GeometryField {

  var fieldGeometry: String = ""

  def getFieldGeometry(): String ={
    fieldGeometry
  }

  def setFieldGeometry(fieldName: String): Unit ={
    fieldGeometry = fieldName
  }
}