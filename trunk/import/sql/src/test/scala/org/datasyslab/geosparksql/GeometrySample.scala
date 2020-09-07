package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader
import org.apache.spark.sql.{Dataset, Row}

import scala.tools.nsc.interpreter.InputStream


trait GeometrySample {
  self: functionTestScala =>
  val wktReader=new WKTReader()

  import sparkSession.implicits._

  def createSimplePolygons(polygonCount: Int, columnNames: String*): Dataset[Row] =
    sampleSimplePolygons.take(polygonCount).map(mapToSpark).toDF(columnNames:_*)

  def createSamplePolygonDf(polygonCount: Int, columnNames: String*): Dataset[Row] =
    samplePolygons.take(polygonCount).map(mapToSpark).toDF(columnNames: _*)

  def createSamplePointDf(pointCount: Int, columnNames: String*): Dataset[Row] =
    samplePoints.take(pointCount).map(mapToSpark).toDF(columnNames:_*)

  def createSampleLineStringsDf(lineCount: Int, columnNames: String*): Dataset[Row] =
    sampleLines.take(lineCount).map(mapToSpark).toDF(columnNames:_*)

  val samplePoints: List[Geometry] =
    loadGeometriesFromResources("/python/samplePoints")

  private val sampleSimplePolygons: List[Geometry] =
    loadGeometriesFromResources("/python/simplePolygons")

  private val sampleLines: List[Geometry] =
    loadGeometriesFromResources("/python/sampleLines")

  private val samplePolygons: List[Geometry] =
    loadGeometriesFromResources("/python/samplePolygons")

  private def loadGeometriesFromResources(fileName: String): List[Geometry] = {
    val resourceFileText = loadResourceFile(fileName)
    loadFromWktStrings(resourceFileText)
  }

  private def loadFromWktStrings(geometries: List[String]): List[Geometry] = {
    geometries.map(
      geometryWKT => wktReader.read(geometryWKT)
    )
  }

  private def mapToSpark(geometry: Geometry): Tuple1[Geometry] =
    Tuple1(geometry)

  private def loadResourceFile(fileName: String): List[String] = {
    val stream: InputStream = getClass.getResourceAsStream(fileName)
    val lines: Iterator[String] = scala.io.Source.fromInputStream( stream ).getLines
    lines.toList
  }
}

