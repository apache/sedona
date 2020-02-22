package org.apache.spark.sql.geosparksql

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.Column


object functions {

  def ST_GeomFromText(wkt: Column): Column=
    expr(s"St_GeomFromWKT(${wkt.toString})")

  def ST_PointFromText(wkt: Column): Column =
    expr(s"ST_PointFromText(${wkt.toString})")

  def ST_PolygonFromText(wkt: Column): Column =
    expr(s"ST_PolygonFromText(${wkt.toString})")

  def ST_LineStringFromText(wkt: Column): Column =
    expr(s"ST_LineStringFromText(${wkt.toString})")

  def ST_GeomFromWKT(wkt: Column): Column =
    expr(s"ST_GeomFromWKT(${wkt.toString})")

  def ST_GeomFromWKB(wkb: Column): Column =
    expr(s"ST_GeomFromWKB(${wkb.toString})")

  def ST_GeomFromGeoJSON(geojson: Column): Column =
    expr(s"ST_GeomFromGeoJSON(${geojson.toString})")

  def ST_Point(wkt: Column): Column =
    expr(s"ST_Point(${wkt.toString})")

  def ST_PolygonFromEnvelope(geom: Column): Column =
    expr(s"ST_Point(${geom.toString})")
}
