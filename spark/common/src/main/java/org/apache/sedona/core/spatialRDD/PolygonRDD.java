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
package org.apache.sedona.core.spatialRDD;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.formatMapper.PolygonFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

// TODO: Auto-generated Javadoc

/** The Class PolygonRDD. */
public class PolygonRDD extends SpatialRDD<Polygon> {
  /** Instantiates a new polygon RDD. */
  public PolygonRDD() {}

  /**
   * Instantiates a new polygon RDD.
   *
   * @param rawSpatialRDD the raw spatial RDD
   */
  public PolygonRDD(JavaRDD<Polygon> rawSpatialRDD) {
    this.setRawSpatialRDD(rawSpatialRDD);
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   */
  public PolygonRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer startOffset,
      Integer endOffset,
      FileDataSplitter splitter,
      boolean carryInputData) {
    this(sparkContext, InputLocation, startOffset, endOffset, splitter, carryInputData, null);
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param splitter the splitter
   * @param carryInputData the carry input data
   * @param partitions the partitions
   */
  public PolygonRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      FileDataSplitter splitter,
      boolean carryInputData,
      Integer partitions) {
    this(sparkContext, InputLocation, null, null, splitter, carryInputData, partitions);
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param splitter the splitter
   * @param carryInputData the carry input data
   */
  public PolygonRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      FileDataSplitter splitter,
      boolean carryInputData) {
    this(sparkContext, InputLocation, null, null, splitter, carryInputData, null);
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param partitions the partitions
   * @param userSuppliedMapper the user supplied mapper
   */
  public PolygonRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer partitions,
      FlatMapFunction userSuppliedMapper) {
    this.setRawSpatialRDD(
        sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param userSuppliedMapper the user supplied mapper
   */
  public PolygonRDD(
      JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
    this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
  }

  /**
   * Instantiates a new polygon RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param startOffset the start offset
   * @param endOffset the end offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   * @param partitions the partitions
   */
  public PolygonRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer startOffset,
      Integer endOffset,
      FileDataSplitter splitter,
      boolean carryInputData,
      Integer partitions) {
    JavaRDD rawTextRDD =
        partitions != null
            ? sparkContext.textFile(InputLocation, partitions)
            : sparkContext.textFile(InputLocation);
    if (startOffset != null && endOffset != null) {
      this.setRawSpatialRDD(
          rawTextRDD.mapPartitions(
              new PolygonFormatMapper(startOffset, endOffset, splitter, carryInputData)));
    } else {
      this.setRawSpatialRDD(
          rawTextRDD.mapPartitions(new PolygonFormatMapper(splitter, carryInputData)));
    }
    if (splitter.equals(FileDataSplitter.GEOJSON)) {
      this.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString());
    }
    this.analyze();
  }

  /**
   * Polygon union.
   *
   * @return the polygon
   */
  public Polygon PolygonUnion() {
    Polygon result =
        this.rawSpatialRDD.reduce(
            new Function2<Polygon, Polygon, Polygon>() {
              public Polygon call(Polygon v1, Polygon v2) {
                // Reduce precision in JTS to avoid TopologyException
                PrecisionModel pModel = new PrecisionModel();
                GeometryPrecisionReducer pReducer = new GeometryPrecisionReducer(pModel);
                Geometry p1 = pReducer.reduce(v1);
                Geometry p2 = pReducer.reduce(v2);
                // Union two polygons
                Geometry polygonGeom = p1.union(p2);
                Coordinate[] coordinates = polygonGeom.getCoordinates();
                ArrayList<Coordinate> coordinateList =
                    new ArrayList<Coordinate>(Arrays.asList(coordinates));
                Coordinate lastCoordinate = coordinateList.get(0);
                coordinateList.add(lastCoordinate);
                Coordinate[] coordinatesClosed = new Coordinate[coordinateList.size()];
                coordinatesClosed = coordinateList.toArray(coordinatesClosed);
                GeometryFactory fact = new GeometryFactory();
                LinearRing linear = new GeometryFactory().createLinearRing(coordinatesClosed);
                Polygon polygon = new Polygon(linear, null, fact);
                // Return the two polygon union result
                return polygon;
              }
            });
    return result;
  }
}
