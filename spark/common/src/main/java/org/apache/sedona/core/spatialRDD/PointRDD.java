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

import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.formatMapper.PointFormatMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Point;

// TODO: Auto-generated Javadoc

/** The Class PointRDD. */
public class PointRDD extends SpatialRDD<Point> {

  /** Instantiates a new point RDD. */
  public PointRDD() {}

  /**
   * Instantiates a new point RDD.
   *
   * @param rawSpatialRDD the raw spatial RDD
   */
  public PointRDD(JavaRDD<Point> rawSpatialRDD) {
    this.setRawSpatialRDD(rawSpatialRDD);
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param Offset the offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   */
  public PointRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer Offset,
      FileDataSplitter splitter,
      boolean carryInputData) {
    this(sparkContext, InputLocation, Offset, splitter, carryInputData, null);
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param splitter the splitter
   * @param carryInputData the carry input data
   * @param partitions the partitions
   */
  public PointRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      FileDataSplitter splitter,
      boolean carryInputData,
      Integer partitions) {
    this(sparkContext, InputLocation, null, splitter, carryInputData, partitions);
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param splitter the splitter
   * @param carryInputData the carry input data
   */
  public PointRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      FileDataSplitter splitter,
      boolean carryInputData) {
    this(sparkContext, InputLocation, null, splitter, carryInputData, null);
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param partitions the partitions
   * @param userSuppliedMapper the user supplied mapper
   */
  public PointRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer partitions,
      FlatMapFunction userSuppliedMapper) {
    this.setRawSpatialRDD(
        sparkContext.textFile(InputLocation, partitions).mapPartitions(userSuppliedMapper));
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param userSuppliedMapper the user supplied mapper
   */
  public PointRDD(
      JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
    this.setRawSpatialRDD(sparkContext.textFile(InputLocation).mapPartitions(userSuppliedMapper));
  }

  /**
   * Instantiates a new point RDD.
   *
   * @param sparkContext the spark context
   * @param InputLocation the input location
   * @param Offset the offset
   * @param splitter the splitter
   * @param carryInputData the carry input data
   * @param partitions the partitions
   */
  public PointRDD(
      JavaSparkContext sparkContext,
      String InputLocation,
      Integer Offset,
      FileDataSplitter splitter,
      boolean carryInputData,
      Integer partitions) {
    JavaRDD rawTextRDD =
        partitions != null
            ? sparkContext.textFile(InputLocation, partitions)
            : sparkContext.textFile(InputLocation);
    if (Offset != null) {
      this.setRawSpatialRDD(
          rawTextRDD.mapPartitions(new PointFormatMapper(Offset, splitter, carryInputData)));
    } else {
      this.setRawSpatialRDD(
          rawTextRDD.mapPartitions(new PointFormatMapper(splitter, carryInputData)));
    }
    if (splitter.equals(FileDataSplitter.GEOJSON)) {
      this.fieldNames = FormatMapper.readGeoJsonPropertyNames(rawTextRDD.take(1).get(0).toString());
    }
    this.analyze();
  }
}
