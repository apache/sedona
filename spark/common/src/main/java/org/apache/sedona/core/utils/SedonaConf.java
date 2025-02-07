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
package org.apache.sedona.core.utils;

import java.io.Serializable;
import java.lang.reflect.Field;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.JoinSpartitionDominantSide;
import org.apache.sedona.core.enums.SpatialJoinOptimizationMode;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Envelope;

public class SedonaConf implements Serializable {

  // Global parameters of Sedona. All these parameters can be initialized through SparkConf.

  private boolean useIndex;

  private IndexType indexType;

  // Parameters for JoinQuery including RangeJoin and DistanceJoin

  private JoinSpartitionDominantSide joinSpartitionDominantSide;

  private JoinBuildSide joinBuildSide;

  private long joinApproximateTotalCount;

  private Envelope datasetBoundary;

  private int fallbackPartitionNum;

  private GridType joinGridType;

  private long autoBroadcastJoinThreshold;

  private SpatialJoinOptimizationMode spatialJoinOptimizationMode;

  // Parameters for knn joins
  private boolean includeTieBreakersInKNNJoins = false;

  // Parameters for geostats
  private Boolean DBSCANIncludeOutliers = true;

  public static SedonaConf fromActiveSession() {
    return new SedonaConf(SparkSession.active().conf());
  }

  public SedonaConf(RuntimeConfig runtimeConfig) {
    this.useIndex = Boolean.parseBoolean(getConfigValue(runtimeConfig, "global.index", "true"));
    this.indexType =
        IndexType.getIndexType(getConfigValue(runtimeConfig, "global.indextype", "rtree"));
    this.joinApproximateTotalCount =
        Long.parseLong(getConfigValue(runtimeConfig, "join.approxcount", "-1"));
    String[] boundaryString = getConfigValue(runtimeConfig, "join.boundary", "0,0,0,0").split(",");
    this.datasetBoundary =
        new Envelope(
            Double.parseDouble(boundaryString[0]),
            Double.parseDouble(boundaryString[1]),
            Double.parseDouble(boundaryString[2]),
            Double.parseDouble(boundaryString[3]));
    this.joinGridType =
        GridType.getGridType(getConfigValue(runtimeConfig, "join.gridtype", "kdbtree"));
    this.joinBuildSide =
        JoinBuildSide.getBuildSide(getConfigValue(runtimeConfig, "join.indexbuildside", "left"));
    this.joinSpartitionDominantSide =
        JoinSpartitionDominantSide.getJoinSpartitionDominantSide(
            getConfigValue(runtimeConfig, "join.spatitionside", "left"));
    this.fallbackPartitionNum =
        Integer.parseInt(getConfigValue(runtimeConfig, "join.numpartition", "-1"));
    this.autoBroadcastJoinThreshold =
        bytesFromString(
            getConfigValue(
                runtimeConfig,
                "join.autoBroadcastJoinThreshold",
                runtimeConfig.get("spark.sql.autoBroadcastJoinThreshold")));
    this.spatialJoinOptimizationMode =
        SpatialJoinOptimizationMode.getSpatialJoinOptimizationMode(
            getConfigValue(runtimeConfig, "join.optimizationmode", "nonequi"));

    // Parameters for knn joins
    this.includeTieBreakersInKNNJoins =
        Boolean.parseBoolean(getConfigValue(runtimeConfig, "join.knn.includeTieBreakers", "false"));

    // Parameters for geostats
    this.DBSCANIncludeOutliers =
        Boolean.parseBoolean(runtimeConfig.get("spark.sedona.dbscan.includeOutliers", "true"));
  }

  // Helper method to prioritize `sedona.*` over `spark.sedona.*`
  private String getConfigValue(
      RuntimeConfig runtimeConfig, String keySuffix, String defaultValue) {
    String sedonaKey = "sedona." + keySuffix;
    String sparkSedonaKey = "spark.sedona." + keySuffix;

    if (runtimeConfig.contains(sedonaKey)) {
      return runtimeConfig.get(sedonaKey, defaultValue);
    } else {
      return runtimeConfig.get(sparkSedonaKey, defaultValue);
    }
  }

  public boolean getUseIndex() {
    return useIndex;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public long getJoinApproximateTotalCount() {
    return joinApproximateTotalCount;
  }

  public Envelope getDatasetBoundary() {
    return datasetBoundary;
  }

  public JoinBuildSide getJoinBuildSide() {
    return joinBuildSide;
  }

  public GridType getJoinGridType() {
    return joinGridType;
  }

  public JoinSpartitionDominantSide getJoinSpartitionDominantSide() {
    return joinSpartitionDominantSide;
  }

  public int getFallbackPartitionNum() {
    return fallbackPartitionNum;
  }

  public long getAutoBroadcastJoinThreshold() {
    return autoBroadcastJoinThreshold;
  }

  public boolean isIncludeTieBreakersInKNNJoins() {
    return includeTieBreakersInKNNJoins;
  }

  public String toString() {
    try {
      String sb = "";
      Class<?> objClass = this.getClass();
      sb += "Sedona Configuration:\n";
      Field[] fields = objClass.getDeclaredFields();
      for (Field field : fields) {
        String name = field.getName();
        Object value = field.get(this);
        sb += name + ": " + value.toString() + "\n";
      }
      return sb;
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  static long bytesFromString(String str) {
    if (str.startsWith("-")) {
      return -1 * Utils.byteStringAsBytes(str.substring(1));
    } else {
      return Utils.byteStringAsBytes(str);
    }
  }

  public SpatialJoinOptimizationMode getSpatialJoinOptimizationMode() {
    return spatialJoinOptimizationMode;
  }

  public Boolean getDBSCANIncludeOutliers() {
    return DBSCANIncludeOutliers;
  }
}
