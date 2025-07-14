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
import java.nio.file.Paths;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.JoinSpartitionDominantSide;
import org.apache.sedona.core.enums.SpatialJoinOptimizationMode;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
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

  // Parameters for libpostal integration
  private String libPostalDataDir;
  private Boolean libPostalUseSenzing = false;

  public static SedonaConf fromActiveSession() {
    return new SedonaConf(SparkSession.active().conf());
  }

  public static SedonaConf fromSparkEnv() {
    return new SedonaConf(SparkEnv.get().conf());
  }

  public SedonaConf(SparkConf sparkConf) {
    this(
        new ConfGetter() {
          @Override
          public String get(String key, String defaultValue) {
            return sparkConf.get(key, defaultValue);
          }

          @Override
          public String get(String key) {
            return sparkConf.get(key, null);
          }

          public boolean contains(String key) {
            return sparkConf.contains(key);
          }
        });
  }

  public SedonaConf(RuntimeConfig runtimeConfig) {
    this(
        new ConfGetter() {
          @Override
          public String get(String key, String defaultValue) {
            return runtimeConfig.get(key, defaultValue);
          }

          @Override
          public String get(String key) {
            return runtimeConfig.get(key);
          }

          public boolean contains(String key) {
            return runtimeConfig.contains(key);
          }
        });
  }

  private interface ConfGetter {
    String get(String key, String defaultValue);

    String get(String key);

    boolean contains(String key);
  }

  private SedonaConf(ConfGetter confGetter) {
    this.useIndex = Boolean.parseBoolean(getConfigValue(confGetter, "global.index", "true"));
    this.indexType =
        IndexType.getIndexType(getConfigValue(confGetter, "global.indextype", "rtree"));
    this.joinApproximateTotalCount =
        Long.parseLong(getConfigValue(confGetter, "join.approxcount", "-1"));
    String[] boundaryString = getConfigValue(confGetter, "join.boundary", "0,0,0,0").split(",");
    this.datasetBoundary =
        new Envelope(
            Double.parseDouble(boundaryString[0]),
            Double.parseDouble(boundaryString[1]),
            Double.parseDouble(boundaryString[2]),
            Double.parseDouble(boundaryString[3]));
    this.joinGridType =
        GridType.getGridType(getConfigValue(confGetter, "join.gridtype", "kdbtree"));
    this.joinBuildSide =
        JoinBuildSide.getBuildSide(getConfigValue(confGetter, "join.indexbuildside", "left"));
    this.joinSpartitionDominantSide =
        JoinSpartitionDominantSide.getJoinSpartitionDominantSide(
            getConfigValue(confGetter, "join.spatitionside", "left"));
    this.fallbackPartitionNum =
        Integer.parseInt(getConfigValue(confGetter, "join.numpartition", "-1"));
    this.autoBroadcastJoinThreshold =
        bytesFromString(
            getConfigValue(
                confGetter,
                "join.autoBroadcastJoinThreshold",
                confGetter.get("spark.sql.autoBroadcastJoinThreshold")));
    this.spatialJoinOptimizationMode =
        SpatialJoinOptimizationMode.getSpatialJoinOptimizationMode(
            getConfigValue(confGetter, "join.optimizationmode", "nonequi"));

    // Parameters for knn joins
    this.includeTieBreakersInKNNJoins =
        Boolean.parseBoolean(getConfigValue(confGetter, "join.knn.includeTieBreakers", "false"));

    // Parameters for geostats
    this.DBSCANIncludeOutliers =
        Boolean.parseBoolean(confGetter.get("spark.sedona.dbscan.includeOutliers", "true"));

    // Parameters for libpostal integration
    String libPostalDataDir =
        confGetter.get(
            "spark.sedona.libpostal.dataDir",
            Paths.get(System.getProperty("java.io.tmpdir"))
                .resolve(Paths.get("libpostal"))
                .toString());
    if (!libPostalDataDir.isEmpty() && !libPostalDataDir.endsWith("/")) {
      libPostalDataDir = libPostalDataDir + "/";
    }
    this.libPostalDataDir = libPostalDataDir;

    this.libPostalUseSenzing =
        Boolean.parseBoolean(confGetter.get("spark.sedona.libpostal.useSenzing", "true"));
  }

  // Helper method to prioritize `sedona.*` over `spark.sedona.*`
  private String getConfigValue(ConfGetter confGetter, String keySuffix, String defaultValue) {
    String sedonaKey = "sedona." + keySuffix;
    String sparkSedonaKey = "spark.sedona." + keySuffix;

    if (confGetter.contains(sedonaKey)) {
      return confGetter.get(sedonaKey, defaultValue);
    } else {
      return confGetter.get(sparkSedonaKey, defaultValue);
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
    if (str == null || str.isEmpty()) {
      return 0;
    }
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

  public String getLibPostalDataDir() {
    return libPostalDataDir;
  }

  public Boolean getLibPostalUseSenzing() {
    return libPostalUseSenzing;
  }
}
