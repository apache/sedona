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
import java.util.Locale;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SedonaConf implements Serializable {
  static final Logger logger = LoggerFactory.getLogger(SedonaConf.class);

  /**
   * CRS transformation mode for ST_Transform function.
   *
   * <ul>
   *   <li>{@link #NONE} - Use proj4sedona for all CRS transformations (raster not yet supported)
   *   <li>{@link #RASTER} - Use proj4sedona for vector, GeoTools for raster (default)
   *   <li>{@link #ALL} - Use GeoTools for all CRS transformations (legacy behavior)
   * </ul>
   *
   * @since 1.9.0
   */
  public enum CRSTransformMode {
    /** Use proj4sedona for all transformations. Raster transform not yet supported. */
    NONE,
    /** Use proj4sedona for vector, GeoTools for raster (default). */
    RASTER,
    /** Use GeoTools for all transformations (legacy behavior). */
    ALL;

    public static CRSTransformMode fromString(String value) {
      if (value == null || value.isEmpty()) {
        return RASTER; // default
      }
      try {
        return valueOf(value.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        logger.warn(
            "Invalid value '{}' for spark.sedona.crs.geotools, using default 'raster'", value);
        return RASTER;
      }
    }

    /** Returns true if GeoTools should be used for vector CRS transformations. */
    public boolean useGeoToolsForVector() {
      return this == ALL;
    }

    /** Returns true if GeoTools should be used for raster CRS transformations. */
    public boolean useGeoToolsForRaster() {
      return this == RASTER || this == ALL;
    }
  }

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

  // Parameter for CRS transformation mode
  private CRSTransformMode crsTransformMode;

  // Parameters for URL-based CRS provider
  private String crsUrlBase;
  private String crsUrlPathTemplate;
  private String crsUrlFormat;

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

    // CRS transformation mode configuration
    // - "none": Use proj4sedona for all transformations (raster not yet supported)
    // - "raster" (default): Use proj4sedona for vector, GeoTools for raster
    // - "all": Use GeoTools for all transformations (legacy behavior)
    this.crsTransformMode =
        CRSTransformMode.fromString(confGetter.get("spark.sedona.crs.geotools", "raster"));

    // URL-based CRS provider configuration
    // When spark.sedona.crs.url.base is set, a UrlCRSProvider is registered to resolve
    // SRID definitions from the given HTTP(S) endpoint before falling back to built-in defs.
    this.crsUrlBase = confGetter.get("spark.sedona.crs.url.base", "");
    this.crsUrlPathTemplate =
        confGetter.get("spark.sedona.crs.url.pathTemplate", "/{authority}/{code}.json");
    this.crsUrlFormat = confGetter.get("spark.sedona.crs.url.format", "projjson");
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

  /**
   * Get the CRS transformation mode for ST_Transform.
   *
   * @return The CRS transformation mode
   * @since 1.9.0
   */
  public CRSTransformMode getCRSTransformMode() {
    return crsTransformMode;
  }

  /**
   * Get the base URL for the URL-based CRS provider. When non-empty, a {@code UrlCRSProvider} is
   * registered to resolve SRID definitions from this HTTP(S) endpoint.
   *
   * @return The base URL, or empty string if disabled
   * @since 1.9.0
   */
  public String getCrsUrlBase() {
    return crsUrlBase;
  }

  /**
   * Get the path template for the URL-based CRS provider. Supports placeholders: {@code
   * {authority}} and {@code {code}}.
   *
   * @return The path template (default: "/{authority}/{code}.json")
   * @since 1.9.0
   */
  public String getCrsUrlPathTemplate() {
    return crsUrlPathTemplate;
  }

  /**
   * Get the expected response format for the URL-based CRS provider.
   *
   * @return The format string: "projjson", "proj", "wkt1", or "wkt2" (default: "projjson")
   * @since 1.9.0
   */
  public String getCrsUrlFormat() {
    return crsUrlFormat;
  }
}
