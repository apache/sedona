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

import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.enums.JoinBuildSide;
import org.apache.sedona.core.enums.JoinSparitionDominantSide;
import org.apache.sedona.core.enums.SpatialJoinOptimizationMode;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.Utils;
import org.locationtech.jts.geom.Envelope;

import java.io.Serializable;
import java.lang.reflect.Field;

public class SedonaConf
        implements Serializable
{

    // Global parameters of Sedona. All these parameters can be initialized through SparkConf.

    private boolean useIndex;

    private IndexType indexType;

    // Parameters for JoinQuery including RangeJoin and DistanceJoin

    private JoinSparitionDominantSide joinSparitionDominantSide;

    private JoinBuildSide joinBuildSide;

    private long joinApproximateTotalCount;

    private Envelope datasetBoundary;

    private int fallbackPartitionNum;

    private GridType joinGridType;

    private long autoBroadcastJoinThreshold;

    private SpatialJoinOptimizationMode spatialJoinOptimizationMode;

    public static SedonaConf fromActiveSession() {
        return new SedonaConf(SparkSession.active().conf());
    }

    public SedonaConf(RuntimeConfig runtimeConfig)
    {
        this.useIndex = Boolean.parseBoolean(runtimeConfig.get("sedona.global.index", "true"));
        this.indexType = IndexType.getIndexType(runtimeConfig.get("sedona.global.indextype", "quadtree"));
        this.joinApproximateTotalCount = Long.parseLong(runtimeConfig.get("sedona.join.approxcount", "-1"));
        String[] boundaryString = runtimeConfig.get("sedona.join.boundary", "0,0,0,0").split(",");
        this.datasetBoundary = new Envelope(Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[1]),
                Double.parseDouble(boundaryString[2]), Double.parseDouble(boundaryString[3]));
        this.joinGridType = GridType.getGridType(runtimeConfig.get("sedona.join.gridtype", "kdbtree"));
        this.joinBuildSide = JoinBuildSide.getBuildSide(runtimeConfig.get("sedona.join.indexbuildside", "left"));
        this.joinSparitionDominantSide = JoinSparitionDominantSide.getJoinSparitionDominantSide(runtimeConfig.get("sedona.join.spatitionside", "left"));
        this.fallbackPartitionNum = Integer.parseInt(runtimeConfig.get("sedona.join.numpartition", "-1"));
        this.autoBroadcastJoinThreshold = bytesFromString(
                runtimeConfig.get("sedona.join.autoBroadcastJoinThreshold",
                        runtimeConfig.get("spark.sql.autoBroadcastJoinThreshold")
                )
        );
        this.spatialJoinOptimizationMode = SpatialJoinOptimizationMode.getSpatialJoinOptimizationMode(
                runtimeConfig.get("sedona.join.optimizationmode", "nonequi"));
    }

    public boolean getUseIndex()
    {
        return useIndex;
    }

    public IndexType getIndexType()
    {
        return indexType;
    }


    public long getJoinApproximateTotalCount()
    {
        return joinApproximateTotalCount;
    }

    public Envelope getDatasetBoundary()
    {
        return datasetBoundary;
    }

    public JoinBuildSide getJoinBuildSide()
    {
        return joinBuildSide;
    }

    public GridType getJoinGridType()
    {
        return joinGridType;
    }

    public JoinSparitionDominantSide getJoinSparitionDominantSide()
    {
        return joinSparitionDominantSide;
    }

    public int getFallbackPartitionNum()
    {
        return fallbackPartitionNum;
    }

    public long getAutoBroadcastJoinThreshold()
    {
        return autoBroadcastJoinThreshold;
    }

    public String toString()
    {
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
        }
        catch (Exception e) {
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
}
