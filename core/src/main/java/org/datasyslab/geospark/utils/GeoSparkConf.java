/*
 * FILE: GeoSparkConf
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.utils;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.SparkConf;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.JoinBuildSide;
import org.datasyslab.geospark.enums.JoinSparitionDominantSide;

import java.io.Serializable;
import java.lang.reflect.Field;

public class GeoSparkConf
        implements Serializable
{

    // Global parameters of GeoSpark. All these parameters can be initialized through SparkConf.

    private Boolean useIndex = false;

    private IndexType indexType = IndexType.QUADTREE;

    // Parameters for JoinQuery including RangeJoin and DistanceJoin

    private JoinSparitionDominantSide joinSparitionDominantSide = JoinSparitionDominantSide.LEFT;

    private JoinBuildSide joinBuildSide = JoinBuildSide.LEFT;

    private Long joinApproximateTotalCount = (long) -1;

    private Envelope datasetBoundary = new Envelope(0, 0, 0, 0);

    private Integer fallbackPartitionNum = -1;

    private GridType joinGridType = GridType.QUADTREE;

    public GeoSparkConf(SparkConf sparkConf)
    {
        this.useIndex = sparkConf.getBoolean("geospark.global.index", true);
        this.indexType = IndexType.getIndexType(sparkConf.get("geospark.global.indextype", "rtree"));
        this.joinApproximateTotalCount = sparkConf.getLong("geospark.join.approxcount", -1);
        String[] boundaryString = sparkConf.get("geospark.join.boundary", "0,0,0,0").split(",");
        this.datasetBoundary = new Envelope(Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[0]),
                Double.parseDouble(boundaryString[0]), Double.parseDouble(boundaryString[0]));
        this.joinGridType = GridType.getGridType(sparkConf.get("geospark.join.gridtype", "quadtree"));
        this.joinBuildSide = JoinBuildSide.getBuildSide(sparkConf.get("geospark.join.indexbuildside", "left"));
        this.joinSparitionDominantSide = JoinSparitionDominantSide.getJoinSparitionDominantSide(sparkConf.get("geospark.join.spatitionside", "left"));
        this.fallbackPartitionNum = sparkConf.getInt("geospark.join.numpartition", -1);
    }

    public Boolean getUseIndex()
    {
        return useIndex;
    }

    public void setUseIndex(Boolean useIndex)
    {
        this.useIndex = useIndex;
    }

    public IndexType getIndexType()
    {
        return indexType;
    }

    public void setIndexType(IndexType indexType)
    {
        this.indexType = indexType;
    }

    public Long getJoinApproximateTotalCount()
    {
        return joinApproximateTotalCount;
    }

    public void setJoinApproximateTotalCount(Long joinApproximateTotalCount)
    {
        this.joinApproximateTotalCount = joinApproximateTotalCount;
    }

    public Envelope getDatasetBoundary()
    {
        return datasetBoundary;
    }

    public void setDatasetBoundary(Envelope datasetBoundary)
    {
        this.datasetBoundary = datasetBoundary;
    }

    public JoinBuildSide getJoinBuildSide()
    {
        return joinBuildSide;
    }

    public void setJoinBuildSide(JoinBuildSide joinBuildSide)
    {
        this.joinBuildSide = joinBuildSide;
    }

    public GridType getJoinGridType()
    {
        return joinGridType;
    }

    public void setJoinGridType(GridType joinGridType)
    {
        this.joinGridType = joinGridType;
    }

    public JoinSparitionDominantSide getJoinSparitionDominantSide()
    {
        return joinSparitionDominantSide;
    }

    public void setJoinSparitionDominantSide(JoinSparitionDominantSide joinSparitionDominantSide)
    {
        this.joinSparitionDominantSide = joinSparitionDominantSide;
    }

    public Integer getFallbackPartitionNum()
    {
        return fallbackPartitionNum;
    }

    public void setFallbackPartitionNum(Integer fallbackPartitionNum)
    {
        this.fallbackPartitionNum = fallbackPartitionNum;
    }

    public String toString()
    {
        try {
            String sb = "";
            Class<?> objClass = this.getClass();
            sb += "GeoSpark Configuration:\n";
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
}
