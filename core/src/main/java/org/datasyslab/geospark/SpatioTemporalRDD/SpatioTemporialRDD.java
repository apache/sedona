/*
 * FILE: SpatioTemporalRDD
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
package org.datasyslab.geospark.SpatioTemporalRDD;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.random.SamplingUtils;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.SpatioTemporalObject;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.SpatioTemporalGridType;
import org.datasyslab.geospark.spatioTemporalPartitioning.CubeGridPartitioner;
import org.datasyslab.geospark.spatioTemporalPartitioning.EqualDataSlicePartitioning;
import org.datasyslab.geospark.spatioTemporalPartitioning.EqualTimeSlicePartitioning;
import org.datasyslab.geospark.spatioTemporalPartitioning.OctreePartitioning;
import org.datasyslab.geospark.spatioTemporalPartitioning.RTree3DPartitioning;
import org.datasyslab.geospark.spatioTemporalPartitioning.SpatioTemporalPartitioner;
import org.datasyslab.geospark.spatioTemporalPartitioning.octree.Octree;
import org.datasyslab.geospark.spatioTemporalPartitioning.octree.OctreePartitioner;
import org.datasyslab.geospark.spatioTemporalRddTool.SaptioTemporalStatCalculator;
import org.datasyslab.geospark.spatioTemporalRddTool.SpatioTemporalIndexBuilder;
import org.datasyslab.geospark.utils.RDDSampleUtils;

import com.vividsolutions.jts.index.SpatialIndex;

import scala.Tuple2;

// TODO: Auto-generated Javadoc

/**
 * The Class SpatioTemporialRDD.
 */
public class SpatioTemporialRDD<T extends SpatioTemporalObject>
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(SpatioTemporialRDD.class);

    /**
     * The total number of records.
     */
    public long approximateTotalCount = -1;

    /**
     * The boundary cube.
     */
    public Cube boundaryCube = null;

    /**
     * The spatio-temporal partitioned RDD.
     */
    public JavaRDD<T> spatioTemporalPartitionedRDD;

    /**
     * The indexed RDD.
     */
    public JavaRDD<SpatialIndex> indexedRDD;

    /**
     * The indexed raw RDD.
     */
    public JavaRDD<SpatialIndex> indexedRawRDD;

    /**
     * The raw SpatioTemporal RDD.
     */
    public JavaRDD<T> rawSpatioTemporalRDD;

    /**
     * The grids.
     */
    public List<Cube> grids;

    public Octree partitionTree;

    private SpatioTemporalPartitioner partitioner;

    /**
     * The sample number.
     */
    private int sampleNumber = -1;

    public int getSampleNumber()
    {
        return sampleNumber;
    }

    /**
     * Sets the sample number.
     *
     * @param sampleNumber the new sample number
     */
    public void setSampleNumber(int sampleNumber)
    {
        this.sampleNumber = sampleNumber;
    }

    public boolean spatioTemporalPartitioning(SpatioTemporalGridType gridType)
            throws Exception
    {
        int numPartitions = this.rawSpatioTemporalRDD.rdd().partitions().length;
        spatioTemporalPartitioning(gridType, numPartitions);
        return true;
    }

    /**
     * Spatio temporal partitioning.
     *
     * @param gridType the grid type
     * @return true, if successful
     * @throws Exception the exception
     */
    public void spatioTemporalPartitioning(SpatioTemporalGridType gridType, int numPartitions)
            throws Exception
    {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        if (this.boundaryCube == null) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
        }
        if (this.approximateTotalCount == -1) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unkown. Please call analyze() first.");
        }

        // Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount, this.sampleNumber);
        // Take Sample
        // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
        // number of samples requested. Repeated scans increase the latency of the join. This increase
        // is significant for large datasets.
        // See https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
        // Here, we choose to get samples faster over getting exactly specified number of samples.
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, approximateTotalCount, false);
        List<Cube> samples = this.rawSpatioTemporalRDD.sample(false, fraction)
                .map(new Function<T, Cube>()
                {
                    @Override
                    public Cube call(T geometry)
                            throws Exception
                    {
                        return geometry.getCubeInternal();
                    }
                })
                .collect();

        logger.info("Collected " + samples.size() + " samples");

        // Add some padding at the top and right of the paddedBoundary to make
        // sure all geometries lie within the half-open rectangle.
        final Cube paddedBoundary = new Cube(
                boundaryCube.getMinX(), boundaryCube.getMaxX() + 0.01,
                boundaryCube.getMinY(), boundaryCube.getMaxY() + 0.01,
                boundaryCube.getMinZ(), boundaryCube.getMaxZ() + 0.01);



        switch (gridType) {

            case OCTREE:{
                // just build tree
                OctreePartitioning octreePartitioning = new OctreePartitioning(samples, paddedBoundary, numPartitions);
                partitionTree = octreePartitioning.getPartitionTree();
                // implement your own partitioner logic
                // clean tree in construct
                partitioner = new OctreePartitioner(partitionTree);
                break;
            }
            case RTREE3D: {
                RTree3DPartitioning rTree3DPartitioning = new RTree3DPartitioning(samples, paddedBoundary, numPartitions);
                grids = rTree3DPartitioning.getGrids();
                partitioner = new CubeGridPartitioner(SpatioTemporalGridType.RTREE3D, grids);
                break;
            }
            case EQUALTIMESLICE: {
                EqualTimeSlicePartitioning equalTimeSlicePartitioning = new EqualTimeSlicePartitioning(samples,
                        paddedBoundary, numPartitions);
                grids = equalTimeSlicePartitioning.getGrids();
                partitioner = new CubeGridPartitioner(SpatioTemporalGridType.EQUALTIMESLICE, grids);
                break;
            }
            case EQUALDATASLICE: {
                EqualDataSlicePartitioning equalDataSlicePartitioning = new EqualDataSlicePartitioning(samples,
                        paddedBoundary, numPartitions);
                grids = equalDataSlicePartitioning.getGrids();
                partitioner = new CubeGridPartitioner(SpatioTemporalGridType.EQUALDATASLICE, grids);
                break;
            }

            default:
                throw new Exception("[AbstractSpatialRDD][spatioTemporalPartitioning] Unsupported spatioTemporal partitioning method.");
        }

        // partition by the method in partitioner
        this.spatioTemporalPartitionedRDD = partition(partitioner);
    }

    public SpatioTemporalPartitioner getPartitioner()
    {
        return partitioner;
    }

    public void spatioTemporalPartitioning(SpatioTemporalPartitioner partitioner)
    {
        this.partitioner = partitioner;
        this.spatioTemporalPartitionedRDD = partition(partitioner);
    }

    /**
     * @deprecated Use spatioTemporalPartitioning(SpatialPartitioner partitioner)
     */
    public boolean spatioTemporalPartitioning(final Octree partitionTree)
            throws Exception
    {
        this.partitioner = new OctreePartitioner(partitionTree);
        // return already partitioned rdd
        this.spatioTemporalPartitionedRDD = partition(partitioner);
        this.partitionTree = partitionTree;
        return true;
    }

    private JavaRDD<T> partition(final SpatioTemporalPartitioner partitioner)
    {
        return this.rawSpatioTemporalRDD.flatMapToPair(
                new PairFlatMapFunction<T, Integer, T>()
                {
                    @Override
                    public Iterator<Tuple2<Integer, T>> call(T spatioTemporalObject)
                            throws Exception
                    {
                        return partitioner.placeObject(spatioTemporalObject);
                    }
                }
        ).partitionBy(partitioner)
                .mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, T>>, T>()
                {
                    @Override
                    public Iterator<T> call(final Iterator<Tuple2<Integer, T>> tuple2Iterator)
                            throws Exception
                    {
                        return new Iterator<T>()
                        {
                            @Override
                            public boolean hasNext()
                            {
                                return tuple2Iterator.hasNext();
                            }

                            @Override
                            public T next()
                            {
                                return tuple2Iterator.next()._2();
                            }

                            @Override
                            public void remove()
                            {
                                throw new UnsupportedOperationException();
                            }
                        };
                    }
                }, true);
    }

    /**
     * Count without duplicates.
     *
     * @return the long
     */
    public long countWithoutDuplicates()
    {
        List collectedResult = this.rawSpatioTemporalRDD.collect();
        HashSet resultWithoutDuplicates = new HashSet();
        for (int i = 0; i < collectedResult.size(); i++) {
            resultWithoutDuplicates.add(collectedResult.get(i));
        }
        return resultWithoutDuplicates.size();
    }

    /**
     * Count without duplicates SPRDD.
     *
     * @return the long
     */
    public long countWithoutDuplicatesSPRDD()
    {
        JavaRDD cleanedRDD = this.spatioTemporalPartitionedRDD;
        List collectedResult = cleanedRDD.collect();
        HashSet resultWithoutDuplicates = new HashSet();
        for (int i = 0; i < collectedResult.size(); i++) {
            resultWithoutDuplicates.add(collectedResult.get(i));
        }
        return resultWithoutDuplicates.size();
    }

    /**
     * Boundary.
     *
     * @return the envelope
     * @deprecated Call analyze() instead
     */
    public Cube boundary()
    {
        this.analyze();
        return this.boundaryCube;
    }

    /**
     * Gets the raw SpatioTemporal RDD.
     *
     * @return the raw SpatioTemporal RDD
     */
    public JavaRDD<T> getRawSpatioTemporalRDD()
    {
        return rawSpatioTemporalRDD;
    }

    /**
     * Sets the raw SpatioTemporal RDD.
     *
     * @param rawSpatialRDD the new raw SpatioTemporal RDD
     */
    public void setRawSpatioTemporalRDD(JavaRDD<T> rawSpatialRDD)
    {
        this.rawSpatioTemporalRDD = rawSpatialRDD;
    }

    /**
     * Analyze.
     *
     * @param newLevel the new level
     * @return true, if successful
     */
    public boolean analyze(StorageLevel newLevel)
    {
        this.rawSpatioTemporalRDD = this.rawSpatioTemporalRDD.persist(newLevel);
        this.analyze();
        return true;
    }

    /**
     * Analyze.
     *
     * @return true, if successful
     */
    public boolean analyze()
    {
        final Function2 combOp =
                new Function2<SaptioTemporalStatCalculator, SaptioTemporalStatCalculator, SaptioTemporalStatCalculator>()
                {
                    @Override
                    public SaptioTemporalStatCalculator call(SaptioTemporalStatCalculator agg1, SaptioTemporalStatCalculator agg2)
                            throws Exception
                    {
                        return SaptioTemporalStatCalculator.combine(agg1, agg2);
                    }
                };

        final Function2 seqOp = new Function2<SaptioTemporalStatCalculator, SpatioTemporalObject, SaptioTemporalStatCalculator>()
        {
            @Override
            public SaptioTemporalStatCalculator call(SaptioTemporalStatCalculator agg, SpatioTemporalObject object)
                    throws Exception
            {
                return SaptioTemporalStatCalculator.add(agg, object);
            }
        };

        SaptioTemporalStatCalculator
                agg = (SaptioTemporalStatCalculator) this.rawSpatioTemporalRDD.aggregate(null, seqOp, combOp);
        if (agg != null) {
            this.boundaryCube = agg.getBoundary();
            this.approximateTotalCount = agg.getCount();
        }
        else {
            this.boundaryCube = null;
            this.approximateTotalCount = 0;
        }
        return true;
    }

    public boolean analyze(Cube datasetBoundary, Integer approximateTotalCount)
    {
        this.boundaryCube = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
        return true;
    }

    /**
     * Builds the index.
     *
     * @param indexType the index type
     * @param buildIndexOnSpatioTemporalPartitionedRDD the build index on spatial partitioned RDD
     * @throws Exception the exception
     */
    public void buildIndex(final IndexType indexType, boolean buildIndexOnSpatioTemporalPartitionedRDD)
            throws Exception
    {
        if (buildIndexOnSpatioTemporalPartitionedRDD == false) {
            //This index is built on top of unpartitioned SRDD
            this.indexedRawRDD = this.rawSpatioTemporalRDD.mapPartitions(new SpatioTemporalIndexBuilder(indexType));
        }
        else {
            if (this.spatioTemporalPartitionedRDD == null) {
                throw new Exception("[AbstractSpatialRDD][buildIndex] spatioTemporalPartitionedRDD is null. Please do spatial partitioning before build index.");
            }
            this.indexedRDD = this.spatioTemporalPartitionedRDD.mapPartitions(new SpatioTemporalIndexBuilder(indexType));
        }
    }

}
