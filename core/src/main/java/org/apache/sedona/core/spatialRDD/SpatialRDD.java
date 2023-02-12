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

import org.apache.commons.lang.NullArgumentException;
import org.apache.log4j.Logger;
import org.apache.sedona.common.utils.GeomUtils;
import org.apache.sedona.core.enums.GridType;
import org.apache.sedona.core.enums.IndexType;
import org.apache.sedona.core.spatialPartitioning.*;
import org.apache.sedona.core.spatialPartitioning.quadtree.StandardQuadTree;
import org.apache.sedona.core.spatialRddTool.IndexBuilder;
import org.apache.sedona.core.spatialRddTool.StatCalculator;
import org.apache.sedona.core.utils.RDDSampleUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.random.SamplingUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.SpatialIndex;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.wololo.geojson.Feature;
import org.wololo.jts2geojson.GeoJSONWriter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

// TODO: Auto-generated Javadoc

/**
 * The Class SpatialRDD.
 */
public class SpatialRDD<T extends Geometry>
        implements Serializable
{

    /**
     * The Constant logger.
     */
    final static Logger logger = Logger.getLogger(SpatialRDD.class);

    /**
     * The total number of records.
     */
    public long approximateTotalCount = -1;

    /**
     * The boundary envelope.
     */
    public Envelope boundaryEnvelope = null;

    /**
     * The spatial partitioned RDD.
     */
    public JavaRDD<T> spatialPartitionedRDD;

    /**
     * The indexed RDD.
     */
    public JavaRDD<SpatialIndex> indexedRDD;

    /**
     * The indexed raw RDD.
     */
    public JavaRDD<SpatialIndex> indexedRawRDD;

    /**
     * The raw spatial RDD.
     */
    public JavaRDD<T> rawSpatialRDD;

    public List<String> fieldNames;
    /**
     * The CR stransformation.
     */
    protected boolean CRStransformation = false;
    /**
     * The source epsg code.
     */
    protected String sourceEpsgCode = "";
    /**
     * The target epgsg code.
     */
    protected String targetEpgsgCode = "";
    private SpatialPartitioner partitioner;
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

    /**
     * CRS transform.
     *
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     * @param lenient consider the difference of the geodetic datum between the two coordinate systems,
     * if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but not
     * recommended for careful analysis work
     * @return true, if successful
     */
    public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient)
    {
        try {
            CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
            CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
            final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
            this.CRStransformation = true;
            this.sourceEpsgCode = sourceEpsgCRSCode;
            this.targetEpgsgCode = targetEpsgCRSCode;
            this.rawSpatialRDD = this.rawSpatialRDD.map(new Function<T, T>()
            {
                @Override
                public T call(T originalObject)
                        throws Exception
                {
                    return (T) JTS.transform(originalObject, transform);
                }
            });
            return true;
        }
        catch (FactoryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }

    /**
     * CRS transform.
     *
     * @param sourceEpsgCRSCode the source epsg CRS code
     * @param targetEpsgCRSCode the target epsg CRS code
     * @return true, if successful
     */
    public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode)
    {
        return CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode, false);
    }

    public boolean spatialPartitioning(GridType gridType)
            throws Exception
    {
        int numPartitions = this.rawSpatialRDD.rdd().partitions().length;
        spatialPartitioning(gridType, numPartitions);
        return true;
    }

    /**
     * Spatial partitioning.
     *
     * @param gridType the grid type
     * @return true, if successful
     * @throws Exception the exception
     */
    public void calc_partitioner(GridType gridType, int numPartitions)
            throws Exception
    {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be >= 0");
        }

        if (this.boundaryEnvelope == null) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call analyze() first.");
        }
        if (this.approximateTotalCount == -1) {
            throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD total count is unknown. Please call analyze() first.");
        }

        //Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.approximateTotalCount, this.sampleNumber);
        //Take Sample
        // RDD.takeSample implementation tends to scan the data multiple times to gather the exact
        // number of samples requested. Repeated scans increase the latency of the join. This increase
        // is significant for large datasets.
        // See https://github.com/apache/spark/blob/412b0e8969215411b97efd3d0984dc6cac5d31e0/core/src/main/scala/org/apache/spark/rdd/RDD.scala#L508
        // Here, we choose to get samples faster over getting exactly specified number of samples.
        final double fraction = SamplingUtils.computeFractionForSampleSize(sampleNumberOfRecords, approximateTotalCount, false);
        List<Envelope> samples = this.rawSpatialRDD.sample(false, fraction)
                .map(new Function<T, Envelope>()
                {
                    @Override
                    public Envelope call(T geometry)
                            throws Exception
                    {
                        return geometry.getEnvelopeInternal();
                    }
                })
                .collect();

        logger.info("Collected " + samples.size() + " samples");

        // Add some padding at the top and right of the boundaryEnvelope to make
        // sure all geometries lie within the half-open rectangle.
        final Envelope paddedBoundary = new Envelope(
                boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX() + 0.01,
                boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY() + 0.01);

        switch (gridType) {
            case EQUALGRID: {
                // Force the quad-tree to grow up to a certain level
                // So the actual num of partitions might be slightly different
                int minLevel = (int) Math.max(Math.log(numPartitions)/Math.log(4), 0);
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(new ArrayList<Envelope>(), paddedBoundary,
                        numPartitions, minLevel);
                StandardQuadTree tree = quadtreePartitioning.getPartitionTree();
                partitioner = new QuadTreePartitioner(tree);
                break;
            }
            case QUADTREE: {
                QuadtreePartitioning quadtreePartitioning = new QuadtreePartitioning(samples, paddedBoundary, numPartitions);
                StandardQuadTree tree = quadtreePartitioning.getPartitionTree();
                partitioner = new QuadTreePartitioner(tree);
                break;
            }
            case KDBTREE: {
                final KDB tree = new KDB(samples.size() / numPartitions, numPartitions, paddedBoundary);
                for (final Envelope sample : samples) {
                    tree.insert(sample);
                }
                tree.assignLeafIds();
                partitioner = new KDBTreePartitioner(tree);
                break;
            }
            default:
                throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method. " +
                        "The following partitioning methods are not longer supported: R-Tree, Hilbert curve, Voronoi");
        }
    }

    public void spatialPartitioning(GridType gridType, int numPartitions)
            throws Exception
    {
        calc_partitioner(gridType, numPartitions);
        this.spatialPartitionedRDD = partition(partitioner);
    }

    public SpatialPartitioner getPartitioner()
    {
        return partitioner;
    }

    public void spatialPartitioning(SpatialPartitioner partitioner)
    {
        this.partitioner = partitioner;
        this.spatialPartitionedRDD = partition(partitioner);
    }

    /**
     * @deprecated Use spatialPartitioning(SpatialPartitioner partitioner)
     */
    public boolean spatialPartitioning(final List<Envelope> otherGrids)
            throws Exception
    {
        this.partitioner = new FlatGridPartitioner(otherGrids);
        this.spatialPartitionedRDD = partition(partitioner);
        return true;
    }

    /**
     * @deprecated Use spatialPartitioning(SpatialPartitioner partitioner)
     */
    public boolean spatialPartitioning(final StandardQuadTree partitionTree)
            throws Exception
    {
        this.partitioner = new QuadTreePartitioner(partitionTree);
        this.spatialPartitionedRDD = partition(partitioner);
        return true;
    }

    private JavaRDD<T> partition(final SpatialPartitioner partitioner)
    {
        return this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<T, Integer, T>()
                {
                    @Override
                    public Iterator<Tuple2<Integer, T>> call(T spatialObject)
                            throws Exception
                    {
                        return partitioner.placeObject(spatialObject);
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

        List collectedResult = this.rawSpatialRDD.collect();
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
        JavaRDD cleanedRDD = this.spatialPartitionedRDD;
        List collectedResult = cleanedRDD.collect();
        HashSet resultWithoutDuplicates = new HashSet();
        for (int i = 0; i < collectedResult.size(); i++) {
            resultWithoutDuplicates.add(collectedResult.get(i));
        }
        return resultWithoutDuplicates.size();
    }

    /**
     * Builds the index.
     *
     * @param indexType the index type
     * @param buildIndexOnSpatialPartitionedRDD the build index on spatial partitioned RDD
     * @throws Exception the exception
     */
    public void buildIndex(final IndexType indexType, boolean buildIndexOnSpatialPartitionedRDD)
            throws Exception
    {
        if (buildIndexOnSpatialPartitionedRDD == false) {
            //This index is built on top of unpartitioned SRDD
            this.indexedRawRDD = this.rawSpatialRDD.mapPartitions(new IndexBuilder(indexType));
        }
        else {
            if (this.spatialPartitionedRDD == null) {
                throw new Exception("[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do spatial partitioning before build index.");
            }
            this.indexedRDD = this.spatialPartitionedRDD.mapPartitions(new IndexBuilder(indexType));
        }
    }

    /**
     * Boundary.
     *
     * @return the envelope
     * @deprecated Call analyze() instead
     */
    public Envelope boundary()
    {
        this.analyze();
        return this.boundaryEnvelope;
    }

    /**
     * Gets the raw spatial RDD.
     *
     * @return the raw spatial RDD
     */
    public JavaRDD<T> getRawSpatialRDD()
    {
        return rawSpatialRDD;
    }

    /**
     * Sets the raw spatial RDD.
     *
     * @param rawSpatialRDD the new raw spatial RDD
     */
    public void setRawSpatialRDD(JavaRDD<T> rawSpatialRDD)
    {
        this.rawSpatialRDD = rawSpatialRDD;
    }

    /**
     * Analyze.
     *
     * @param newLevel the new level
     * @return true, if successful
     */
    public boolean analyze(StorageLevel newLevel)
    {
        this.rawSpatialRDD = this.rawSpatialRDD.persist(newLevel);
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
                new Function2<StatCalculator, StatCalculator, StatCalculator>()
                {
                    @Override
                    public StatCalculator call(StatCalculator agg1, StatCalculator agg2)
                            throws Exception
                    {
                        return StatCalculator.combine(agg1, agg2);
                    }
                };

        final Function2 seqOp = new Function2<StatCalculator, Geometry, StatCalculator>()
        {
            @Override
            public StatCalculator call(StatCalculator agg, Geometry object)
                    throws Exception
            {
                return StatCalculator.add(agg, object);
            }
        };

        StatCalculator agg = (StatCalculator) this.rawSpatialRDD.aggregate(null, seqOp, combOp);
        if (agg != null) {
            this.boundaryEnvelope = agg.getBoundary();
            this.approximateTotalCount = agg.getCount();
        }
        else {
            this.boundaryEnvelope = null;
            this.approximateTotalCount = 0;
        }
        return true;
    }

    public boolean analyze(Envelope datasetBoundary, Integer approximateTotalCount)
    {
        this.boundaryEnvelope = datasetBoundary;
        this.approximateTotalCount = approximateTotalCount;
        return true;
    }

    /**
     * Save as WKB.
     *
     * @param outputLocation the output location
     */
    public void saveAsWKB(String outputLocation)
    {
        if (this.rawSpatialRDD == null) {
            throw new NullArgumentException("save as WKB cannot operate on null RDD");
        }
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>()
        {
            @Override
            public Iterator<String> call(Iterator<T> iterator)
                    throws Exception
            {
                WKBWriter writer = new WKBWriter(3, true);
                ArrayList<String> wkbs = new ArrayList<>();

                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    String wkb = WKBWriter.toHex(writer.write(spatialObject));

                    if (spatialObject.getUserData() != null) {
                        wkbs.add(wkb + "\t" + spatialObject.getUserData());
                    }
                    else {
                        wkbs.add(wkb);
                    }
                }
                return wkbs.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Save as WKT
     */
    public void saveAsWKT(String outputLocation)
    {
        if (this.rawSpatialRDD == null) {
            throw new NullArgumentException("save as WKT cannot operate on null RDD");
        }
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<T>, String>()
        {
            @Override
            public Iterator<String> call(Iterator<T> iterator)
                    throws Exception
            {
                WKTWriter writer = new WKTWriter(3);
                ArrayList<String> wkts = new ArrayList<>();

                while (iterator.hasNext()) {
                    Geometry spatialObject = iterator.next();
                    String wkt = writer.write(spatialObject);

                    if (spatialObject.getUserData() != null) {
                        wkts.add(wkt + "\t" + spatialObject.getUserData());
                    }
                    else {
                        wkts.add(wkt);
                    }
                }
                return wkts.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation)
    {
        this.rawSpatialRDD.mapPartitions((FlatMapFunction<Iterator<T>, String>) iterator -> {
            ArrayList<String> result = new ArrayList();
            GeoJSONWriter writer = new GeoJSONWriter();
            while (iterator.hasNext()) {
                Geometry spatialObject = iterator.next();
                Feature jsonFeature;
                if (spatialObject.getUserData() != null) {
                    Map<String, Object> fields = new HashMap<String, Object>();
                    String[] fieldValues = spatialObject.getUserData().toString().split("\t");
                    if (fieldNames != null && fieldValues.length == fieldNames.size()) {
                        for (int i = 0 ; i < fieldValues.length ; i++) {
                            fields.put(fieldNames.get(i), fieldValues[i]);
                        }
                    }
                    else {
                        for (int i = 0 ; i < fieldValues.length ; i++) {
                            fields.put("_c" + i, fieldValues[i]);
                        }
                    }
                    jsonFeature = new Feature(writer.write(spatialObject), fields);
                }
                else {
                    jsonFeature = new Feature(writer.write(spatialObject), null);
                }
                String jsonstring = jsonFeature.toString();
                result.add(jsonstring);
            }
            return result.iterator();
        }).saveAsTextFile(outputLocation);
    }

    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    @Deprecated
    public RectangleRDD MinimumBoundingRectangle()
    {
        JavaRDD<Polygon> rectangleRDD = this.rawSpatialRDD.map(new Function<T, Polygon>()
        {
            public Polygon call(T spatialObject)
            {
                Double x1, x2, y1, y2;
                LinearRing linear;
                Coordinate[] coordinates = new Coordinate[5];
                GeometryFactory fact = new GeometryFactory();
                final Envelope envelope = spatialObject.getEnvelopeInternal();
                x1 = envelope.getMinX();
                x2 = envelope.getMaxX();
                y1 = envelope.getMinY();
                y2 = envelope.getMaxY();
                coordinates[0] = new Coordinate(x1, y1);
                coordinates[1] = new Coordinate(x1, y2);
                coordinates[2] = new Coordinate(x2, y2);
                coordinates[3] = new Coordinate(x2, y1);
                coordinates[4] = coordinates[0];
                linear = fact.createLinearRing(coordinates);
                Polygon polygonObject = new Polygon(linear, null, fact);
                return polygonObject;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }

    /**
     * Gets the CR stransformation.
     *
     * @return the CR stransformation
     */
    public boolean getCRStransformation()
    {
        return CRStransformation;
    }

    /**
     * Gets the source epsg code.
     *
     * @return the source epsg code
     */
    public String getSourceEpsgCode()
    {
        return sourceEpsgCode;
    }

    /**
     * Gets the target epgsg code.
     *
     * @return the target epgsg code
     */
    public String getTargetEpgsgCode()
    {
        return targetEpgsgCode;
    }

    public void flipCoordinates() {
        this.rawSpatialRDD = this.rawSpatialRDD.map(f -> {
            GeomUtils.flipCoordinates(f);
            return f;
        });
    }
}
