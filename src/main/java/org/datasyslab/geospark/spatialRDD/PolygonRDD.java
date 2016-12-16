/**
 * FILE: PolygonRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PolygonRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.PolygonFormatMapper;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PolygonXMaxComparator;
import org.datasyslab.geospark.utils.PolygonXMinComparator;
import org.datasyslab.geospark.utils.PolygonYMaxComparator;
import org.datasyslab.geospark.utils.PolygonYMinComparator;
import org.datasyslab.geospark.utils.RDDSampleUtils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;

import scala.Tuple2;


// TODO: Auto-generated Javadoc

/**
 * The Class PolygonRDD.
 */
public class PolygonRDD implements Serializable {

    /** The total number of records. */
    public long totalNumberOfRecords;
    
    /** The raw polygon RDD. */
    public JavaRDD<Polygon> rawPolygonRDD;
   
    
    /** The boundary. */
    Double[] boundary = new Double[4];

    /** The boundary envelope. */
    public Envelope boundaryEnvelope;

    /** The grid polygon RDD. */
    public JavaPairRDD<Integer, Polygon> gridPolygonRDD;

    /** The grids. */
    public HashSet<EnvelopeWithGrid> grids;


    //todo, replace this STRtree to be more generalized, such as QuadTree.
    /** The indexed RDD. */
    public JavaPairRDD<Integer, STRtree> indexedRDD;
    
    /** The indexed RDD no id. */
    public JavaRDD<STRtree> indexedRDDNoId;
    
    private Envelope minXEnvelope;
    private Envelope minYEnvelope;
    private Envelope maxXEnvelope;
    private Envelope maxYEnvelope;


    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawPolygonRDD the raw polygon RDD
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.setRawPolygonRDD(rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY()));
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param partitions the partitions
     */ 
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter, Integer partitions) {

        this.setRawPolygonRDD(spark.textFile(InputLocation, partitions).map(new PolygonFormatMapper(Offset, splitter)));//.persist(StorageLevel.MEMORY_ONLY()));
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     */
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, FileDataSplitter splitter) {

        this.setRawPolygonRDD(spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset, splitter)));//.persist(StorageLevel.MEMORY_ONLY()));
    }



    
    /**
     * Gets the raw polygon RDD.
     *
     * @return the raw polygon RDD
     */
    public JavaRDD<Polygon> getRawPolygonRDD() {
        return rawPolygonRDD;
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawPolygonRDD the raw polygon RDD
     * @param gridType the grid type
     * @param numPartitions the num partitions
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD, GridType gridType, Integer numPartitions)
    {
    	this.setRawPolygonRDD(rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY()));
    	 this.rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY());
         totalNumberOfRecords = this.rawPolygonRDD.count();

         doSpatialPartitioning(gridType,numPartitions);

         //final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
         JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
                 new PairFlatMapFunction<Polygon, Integer, Polygon>() {
                     @Override
                     public Iterator<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
                     	HashSet<Tuple2<Integer, Polygon>> result = PartitionJudgement.getPartitionID(grids,polygon);
                         return result.iterator();
                     }
                 }
         );
         this.rawPolygonRDD.unpersist();
         this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    }
    
    
    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawPolygonRDD the raw polygon RDD
     * @param gridType the grid type
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD, GridType gridType)
    {
    	this.setRawPolygonRDD(rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY()));
    	this.rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY());
    	totalNumberOfRecords = this.rawPolygonRDD.count();
    	int numPartitions=this.rawPolygonRDD.getNumPartitions();

    	doSpatialPartitioning(gridType,numPartitions);

    	//final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
    	JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
    			new PairFlatMapFunction<Polygon, Integer, Polygon>() {
    				@Override
    				public Iterator<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
    					HashSet<Tuple2<Integer, Polygon>> result = PartitionJudgement.getPartitionID(grids,polygon);
    					return result.iterator();
    				}
    			}
         );
    	this.rawPolygonRDD.unpersist();
    	this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    }

    
    //todo: remove offset.
    /**
     * Instantiates a new polygon RDD.
     *
     * @param sc the sc
     * @param inputLocation the input location
     * @param offSet the off set
     * @param splitter the splitter
     * @param gridType the grid type
     * @param numPartitions the num partitions
     */
    public PolygonRDD(JavaSparkContext sc, String inputLocation, Integer offSet, FileDataSplitter splitter, GridType gridType, Integer numPartitions) {
        this.rawPolygonRDD = sc.textFile(inputLocation).map(new PolygonFormatMapper(offSet, splitter));
        this.rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY());
        totalNumberOfRecords = this.rawPolygonRDD.count();

        doSpatialPartitioning(gridType,numPartitions);

        //final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
                new PairFlatMapFunction<Polygon, Integer, Polygon>() {
                    @Override
                    public Iterator<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
                    	HashSet<Tuple2<Integer, Polygon>> result = PartitionJudgement.getPartitionID(grids,polygon);
                        return result.iterator();
                    }
                }
        );
        this.rawPolygonRDD.unpersist();
        this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    
        
    }
    
    /**
     * Instantiates a new polygon RDD.
     *
     * @param sc the sc
     * @param inputLocation the input location
     * @param offSet the off set
     * @param splitter the splitter
     * @param gridType the grid type
     */
    public PolygonRDD(JavaSparkContext sc, String inputLocation, Integer offSet, FileDataSplitter splitter, GridType gridType) {
        this.rawPolygonRDD = sc.textFile(inputLocation).map(new PolygonFormatMapper(offSet, splitter));
        this.rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY());
        totalNumberOfRecords = this.rawPolygonRDD.count();

        int numPartitions=this.rawPolygonRDD.getNumPartitions();
        
        doSpatialPartitioning(gridType,numPartitions);

        //final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
                new PairFlatMapFunction<Polygon, Integer, Polygon>() {
                    @Override
                    public Iterator<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
                    	HashSet<Tuple2<Integer, Polygon>> result = PartitionJudgement.getPartitionID(grids,polygon);
                        return result.iterator();
                    }
                }
        );
        this.rawPolygonRDD.unpersist();
        this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    
        
    }
    
    private void doSpatialPartitioning(GridType gridType, int numPartitions)
    {
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

        ArrayList<Polygon> polygonSampleList = new ArrayList<Polygon> (rawPolygonRDD.takeSample(false, sampleNumberOfRecords));

        this.boundary();

        if(sampleNumberOfRecords == 0) {
            //If the sample Number is too small, we will just use one grid instead.
            System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
            System.err.println("The sample size is " + totalNumberOfRecords /100);
            System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
            System.err.println("we will just build one grid for all input");
            grids = new HashSet<EnvelopeWithGrid>();
            grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
        }  else if (gridType == GridType.EQUALGRID) {
        	EqualPartitioning equalPartitioning =new EqualPartitioning(this.boundaryEnvelope,numPartitions);
        	grids=equalPartitioning.getGrids();
        }
        else if(gridType == GridType.HILBERT)
        {
        	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(polygonSampleList.toArray(new Polygon[polygonSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=hilbertPartitioning.getGrids();
        }
        else if(gridType == GridType.RTREE)
        {
        	RtreePartitioning rtreePartitioning=new RtreePartitioning(polygonSampleList.toArray(new Polygon[polygonSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=rtreePartitioning.getGrids();
        }
        else if(gridType == GridType.VORONOI)
        {
        	VoronoiPartitioning voronoiPartitioning=new VoronoiPartitioning(polygonSampleList.toArray(new Polygon[polygonSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=voronoiPartitioning.getGrids();
        }
        else
        {
        	throw new IllegalArgumentException("Partitioning method is not recognized, please check again.");
        }
    }

    
    /**
     * Builds the index.
     *
     * @param indexType the index type
     */
    public void buildIndex(IndexType indexType) {

        if (this.gridPolygonRDD == null) {
        	
        	//This index is built on top of unpartitioned SRDD
            this.indexedRDDNoId =  this.rawPolygonRDD.mapPartitions(new FlatMapFunction<Iterator<Polygon>,STRtree>()
            		{
						@Override
						public Iterator<STRtree> call(Iterator<Polygon> t)
								throws Exception {
							// TODO Auto-generated method stub
							 STRtree rt = new STRtree();
							while(t.hasNext()){
								Polygon polygon=t.next();
			                    rt.insert(polygon.getEnvelopeInternal(), polygon);
							}
							HashSet<STRtree> result = new HashSet<STRtree>();
			                    result.add(rt);
			                    return result.iterator();
						}
            	
            		});
            this.indexedRDDNoId.persist(StorageLevel.MEMORY_ONLY());
        }
        else
        {
        //Use GroupByKey, since I have repartition data, it should be much faster.
        //todo: Need to test performance here...
        JavaPairRDD<Integer, Iterable<Polygon>> gridedRectangleListRDD = this.gridPolygonRDD.groupByKey();

        this.indexedRDD = gridedRectangleListRDD.flatMapValues(new Function<Iterable<Polygon>, Iterable<STRtree>>() {
            @Override
            public Iterable<STRtree> call(Iterable<Polygon> polygons) throws Exception {
                STRtree rt = new STRtree();
                for (Polygon p : polygons)
                    rt.insert(p.getEnvelopeInternal(), p);
                HashSet<STRtree> result = new HashSet<STRtree>();
                result.add(rt);
                return result;
            }
        });
        this.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        }
    }
    
    /**
     * Sets the raw polygon RDD.
     *
     * @param rawPolygonRDD the new raw polygon RDD
     */
    public void setRawPolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.rawPolygonRDD = rawPolygonRDD;
    }

	/**
	 * Re partition.
	 *
	 * @param partitions the partitions
	 * @return the java RDD
	 */
	public JavaRDD<Polygon> rePartition(Integer partitions)
	{
		return this.rawPolygonRDD.repartition(partitions);
	}
	

    /**
     * Boundary.
     *
     * @return the envelope
     */
    public Envelope boundary() {
        minXEnvelope = this.rawPolygonRDD
                .min((PolygonXMinComparator) GeometryComparatorFactory.createComparator("polygon", "x", "min")).getEnvelopeInternal();
        Double minLongitude = minXEnvelope.getMinX();

        maxXEnvelope = this.rawPolygonRDD
                .max((PolygonXMaxComparator) GeometryComparatorFactory.createComparator("polygon", "x", "max")).getEnvelopeInternal();
        Double maxLongitude = maxXEnvelope.getMaxX();

        minYEnvelope = this.rawPolygonRDD
                .min((PolygonYMinComparator) GeometryComparatorFactory.createComparator("polygon", "y", "min")).getEnvelopeInternal();
        Double minLatitude = minYEnvelope.getMinY();

        maxYEnvelope = this.rawPolygonRDD
                .max((PolygonYMaxComparator) GeometryComparatorFactory.createComparator("polygon", "y", "max")).getEnvelopeInternal();
        Double maxLatitude = maxYEnvelope.getMaxY();
        this.boundary[0] = minLongitude;
        this.boundary[1] = minLatitude;
        this.boundary[2] = maxLongitude;
        this.boundary[3] = maxLatitude;
        this.boundaryEnvelope = new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
        return new Envelope(boundary[0], boundary[2], boundary[1], boundary[3]);
    }

    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Envelope> rectangleRDD = this.rawPolygonRDD.map(new Function<Polygon, Envelope>() {

            public Envelope call(Polygon s) {
                Envelope MBR = s.getEnvelope().getEnvelopeInternal();//.getEnvelopeInternal();
                return MBR;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }

    /**
     * Polygon union.
     *
     * @return the polygon
     */
    public Polygon PolygonUnion() {
        Polygon result = this.rawPolygonRDD.reduce(new Function2<Polygon, Polygon, Polygon>() {

            public Polygon call(Polygon v1, Polygon v2) {

                //Reduce precision in JTS to avoid TopologyException
                PrecisionModel pModel = new PrecisionModel();
                GeometryPrecisionReducer pReducer = new GeometryPrecisionReducer(pModel);
                Geometry p1 = pReducer.reduce(v1);
                Geometry p2 = pReducer.reduce(v2);
                //Union two polygons
                Geometry polygonGeom = p1.union(p2);
                Coordinate[] coordinates = polygonGeom.getCoordinates();
                ArrayList<Coordinate> coordinateList = new ArrayList<Coordinate>(Arrays.asList(coordinates));
                Coordinate lastCoordinate = coordinateList.get(0);
                coordinateList.add(lastCoordinate);
                Coordinate[] coordinatesClosed = new Coordinate[coordinateList.size()];
                coordinatesClosed = coordinateList.toArray(coordinatesClosed);
                GeometryFactory fact = new GeometryFactory();
                LinearRing linear = new GeometryFactory().createLinearRing(coordinatesClosed);
                Polygon polygon = new Polygon(linear, null, fact);
                //Return the two polygon union result
                return polygon;
            }

        });
        return result;
    }
    
 
    /**
     * Spatial partition.
     *
     * @param gridsFromOtherSRDD the grids from other SRDD
     */
    public void SpatialPartition(HashSet<EnvelopeWithGrid> gridsFromOtherSRDD) {

		//final Integer offset=Offset;
		this.grids=gridsFromOtherSRDD;
		//final ArrayList<EnvelopeWithGrid> gridEnvelopBroadcasted = grids;
		JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
                new PairFlatMapFunction<Polygon, Integer, Polygon>() {
                    @Override
                    public Iterator<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
                    	HashSet<Tuple2<Integer, Polygon>> result = PartitionJudgement.getPartitionID(grids,polygon);
                        return result.iterator();
                    }
                }
        );
		this.rawPolygonRDD.unpersist();
        this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new SpatialPartitioner(grids.size())).persist(StorageLevel.MEMORY_ONLY());
    }
}

