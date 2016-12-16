/**
 * FILE: RectangleRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.RectangleRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.formatMapper.RectangleFormatMapper;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.datasyslab.geospark.utils.RectangleXMaxComparator;
import org.datasyslab.geospark.utils.RectangleXMinComparator;
import org.datasyslab.geospark.utils.RectangleYMaxComparator;
import org.datasyslab.geospark.utils.RectangleYMinComparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

// TODO: Auto-generated Javadoc

/**
 * The Class RectangleRDD.
 */

public class RectangleRDD implements Serializable {



	/** The total number of records. */
	public long totalNumberOfRecords;
	
	/** The raw rectangle RDD. */
	public JavaRDD<Envelope> rawRectangleRDD;



	/** The boundary. */
	Double[] boundary = new Double[4];

	/** The boundary envelope. */
	public Envelope boundaryEnvelope;

	/** The grid rectangle RDD. */
	public JavaPairRDD<Integer, Envelope> gridRectangleRDD;

	/** The grids. */
	public HashSet<EnvelopeWithGrid> grids;
	//todo, replace this STRtree to be more generalized, such as QuadTree.
	/** The indexed RDD. */
	public JavaPairRDD<Integer, STRtree> indexedRDD;
    
    /** The indexed RDD no id. */
    public JavaRDD<STRtree> indexedRDDNoId;
    
    
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawRectangleRDD the raw rectangle RDD
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD)
	{
		this.setRawRectangleRDD(rawRectangleRDD);//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param partitions the partitions
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,FileDataSplitter splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setRawRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,FileDataSplitter splitter)
	{
		//final Integer offset=Offset;
		this.setRawRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawRectangleRDD the raw rectangle RDD
	 * @param gridType the grid type
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD, GridType gridType)
	{
		this.setRawRectangleRDD(rawRectangleRDD);
		this.rawRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
		totalNumberOfRecords = this.rawRectangleRDD.count();
		
		int numPartitions=this.rawRectangleRDD.getNumPartitions();
		
		doSpatialPartitioning(gridType,numPartitions);
		
		
		//final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
                new PairFlatMapFunction<Envelope, Integer, Envelope>() {
                    @Override
                    public Iterator<Tuple2<Integer, Envelope>> call(Envelope recntangle) throws Exception {
                    	HashSet<Tuple2<Integer, Envelope>> result = PartitionJudgement.getPartitionID(grids,recntangle);
                        return result.iterator();
                    }
                }
        );
        this.rawRectangleRDD.unpersist();
        this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());
	}

	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawRectangleRDD the raw rectangle RDD
	 * @param gridType the grid type
	 * @param numPartitions the num partitions
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD, GridType gridType, Integer numPartitions)
	{
		this.setRawRectangleRDD(rawRectangleRDD);
		this.rawRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
		totalNumberOfRecords = this.rawRectangleRDD.count();
		
		
		
		doSpatialPartitioning(gridType,numPartitions);
		
		
		//final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
                new PairFlatMapFunction<Envelope, Integer, Envelope>() {
                    @Override
                    public Iterator<Tuple2<Integer, Envelope>> call(Envelope recntangle) throws Exception {
                    	HashSet<Tuple2<Integer, Envelope>> result = PartitionJudgement.getPartitionID(grids,recntangle);
                        return result.iterator();
                    }
                }
        );
        this.rawRectangleRDD.unpersist();
        this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());
	}
	
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sc the sc
     * @param inputLocation the input location
     * @param offSet the off set
     * @param splitter the splitter
     * @param gridType the grid type
     * @param numPartitions the num partitions
     */
	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, FileDataSplitter splitter, GridType gridType, Integer numPartitions) {
		this.rawRectangleRDD = sc.textFile(inputLocation).map(new RectangleFormatMapper(offSet, splitter));
		this.rawRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
		totalNumberOfRecords = this.rawRectangleRDD.count();
		
		
		doSpatialPartitioning(gridType,numPartitions);
		
		
		//final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
                new PairFlatMapFunction<Envelope, Integer, Envelope>() {
                    @Override
                    public Iterator<Tuple2<Integer, Envelope>> call(Envelope recntangle) throws Exception {
                    	HashSet<Tuple2<Integer, Envelope>> result = PartitionJudgement.getPartitionID(grids,recntangle);
                        return result.iterator();
                    }
                }
        );
        this.rawRectangleRDD.unpersist();
        this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

	}

    /**
     * Instantiates a new rectangle RDD.
     *
     * @param sc the sc
     * @param inputLocation the input location
     * @param offSet the off set
     * @param splitter the splitter
     * @param gridType the grid type
     */
	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, FileDataSplitter splitter, GridType gridType) {
		this.rawRectangleRDD = sc.textFile(inputLocation).map(new RectangleFormatMapper(offSet, splitter));
		this.rawRectangleRDD.persist(StorageLevel.MEMORY_ONLY());
		totalNumberOfRecords = this.rawRectangleRDD.count();
		
		int numPartitions=this.rawRectangleRDD.getNumPartitions();
		
		doSpatialPartitioning(gridType,numPartitions);
		
		//final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
                new PairFlatMapFunction<Envelope, Integer, Envelope>() {
                    @Override
                    public Iterator<Tuple2<Integer, Envelope>> call(Envelope recntangle) throws Exception {
                    	HashSet<Tuple2<Integer, Envelope>> result = PartitionJudgement.getPartitionID(grids,recntangle);
                        return result.iterator();
                    }
                }
        );
        this.rawRectangleRDD.unpersist();
        this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

	}
	
	
	private void doSpatialPartitioning(GridType gridType, int numPartitions)
	{
		int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

		ArrayList<Envelope> rectangleSampleList = new ArrayList<Envelope> (rawRectangleRDD.takeSample(false, sampleNumberOfRecords));

		this.boundary();

		JavaPairRDD<Integer, Envelope> unPartitionedGridPointRDD;
		
		if(sampleNumberOfRecords == 0) {
			//If the sample Number is too small, we will just use one grid instead.
			System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
			System.err.println("The sample size is " + totalNumberOfRecords /100);
			System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
			System.err.println("we will just build one grid for all input");
			grids = new HashSet<EnvelopeWithGrid>();
			grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
		} 
     
	else if (gridType == GridType.EQUALGRID) {
    	EqualPartitioning equalPartitioning =new EqualPartitioning(this.boundaryEnvelope,numPartitions);
    	grids=equalPartitioning.getGrids();
    }
    else if(gridType == GridType.HILBERT)
    {
    	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(rectangleSampleList.toArray(new Envelope[rectangleSampleList.size()]),this.boundaryEnvelope,numPartitions);
    	grids=hilbertPartitioning.getGrids();
    }
    else if(gridType == GridType.RTREE)
    {
    	RtreePartitioning rtreePartitioning=new RtreePartitioning(rectangleSampleList.toArray(new Envelope[rectangleSampleList.size()]),this.boundaryEnvelope,numPartitions);
    	grids=rtreePartitioning.getGrids();
    }
    else if(gridType == GridType.VORONOI)
    {
    	VoronoiPartitioning voronoiPartitioning=new VoronoiPartitioning(rectangleSampleList.toArray(new Envelope[rectangleSampleList.size()]),this.boundaryEnvelope,numPartitions);
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

		if (this.gridRectangleRDD == null) {
        	//This index is built on top of unpartitioned SRDD
            this.indexedRDDNoId =  this.rawRectangleRDD.mapPartitions(new FlatMapFunction<Iterator<Envelope>,STRtree>()
            		{
						@Override
						public Iterator<STRtree> call(Iterator<Envelope> t)
								throws Exception {
							// TODO Auto-generated method stub
							 STRtree rt = new STRtree();
							 GeometryFactory geometryFactory = new GeometryFactory();
							while(t.hasNext()){
								Envelope envelope=t.next();
								Geometry item= geometryFactory.toGeometry(envelope);
								item.setUserData(envelope.getUserData());
			                    rt.insert(envelope, item);
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
		JavaPairRDD<Integer, Iterable<Envelope>> gridedRectangleListRDD = this.gridRectangleRDD.groupByKey();

		this.indexedRDD = gridedRectangleListRDD.flatMapValues(new Function<Iterable<Envelope>, Iterable<STRtree>>() {
			@Override
			public Iterable<STRtree> call(Iterable<Envelope> envelopes) throws Exception {
				STRtree rt = new STRtree();
				Iterator<Envelope> objectsIterator=envelopes.iterator();
				while(objectsIterator.hasNext()){
					Envelope object=objectsIterator.next();
						rt.insert(object, object);
					}
				HashSet<STRtree> result = new HashSet<STRtree>();
				result.add(rt);
				return result;
			}
		});//.partitionBy(new SpatialPartitioner(grids.size()));
		this.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
		}
	}


	
    /**
     * Gets the raw rectangle RDD.
     *
     * @return the raw rectangle RDD
     */
	public JavaRDD<Envelope> getRawRectangleRDD() {
		return rawRectangleRDD;
	}
	
    /**
     * Sets the raw rectangle RDD.
     *
     * @param rawRectangleRDD the new raw rectangle RDD
     */
	public void setRawRectangleRDD(JavaRDD<Envelope> rawRectangleRDD) {
		this.rawRectangleRDD = rawRectangleRDD;
	}
	
	/**
	 * Re partition.
	 *
	 * @param partitions the partitions
	 * @return the java RDD
	 */
	public JavaRDD<Envelope> rePartition(Integer partitions)
	{
		return this.rawRectangleRDD.repartition(partitions);
	}
	
	
	
    /**
     * Boundary.
     *
     * @return the envelope
     */
	public Envelope boundary()
	{
		Double minLongtitude1=this.rawRectangleRDD.min((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double maxLongtitude1=this.rawRectangleRDD.max((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double minLatitude1=this.rawRectangleRDD.min((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double maxLatitude1=this.rawRectangleRDD.max((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double minLongtitude2=this.rawRectangleRDD.min((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double maxLongtitude2=this.rawRectangleRDD.max((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double minLatitude2=this.rawRectangleRDD.min((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
		Double maxLatitude2=this.rawRectangleRDD.max((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
		if(minLongtitude1<minLongtitude2)
		{
			boundary[0]=minLongtitude1;
		}
		else
		{
			boundary[0]=minLongtitude2;
		}
		if(minLatitude1<minLatitude2)
		{
			boundary[1]=minLatitude1;
		}
		else
		{
			boundary[1]=minLatitude2;
		}
		if(maxLongtitude1>maxLongtitude2)
		{
			boundary[2]=maxLongtitude1;
		}
		else
		{
			boundary[2]=maxLongtitude2;
		}
		if(maxLatitude1>maxLatitude2)
		{
			boundary[3]=maxLatitude1;
		}
		else
		{
			boundary[3]=maxLatitude2;
		}
		this.boundaryEnvelope = new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
		return boundaryEnvelope;

	}
	
	/**
	 * Spatial partition.
	 *
	 * @param gridsFromOtherSRDD the grids from other SRDD
	 */
	public void SpatialPartition(HashSet<EnvelopeWithGrid> gridsFromOtherSRDD)
	{
		//final Integer offset=Offset;
		this.grids=gridsFromOtherSRDD;
		//final ArrayList<EnvelopeWithGrid> gridEnvelopBroadcasted = grids;
        JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
                new PairFlatMapFunction<Envelope, Integer, Envelope>() {
                    @Override
                    public Iterator<Tuple2<Integer, Envelope>> call(Envelope recntangle) throws Exception {
                    	HashSet<Tuple2<Integer, Envelope>> result = PartitionJudgement.getPartitionID(grids,recntangle);
                        return result.iterator();
                    }
                }
        );
        this.rawRectangleRDD.unpersist();
        this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new SpatialPartitioner(grids.size())).persist(StorageLevel.MEMORY_ONLY());
	}
}
