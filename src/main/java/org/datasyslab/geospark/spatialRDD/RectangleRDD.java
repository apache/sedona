package org.datasyslab.geospark.spatialRDD;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.commons.lang.IllegalClassException;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
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
import org.wololo.jts2geojson.GeoJSONReader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

// TODO: Auto-generated Javadoc



/**
 * The Class RectangleRDD. It accommodates Rectangle object.
 * @author Arizona State University DataSystems Lab
 *
 */

public class RectangleRDD implements Serializable {



	/**
	 * The total number of records stored in this RDD
	 */
	public long totalNumberOfRecords;
	
	/**
	 * The original Spatial RDD which has not been spatial partitioned and has not index
	 */
	public JavaRDD<Envelope> rawRectangleRDD;



	/**
	 * The boundary of this RDD, calculated in constructor method, represented by an array
	 */
	Double[] boundary = new Double[4];

	/**
	 * The boundary of this RDD, calculated in constructor method, represented by an envelope
	 */
	public Envelope boundaryEnvelope;

	/**
	 * The SpatialRDD partitioned by spatial grids. Each integer is a spatial partition id
	 */
	public JavaPairRDD<Integer, Envelope> gridRectangleRDD;

	/**
	 * The SpatialRDD partitioned by spatial grids. Each integer is a spatial partition id
	 */
	public HashSet<EnvelopeWithGrid> grids;
	//todo, replace this STRtree to be more generalized, such as QuadTree.
	/**
	 * The partitoned SpatialRDD with built spatial indexes. Each integer is a spatial partition id
	 */
	public JavaPairRDD<Integer, STRtree> indexedRDD;
    /**
     * The partitoned SpatialRDD with built spatial indexes.  Indexes are built on JavaRDD without spatial partitioning.
     */
    public JavaRDD<STRtree> indexedRDDNoId;
    
    
	/**
	 * Initialize one SpatialRDD with one existing SpatialRDD
	 * @param rawRectangleRDD One existing raw RectangleRDD
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD)
	{
		this.setRawRectangleRDD(rawRectangleRDD);//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     * @param partitions specify the partition number of the SpatialRDD
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setRawRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		//final Integer offset=Offset;
		this.setRawRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
	}
	
	/**
	 * Transform a rawRectangleRDD to a RectangelRDD with a a specified spatial partitioning grid type
	 * @param rawRectangleRDD
	 * @param gridType
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD, String gridType)
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
	 * Transform a rawRectangleRDD to a RectangelRDD with a a specified spatial partitioning grid type and the number of partitions
	 * @param rawRectangleRDD
	 * @param gridType
	 * @param numPartitions
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD, String gridType, Integer numPartitions)
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
     * Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it
     * @param sc spark SparkContext which defines some Spark configurations
     * @param inputLocation specify the input path which can be a HDFS path
     * @param offSet specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     * @param numPartitions specify the partition number of the SpatialRDD
     */
	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType, Integer numPartitions) {
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
     *Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it without specifying the number of partitions.
     * @param sc spark SparkContext which defines some Spark configurations
     * @param inputLocation specify the input path which can be a HDFS path
     * @param offSet specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     */
	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType) {
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
	
	
	private void doSpatialPartitioning(String gridType, int numPartitions)
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
     
	else if (gridType.equals("equalgrid")) {
    	EqualPartitioning equalPartitioning =new EqualPartitioning(this.boundaryEnvelope,numPartitions);
    	grids=equalPartitioning.getGrids();
    }
    else if(gridType.equals("hilbert"))
    {
    	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(rectangleSampleList.toArray(new Envelope[rectangleSampleList.size()]),this.boundaryEnvelope,numPartitions);
    	grids=hilbertPartitioning.getGrids();
    }
    else if(gridType.equals("rtree"))
    {
    	RtreePartitioning rtreePartitioning=new RtreePartitioning(rectangleSampleList.toArray(new Envelope[rectangleSampleList.size()]),this.boundaryEnvelope,numPartitions);
    	grids=rtreePartitioning.getGrids();
    }
    else if(gridType.equals("voronoi"))
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
     * Create an IndexedRDD and cache it in memory. Need to have a grided RDD first. The index is build on each partition.
     * @param indexType Specify the index type: rtree, quadtree
     */
	public void buildIndex(String indexType) {

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
     * Get the raw SpatialRDD
     *
     * @return the raw SpatialRDD
     */
	public JavaRDD<Envelope> getRawRectangleRDD() {
		return rawRectangleRDD;
	}
	
    /**
     * Set the raw SpatialRDD.
     *
     * @param rawRectangleRDD One existing SpatialRDD
     */
	public void setRawRectangleRDD(JavaRDD<Envelope> rawRectangleRDD) {
		this.rawRectangleRDD = rawRectangleRDD;
	}
	
	/**
	 * Repartition the raw SpatialRDD.
	 *
	 * @param partitions the partitions number
	 * @return the repartitioned raw SpatialRDD
	 */
	public JavaRDD<Envelope> rePartition(Integer partitions)
	{
		return this.rawRectangleRDD.repartition(partitions);
	}
	
	
	
    /**
     * Return the boundary of the entire SpatialRDD in terms of an envelope format
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
