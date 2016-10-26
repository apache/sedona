package org.datasyslab.geospark.spatialRDD;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.commons.lang.IllegalClassException;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.formatMapper.PointFormatMapper;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PointXComparator;
import org.datasyslab.geospark.utils.PointYComparator;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;



/**
 * The Class PointRDD. It accommodates Point object.
 * @author Arizona State University DataSystems Lab
 *
 */

public class PointRDD implements Serializable {

    static Logger log = Logger.getLogger(PointFormatMapper.class.getName());
    /**
     * The total number of records stored in this RDD
     */
    public long totalNumberOfRecords;
    /**
     * The boundary of this RDD, calculated in constructor method, represented by an array
     */
    public Double[] boundary = new Double[4];
    /**
     * The boundary of this RDD, calculated in constructor method, represented by an envelope
     */
    public Envelope boundaryEnvelope;
    /**
     * The SpatialRDD partitioned by spatial grids. Each integer is a spatial partition id
     */
    public JavaPairRDD<Integer, Point> gridPointRDD;
    /**
     * The partitoned SpatialRDD with built spatial indexes. Each integer is a spatial partition id
     */
    public JavaPairRDD<Integer, STRtree> indexedRDD;
    /**
     * The partitoned SpatialRDD with built spatial indexes.  Indexes are built on JavaRDD without spatial partitioning.
     */
    public JavaRDD<STRtree> indexedRDDNoId;
    /**
     * The original Spatial RDD which has not been spatial partitioned and has not index
     */
    public JavaRDD<Point> rawPointRDD;
    /**
     * The spatial partition boundaries.
     */
    public HashSet<EnvelopeWithGrid> grids;


    /**
     * Initialize one SpatialRDD with one existing SpatialRDD
     * @param rawPointRDD One existing rawPointRDD
     */
    public PointRDD(JavaRDD<Point> rawPointRDD) {
        this.setRawPointRDD(rawPointRDD);
    }
    
    /**
     * Transform a rawPointRDD to a PointRDD with a specified spatial partitioning grid type and the number of partitions
     * @param rawPointRDD
     * @param gridType gridType specify the spatial partitioning method
     * @param numPartitions specify the partition number of the SpatialRDD
     */
    public PointRDD(JavaRDD<Point> rawPointRDD,String gridType, Integer numPartitions) {
        this.setRawPointRDD(rawPointRDD);
        this.rawPointRDD.persist(StorageLevel.MEMORY_ONLY());
        this.totalNumberOfRecords = this.rawPointRDD.count();

        doSpatialPartitioning(gridType,numPartitions);
        

        //final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer, Point> unPartitionedGridPointRDD = this.rawPointRDD.flatMapToPair(
                new PairFlatMapFunction<Point, Integer, Point>() {
                    @Override
                    public Iterator<Tuple2<Integer, Point>> call(Point point) throws Exception {
                    	return PartitionJudgement.getPartitionID(grids,point);
                        
                    }
                }
        );
        this.rawPointRDD.unpersist();
        this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    }

    /**
     * Transform a rawPointRDD to a PointRDD with a specified spatial partitioning grid type
     * @param rawPointRDD
     * @param gridType gridType specify the spatial partitioning method
     */
    public PointRDD(JavaRDD<Point> rawPointRDD,String gridType) {
        this.setRawPointRDD(rawPointRDD);
        this.rawPointRDD.persist(StorageLevel.MEMORY_ONLY());
        this.totalNumberOfRecords = this.rawPointRDD.count();

        int numPartitions=this.rawPointRDD.getNumPartitions();
        
        doSpatialPartitioning(gridType,numPartitions);
        
        JavaPairRDD<Integer, Point> unPartitionedGridPointRDD = this.rawPointRDD.flatMapToPair(
                new PairFlatMapFunction<Point, Integer, Point>() {
                    @Override
                    public Iterator<Tuple2<Integer, Point>> call(Point point) throws Exception {
                    	 return PartitionJudgement.getPartitionID(grids,point);
                       
                    }
                }
        );
        this.rawPointRDD.unpersist();
        this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     * @param partitions specify the partition number of the SpatialRDD
     */
    public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {
        // final Integer offset=Offset;
        this.setRawPointRDD(
                spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     */
    public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {
        // final Integer offset=Offset;
        this.setRawPointRDD(
                spark.textFile(InputLocation).map(new PointFormatMapper(Offset, Splitter)));//.persist(StorageLevel.MEMORY_AND_DISK_SER()));
    }
    
    /**
     * Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it
     * @param sc spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     * @param numPartitions specify the partition number of the SpatialRDD
     */
    public PointRDD(JavaSparkContext sc, String InputLocation, Integer offset, String splitter, String gridType, Integer numPartitions) {
        this.rawPointRDD = sc.textFile(InputLocation).map(new PointFormatMapper(offset, splitter));
        this.rawPointRDD.persist(StorageLevel.MEMORY_ONLY());
        this.totalNumberOfRecords = this.rawPointRDD.count();

        doSpatialPartitioning(gridType,numPartitions);
        
        //final Broadcast<HashSet<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        JavaPairRDD<Integer, Point> unPartitionedGridPointRDD = this.rawPointRDD.flatMapToPair(
                new PairFlatMapFunction<Point, Integer, Point>() {
                    @Override
                    public Iterator<Tuple2<Integer, Point>> call(Point point) throws Exception {
                    	return PartitionJudgement.getPartitionID(grids,point);
                        
                    }
                }
        );
        this.rawPointRDD.unpersist();
        this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

        
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it without specifying the number of partitions.
     * @param sc spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     */    
    public PointRDD(JavaSparkContext sc, String InputLocation, Integer offset, String splitter, String gridType) {
        this.rawPointRDD = sc.textFile(InputLocation).map(new PointFormatMapper(offset, splitter));
        this.rawPointRDD.persist(StorageLevel.MEMORY_ONLY());
        this.totalNumberOfRecords = this.rawPointRDD.count();

        int numPartitions=this.rawPointRDD.getNumPartitions();
        
        doSpatialPartitioning(gridType,numPartitions);
        

        JavaPairRDD<Integer, Point> unPartitionedGridPointRDD = this.rawPointRDD.flatMapToPair(
                new PairFlatMapFunction<Point, Integer, Point>() {
                    @Override
                    public Iterator<Tuple2<Integer, Point>> call(Point point) throws Exception {
                    	 return PartitionJudgement.getPartitionID(grids,point);
                       
                    }
                }
        );
        this.rawPointRDD.unpersist();
        this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new SpatialPartitioner(grids.size()));//.persist(StorageLevel.DISK_ONLY());

        
    }
    
	private void doSpatialPartitioning(String gridType, int numPartitions)
	{
        //Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

        if(sampleNumberOfRecords == -1) {
            throw new IllegalArgumentException("The input size is smaller the number of grid, please reduce the grid size!");
        }
        //Take Sample
        //todo: This creates troubles.. In fact it should be List interface. But List reports problems in Scala Shell, seems that it can not implement polymorphism and Still use List.
        ArrayList<Point> pointSampleList = new ArrayList<Point>(this.rawPointRDD.takeSample(false, sampleNumberOfRecords));
        //Sort
        //Get minX and minY;
        this.boundary();
        //Pick and create boundary;

        

        //This is the case where data size is too small. We will just create one grid
        if(sampleNumberOfRecords == 0) {
            //If the sample Number is too small, we will just use one grid instead.
            System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
            System.err.println("The sample size is " + totalNumberOfRecords/100);
            System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
            System.err.println("we will just build one grid for all input");
            grids = new HashSet<EnvelopeWithGrid>();
            grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
        } else if (gridType.equals("equalgrid")) {
        	EqualPartitioning equalPartitioning =new EqualPartitioning(this.boundaryEnvelope,numPartitions);
        	grids=equalPartitioning.getGrids();
        }
        else if(gridType.equals("hilbert"))
        {
        	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(pointSampleList.toArray(new Point[pointSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=hilbertPartitioning.getGrids();
        }
        else if(gridType.equals("rtree"))
        {
        	RtreePartitioning rtreePartitioning=new RtreePartitioning(pointSampleList.toArray(new Point[pointSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=rtreePartitioning.getGrids();
        }
        else if(gridType.equals("voronoi"))
        {
        	VoronoiPartitioning voronoiPartitioning=new VoronoiPartitioning(pointSampleList.toArray(new Point[pointSampleList.size()]),this.boundaryEnvelope,numPartitions);
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

        if (this.gridPointRDD == null) {

        	
        	//This index is built on top of unpartitioned SRDD
            this.indexedRDDNoId =  this.rawPointRDD.mapPartitions(new FlatMapFunction<Iterator<Point>,STRtree>()
            		{
						@Override
						public Iterator<STRtree> call(Iterator<Point> t)
								throws Exception {
							// TODO Auto-generated method stub
							 STRtree rt = new STRtree();
							while(t.hasNext()){
								Point point=t.next();
			                    rt.insert(point.getEnvelopeInternal(), point);
							}
							HashSet<STRtree> result = new HashSet<STRtree>();
							rt.query(new Envelope(0.0,0.0,0.0,0.0));
			                    result.add(rt);
			                    return result.iterator();
						}
            	
            		}).persist(StorageLevel.MEMORY_ONLY());
            //this.rawPointRDD.unpersist();
            
        }
        else{

        //Use GroupByKey, since I have repartition data, it should be much faster.
        //todo: Need to test performance here...
        JavaPairRDD<Integer, Iterable<Point>> gridedPointListRDD = this.gridPointRDD.groupByKey();

       
        this.indexedRDD = gridedPointListRDD.flatMapValues(new Function<Iterable<Point>, Iterable<STRtree>>() {
            @Override
            public Iterable<STRtree> call(Iterable<Point> points) throws Exception {
                STRtree rt = new STRtree();
                Iterator<Point> iterator=points.iterator();
                while(iterator.hasNext()){
                	Point point =iterator.next();
                    rt.insert(point.getEnvelopeInternal(), point);
                }
                rt.query(new Envelope(0.0,0.0,0.0,0.0));
                HashSet<STRtree> result = new HashSet<STRtree>();
                result.add(rt);
                return result;
            }
        }
        );
        this.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
        //this.rawPointRDD.unpersist();
        }
    }

    /**
     * Get the raw SpatialRDD
     *
     * @return the raw SpatialRDD
     */
    public JavaRDD<Point> getRawPointRDD() {
        return rawPointRDD;
    }

    /**
     * Set the raw SpatialRDD.
     *
     * @param rawPointRDD One existing SpatialRDD
     */
    public void setRawPointRDD(JavaRDD<Point> rawPointRDD) {
        this.rawPointRDD = rawPointRDD;
    }

    /**
     * Return the boundary of the entire SpatialRDD in terms of an envelope format
     *
     * @return the envelope
     */
    public Envelope boundary() {
        Double minLongitude = this.rawPointRDD
                .min((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
        Double maxLongitude = this.rawPointRDD
                .max((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
        Double minLatitude = this.rawPointRDD
                .min((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
        Double maxLatitude = this.rawPointRDD
                .max((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
        this.boundary[0] = minLongitude;
        this.boundary[1] = minLatitude;
        this.boundary[2] = maxLongitude;
        this.boundary[3] = maxLatitude;
        this.boundaryEnvelope = new Envelope(minLongitude, maxLongitude, minLatitude, maxLatitude);
        return boundaryEnvelope;
    }

    /**
     * Output the raw RDD as GeoJSON
     * @param outputLocation Output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawPointRDD.mapPartitions(new FlatMapFunction<Iterator<Point>, String>() {
            @Override
            public Iterator<String> call(Iterator<Point> pointIterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (pointIterator.hasNext()) {
                    GeoJSON json = writer.write(pointIterator.next());
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }
    
	/**
	 * Repartition the raw SpatialRDD.
	 *
	 * @param partitions the partitions number
	 * @return the repartitioned raw SpatialRDD
	 */
	public JavaRDD<Point> rePartition(Integer partitions)
	{
		return this.rawPointRDD.repartition(partitions);
	}
	

}
