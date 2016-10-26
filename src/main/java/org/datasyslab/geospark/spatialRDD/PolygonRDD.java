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
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
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
 * The Class PolygonRDD. It accommodates Polygon object.
 * @author Arizona State University DataSystems Lab
 *
 */
public class PolygonRDD implements Serializable {

    /**
     * The total number of records stored in this RDD
     */
    public long totalNumberOfRecords;
    
    /**
     * The original Spatial RDD which has not been spatial partitioned and has not index
     */
    public JavaRDD<Polygon> rawPolygonRDD;
   
    
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
    public JavaPairRDD<Integer, Polygon> gridPolygonRDD;

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
    
    private Envelope minXEnvelope;
    private Envelope minYEnvelope;
    private Envelope maxXEnvelope;
    private Envelope maxYEnvelope;


    /**
     * Initialize one SpatialRDD with one existing SpatialRDD
     * @param rawPolygonRDD One existing raw SpatialRDD
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.setRawPolygonRDD(rawPolygonRDD.persist(StorageLevel.MEMORY_ONLY()));
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     * @param partitions specify the partition number of the SpatialRDD
     */ 
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {

        this.setRawPolygonRDD(spark.textFile(InputLocation, partitions).map(new PolygonFormatMapper(Offset, Splitter)));//.persist(StorageLevel.MEMORY_ONLY()));
    }

    /**
     * Initialize one raw SpatialRDD with a raw input file
     * @param spark SparkContext which defines some Spark configurations
     * @param InputLocation specify the input path which can be a HDFS path
     * @param Offset specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param Splitter specify the input file format: csv, tsv, geojson, wkt
     */
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {

        this.setRawPolygonRDD(spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset, Splitter)));//.persist(StorageLevel.MEMORY_ONLY()));
    }



    
    /**
     * Get the raw SpatialRDD
     *
     * @return The raw SpatialRDD
     */
    public JavaRDD<Polygon> getRawPolygonRDD() {
        return rawPolygonRDD;
    }

    /**
     * Transform a rawRectangleRDD to a RectangelRDD with a a specified spatial partitioning grid type and the number of partitions
     * @param rawPolygonRDD
     * @param gridType
     * @param numPartitions
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD, String gridType, Integer numPartitions)
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
     * Transform a rawRectangleRDD to a RectangelRDD with a a specified spatial partitioning grid type and the number of partitions
     * @param rawPolygonRDD
     * @param gridType
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD, String gridType)
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
     * Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it
     * @param sc spark SparkContext which defines some Spark configurations
     * @param inputLocation specify the input path which can be a HDFS path
     * @param offSet specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     * @param numPartitions specify the partition number of the SpatialRDD
     */
    public PolygonRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType, Integer numPartitions) {
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
     *Initialize one raw SpatialRDD with a raw input file and do spatial partitioning on it without specifying the number of partitions.
     * @param sc spark SparkContext which defines some Spark configurations
     * @param inputLocation specify the input path which can be a HDFS path
     * @param offSet specify the starting column of valid spatial attributes in CSV and TSV. e.g. XXXX,XXXX,x,y,XXXX,XXXX
     * @param splitter specify the input file format: csv, tsv, geojson, wkt
     * @param gridType specify the spatial partitioning method: equalgrid, rtree, voronoi
     */
    public PolygonRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType) {
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
    
    private void doSpatialPartitioning(String gridType, int numPartitions)
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
        }  else if (gridType.equals("equalgrid")) {
        	EqualPartitioning equalPartitioning =new EqualPartitioning(this.boundaryEnvelope,numPartitions);
        	grids=equalPartitioning.getGrids();
        }
        else if(gridType.equals("hilbert"))
        {
        	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(polygonSampleList.toArray(new Polygon[polygonSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=hilbertPartitioning.getGrids();
        }
        else if(gridType.equals("rtree"))
        {
        	RtreePartitioning rtreePartitioning=new RtreePartitioning(polygonSampleList.toArray(new Polygon[polygonSampleList.size()]),this.boundaryEnvelope,numPartitions);
        	grids=rtreePartitioning.getGrids();
        }
        else if(gridType.equals("voronoi"))
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
     * Create an IndexedRDD and cache it in memory. Need to have a grided RDD first. The index is build on each partition.
     * @param indexType Specify the index type: rtree, quadtree
     */
    public void buildIndex(String indexType) {

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
     * Set the raw SpatialRDD
     *
     * @param rawPolygonRDD One existing SpatialRDD
     */
    public void setRawPolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.rawPolygonRDD = rawPolygonRDD;
    }

	/**
	 * Repartition the raw SpatialRDD.
	 *
	 * @param partitions the partitions number
	 * @return the repartitioned raw SpatialRDD
	 */
	public JavaRDD<Polygon> rePartition(Integer partitions)
	{
		return this.rawPolygonRDD.repartition(partitions);
	}
	

    /**
     * Return the boundary of the entire SpatialRDD in terms of an envelope format
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
     * Return RectangleRDD version of the PolygonRDD. Each record in RectangleRDD is the Minimum bounding rectangle of the corresponding Polygon
     *
     * @return the rectangle rdd
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
     * Return a polygon which is the union of the entire polygon dataset
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

