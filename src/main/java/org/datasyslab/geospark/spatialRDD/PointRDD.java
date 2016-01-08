/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang.IllegalClassException;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;


import scala.Tuple2;

import org.apache.spark.storage.StorageLevel;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.datasyslab.geospark.utils.*;
;
import org.apache.log4j.Logger;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

// TODO: Auto-generated Javadoc
class PointFormatMapper implements Serializable, Function<String, Point> {
	Integer offset = 0;
	String splitter = "csv";


	public PointFormatMapper(Integer Offset, String Splitter) {
		this.offset = Offset;
		this.splitter = Splitter;
	}

	public Point call(String s) {
		String seperater = ",";
		if (this.splitter.contains("tsv")) {
			seperater = "\t";
		} else {
			seperater = ",";
		}
		GeometryFactory fact = new GeometryFactory();
		List<String> input = Arrays.asList(s.split(seperater));
		Coordinate coordinate = new Coordinate(Double.parseDouble(input.get(0 + this.offset)),
				Double.parseDouble(input.get(1 + this.offset)));
		Point point = fact.createPoint(coordinate);
		return point;
	}
}

/**
 * The Class PointRDD.
 */
public class PointRDD implements Serializable {

    static Logger log = Logger.getLogger(PointFormatMapper.class.getName());
    public long totalNumberOfRecords;
    /** The point rdd. */
	private JavaRDD<Point> pointRDD;


    /**
     * The boundary of this RDD, calculated in constructor method,
     */
    public Double[] boundary = new Double[4];

    /**
     * The boundary of this RDD, calculated in constructor method.
     */
    public Envelope boundaryEnvelope;
    /**
	 * Instantiates a new point rdd.
	 *
	 * @param pointRDD
	 *            the point rdd
	 */
	public PointRDD(JavaRDD<Point> pointRDD) {
		this.setPointRDD(pointRDD.cache());
	}

	//Define a JavaPairRDD<Int, Point>
	public JavaPairRDD<Integer, Point> gridPointRDD;
	//Define a JavaPairRDD<Int, STRTREE>
	public JavaPairRDD<Integer,STRtree> indexedRDD;

	public ArrayList<Double> getGrid() {
		return grid;
	}

	//Variable Table: Grid
    public ArrayList<Double> grid;
    /**
     * gridType, could be "x", "y". Later we will add kd-tree based grid.
     */
    String gridType;
	//List of duplicates... 如果在grid 边界上? 一个点在两个partition里都有. 那么



	//突然感觉如果join的第二个参数如果是set, 是不是比较好.


	/*
		Method: Cache Grided RDD.
	 */
	/*
		Method: Build Grid, do a Spark Sample. Should create a Table.
	 */

	//对于有多种实现的, 应该怎么写代码呢? 通过参数?
	/*
		Method: Build Index,
		Need to make sure we have a table of grid
	 */


	//Method: Write RDD to GeoJSON

	//Method: Write RDD to ShapeFile

	/**
	 * Instantiates a new point rdd.
	 *
	 * @param spark
	 *            the spark
	 * @param InputLocation
	 *            the input location
	 * @param Offset
	 *            the offset
	 * @param Splitter
	 *            the splitter
	 * @param partitions
	 *            the partitions
	 */
	public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {
		// final Integer offset=Offset;
		this.setPointRDD(
				spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter)).cache());
	}

	/**
	 *
	 * New pointRDD constructure crate by Jinxuan Wu.
	 *
	 * @param sc
	 * @param InputLocation
	 * @param offset
	 * @param splitter
	 * @param gridType
	 * @param numPartitions, althouth this is provided by the user, but should be multiple of number of cores.
	 */
	//Create By Jinxuan Wu.
	//
	public PointRDD(JavaSparkContext sc, String InputLocation, Integer offset, String splitter, String gridType, Integer numPartitions) {
		this.pointRDD = sc.textFile(InputLocation, numPartitions).map(new PointFormatMapper(offset, splitter));

		this.totalNumberOfRecords = this.pointRDD.count();

		//Calculate the number of samples we need to take.
		int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

		if( sampleNumberOfRecords == 0) {
            log.warn("the number of input is too small, no need to create a grid");
            //todo: Fix it later, Unable to return null..
		}

		//Take Sample
        ArrayList<Point> pointSampleList = new ArrayList<Point>(this.pointRDD.takeSample(false, sampleNumberOfRecords));
		//Sort
        //todo: Switch Case
		//Cause troubles in scala.
        Collections.sort(pointSampleList, new PointXComparator());

        //Get minX and minY;
        this.boundary();

		//Pick and create boundary;
        int curLocation = 0;
        int step = sampleNumberOfRecords / numPartitions;
		//If step = 0, which  means data set is really small, choose the
        grid = new ArrayList<Double> ();

        while(curLocation < sampleNumberOfRecords) {
            grid.add(pointSampleList.get(curLocation).getX());
            curLocation += step; // 0 + 10?
        }
        grid.set(0, boundary[0]);
        grid.set(grid.size() - 1, boundary[2]);
        // We can make this grid as broadcast variable:
        final Broadcast<ArrayList<Double>> gridBroadcasted= sc.broadcast(grid);
        //todo: I'm not sure, make be change to mapToPartitions will be faster?
		//Using search insert position. This is not correct... where does that
		JavaPairRDD<Integer, Point> unPartitionedGridPointRDD = this.pointRDD.mapToPair(new PairFunction<Point, Integer, Point>() {
			@Override
			public Tuple2<Integer, Point> call(Point point) throws Exception {
				//Do a Binary Search..
				ArrayList<Double> grid = gridBroadcasted.getValue();
				//Binary search to find index;
				//In binary high is the index number that is just smaller than target.
//				int low = 0, high = grid.size();
//				int result = 0;
//				while (low <= high) {
//					int mid = (low + high) / 2;
//					//todo: Double is not accurate..
//					if (Double.compare(grid.get(mid).doubleValue(), point.getX()) == 1 )
//						high = mid - 1;
//					else if (Double.compare(grid.get(mid).doubleValue(), point.getX()) == -1)
//						low = mid + 1;
//					else {
//						high = mid;
//						//check correctness later
//						break;
//					}
//				}
				int begin = 0, end = grid.size() - 1;

				int result = 0;
				while( begin < end ) {
					int mid = (begin + end) / 2;
					if(Double.compare(grid.get(mid).doubleValue(), point.getX()) == -1) {
						begin = mid + 1;
					}
					else
						end = mid;
				}

				result = begin;

				return new Tuple2<Integer, Point>(result, point);
			}
		});
		//Note, we don't need cache this RDD in memory, store it in the disk is enough.
		//todo, may be refactor this so that user have choice.
		//todo, measure this part's time. Using log4j debug level.
		//todo, This hashPartitioner could be improved by a simple %, becuase we know they're parition number.
		this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.DISK_ONLY());

	}


    /**
     * @author Jinxuan Wu
     *
     * Create an IndexedRDD and cached it. Need to have a grided RDD first.
     */
    public void buildIndex(String indexType) {

        if (this.gridPointRDD == null) {
            throw new IllegalClassException("To build index, you must build grid first");
        }

		//Use GroupByKey, since I have repartition data, it should be much faster.
		//todo: Need to test performance here...
		JavaPairRDD<Integer, Iterable<Point>> gridedPointListRDD = this.gridPointRDD.groupByKey();

		this.indexedRDD = gridedPointListRDD.flatMapValues(new Function<Iterable<Point>, Iterable<STRtree>>() {
			@Override
			public Iterable<STRtree> call(Iterable<Point> points) throws Exception {
				STRtree rt = new STRtree();
				for(Point p:points)
					rt.insert(p.getEnvelopeInternal(), p);
				ArrayList<STRtree> result = new ArrayList<STRtree>();
				result.add(rt);
				return result;
			}
		});

		this.indexedRDD.cache();
    }

	/**
	 * Instantiates a new point rdd.
	 *
	 * @param spark
	 *            the spark
	 * @param InputLocation
	 *            the input location
	 * @param Offset
	 *            the offset
	 * @param Splitter
	 *            the splitter
	 */
	public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {
		// final Integer offset=Offset;
		this.setPointRDD(spark.textFile(InputLocation).map(new PointFormatMapper(Offset, Splitter)).cache());
	}

	/**
	 * Gets the point rdd.
	 *
	 * @return the point rdd
	 */
	public JavaRDD<Point> getPointRDD() {
		return pointRDD;
	}

	/**
	 * Sets the point rdd.
	 *
	 * @param pointRDD
	 *            the new point rdd
	 */
	public void setPointRDD(JavaRDD<Point> pointRDD) {
		this.pointRDD = pointRDD;
	}

	/**
	 * Re partition.
	 *
	 * @param partitions
	 *            the partitions
	 * @return the java rdd
	 */
	public JavaRDD<Point> rePartition(Integer partitions) {
		return this.pointRDD.repartition(partitions);
	}



	/**
	 * Boundary.
	 *
	 * @return the envelope
	 */
	public Envelope boundary() {
		Double minLongitude = this.pointRDD
				.min((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
		Double maxLongitude = this.pointRDD
				.max((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
		Double minLatitude = this.pointRDD
				.min((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
		Double maxLatitude = this.pointRDD
				.max((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
		this.boundary[0] = minLongitude;
        this.boundary[1] = minLatitude;
        this.boundary[2] = maxLongitude;
        this.boundary[3] = maxLatitude;
        this.boundaryEnvelope = new Envelope(minLongitude, maxLongitude, minLatitude, maxLatitude);
		return boundaryEnvelope;
	}

	public void saveAsGeoJSON(String outputLocation){
		this.pointRDD.mapPartitions(new FlatMapFunction<Iterator<Point>, String>() {
			@Override
			public Iterable<String> call(Iterator<Point> pointIterator) throws Exception {
				ArrayList<String> result = new ArrayList<String>();
				GeoJSONWriter writer = new GeoJSONWriter();
				while(pointIterator.hasNext()) {
					GeoJSON json = writer.write(pointIterator.next());
					String jsonstring = json.toString();
					result.add(jsonstring);
				}
				return result;
			}
		}).saveAsTextFile(outputLocation);
	}

}
