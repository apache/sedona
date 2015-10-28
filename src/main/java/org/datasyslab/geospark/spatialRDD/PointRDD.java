/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;


import scala.Tuple2;
import org.datasyslab.geospark.partition.*;
import org.datasyslab.geospark.rangeFilter.PointRangeFilter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import org.datasyslab.geospark.utils.*;
import org.datasyslab.geospark.gemotryObjects.*;
import org.datasyslab.geospark.boundryFilter.*;;

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

	/** The point rdd. */
	private JavaRDD<Point> pointRDD;

	/**
	 * Instantiates a new point rdd.
	 *
	 * @param pointRDD
	 *            the point rdd
	 */
	public PointRDD(JavaRDD<Point> pointRDD) {
		this.setPointRDD(pointRDD.cache());
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
	 * @param partitions
	 *            the partitions
	 */
	public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {
		// final Integer offset=Offset;
		this.setPointRDD(
				spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter)).cache());
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
		Double[] boundary = new Double[4];

		Double minLongitude = this.pointRDD
				.min((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
		Double maxLongitude = this.pointRDD
				.max((PointXComparator) GeometryComparatorFactory.createComparator("point", "x")).getX();
		Double minLatitude = this.pointRDD
				.min((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
		Double maxLatitude = this.pointRDD
				.max((PointYComparator) GeometryComparatorFactory.createComparator("point", "y")).getY();
		boundary[0] = minLongitude;
		boundary[1] = minLatitude;
		boundary[2] = maxLongitude;
		boundary[3] = maxLatitude;
		return new Envelope(minLongitude, maxLongitude, minLatitude, maxLatitude);
	}
}
