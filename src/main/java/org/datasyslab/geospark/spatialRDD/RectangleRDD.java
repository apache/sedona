/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


import com.vividsolutions.jts.geom.Envelope;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.utils.*;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
class RectangleFormatMapper implements Serializable,Function<String,Envelope>
{
	Integer offset=0;
	String splitter="csv";
	public RectangleFormatMapper(Integer Offset,String Splitter)
	{
		this.offset=Offset;
		this.splitter=Splitter;
	}
	public Envelope call(String s)
	{	
		
		String seperater=",";
		if(this.splitter.contains("tsv"))
		{
			seperater="\t";
		}
		else if(this.splitter.contains("tsv"))
		{
			seperater=",";
		}
		 List<String> input=Arrays.asList(s.split(seperater));
		 Envelope envelope = new Envelope(Double.parseDouble(input.get(0+offset)),Double.parseDouble(input.get(2+offset)),Double.parseDouble(input.get(1+offset)),Double.parseDouble(input.get(3+offset)));
		 return envelope;
	}
}

/**
 * The Class RectangleRDD.
 */
public class RectangleRDD implements Serializable {

	public JavaPairRDD<Integer, Envelope> gridRectangleRDD;
	public long totalNumberofRecords;
	/** The rectangle rdd. */
	public JavaRDD<Envelope> rectangleRDD;

	ArrayList<Double> grid;

	Double[] boundary = new Double[4];
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param rectangleRDD the rectangle rdd
	 */
	public RectangleRDD(JavaRDD<Envelope> rectangleRDD)
	{
		this.setRectangleRDD(rectangleRDD.cache());
	}
	
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 * @param partitions the partitions
	 */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}
	
	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		//final Integer offset=Offset;
		this.setRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}

	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType, Integer numPartitions) {
		this.rectangleRDD = sc.textFile(inputLocation, numPartitions).map(new RectangleFormatMapper(offSet, splitter));

		this.totalNumberofRecords = this.rectangleRDD.count();

		int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberofRecords);
		ArrayList<Envelope> rectangleSampleList = new ArrayList<Envelope> (rectangleRDD.takeSample(false, sampleNumberOfRecords));

		Collections.sort(rectangleSampleList, new RectangleXMinComparator());

		this.boundary();

		grid = new ArrayList<Double>();
		int curLocation = 0;
		int step = sampleNumberOfRecords / numPartitions;

		while(curLocation < sampleNumberOfRecords) {
			grid.add(rectangleSampleList.get(curLocation).getMinX());
			curLocation += step;
		}
		grid.set(0, boundary[0]);
		grid.set(grid.size() - 1, boundary[2]);

		final Broadcast<ArrayList<Double>> gridBroadcasted= sc.broadcast(grid);
		//todo: I'm not sure, make be change to mapToPartitions will be faster?
		JavaPairRDD<Integer, Envelope> unPartitionedGridRectangleRDD = this.rectangleRDD.mapToPair(new PairFunction<Envelope, Integer, Envelope>() {
			@Override
			public Tuple2<Integer, Envelope> call(Envelope envelope) throws Exception {
				//Do a Binary Search..
				ArrayList<Double> grid = gridBroadcasted.getValue();
				//Binary search to find index;
				int low = 0, high = grid.size();
				int result = 0;
				while (low <= high) {
					int mid = (low + high) / 2;
					//todo: Double is not accurate..
					if (grid.get(mid) > envelope.getMinX())
						high = mid - 1;
					else if (grid.get(mid) < envelope.getMinX())
						low = mid + 1;
					else {
						result = mid;
						break;
					}
				}
				if (result != (low + high) / 2)
					result = high;

				return new Tuple2<Integer, Envelope>(result, envelope);
			}
		});

		this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.DISK_ONLY());
	}



	/**
	 * Gets the rectangle rdd.
	 *
	 * @return the rectangle rdd
	 */
	public JavaRDD<Envelope> getRectangleRDD() {
		return rectangleRDD;
	}
	
	/**
	 * Sets the rectangle rdd.
	 *
	 * @param rectangleRDD the new rectangle rdd
	 */
	public void setRectangleRDD(JavaRDD<Envelope> rectangleRDD) {
		this.rectangleRDD = rectangleRDD;
	}
	
	/**
	 * Re partition.
	 *
	 * @param partitions the partitions
	 * @return the java rdd
	 */
	public JavaRDD<Envelope> rePartition(Integer partitions)
	{
		return this.rectangleRDD.repartition(partitions);
	}
	
	
	
	/**
	 * Boundary.
	 *
	 * @return the envelope
	 */
	public Envelope boundary()
	{
		Double minLongtitude1=this.rectangleRDD.min((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double maxLongtitude1=this.rectangleRDD.max((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min")).getMinX();
		Double minLatitude1=this.rectangleRDD.min((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double maxLatitude1=this.rectangleRDD.max((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min")).getMinY();
		Double minLongtitude2=this.rectangleRDD.min((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double maxLongtitude2=this.rectangleRDD.max((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max")).getMaxX();
		Double minLatitude2=this.rectangleRDD.min((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
		Double maxLatitude2=this.rectangleRDD.max((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max")).getMaxY();
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
		return new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
	}

}
