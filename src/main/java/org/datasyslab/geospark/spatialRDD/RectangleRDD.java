/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.commons.lang.IllegalClassException;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.gemotryObjects.EnvelopeWithGrid;
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
import java.util.List;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
class RectangleFormatMapper implements Serializable,Function<String,Envelope>
{
	Integer offset = 0;
	String splitter = "csv";

	public RectangleFormatMapper(Integer Offset, String Splitter) {
		this.offset = Offset;
		this.splitter = Splitter;
	}

	public RectangleFormatMapper( String Splitter) {
		this.offset = 0;
		this.splitter = Splitter;
	}

	public Envelope call(String line) throws Exception {
		Envelope rectangle = null;
		GeometryFactory fact = new GeometryFactory();
		List<String> lineSplitList;
		Coordinate coordinate;
		Double x1,x2,y1,y2;
		switch (splitter) {
			case "csv":
				lineSplitList = Arrays.asList(line.split(","));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				break;
			case "tsv":
				lineSplitList = Arrays.asList(line.split(","));
				x1 = Double.parseDouble(lineSplitList.get(offset));
				x2 = Double.parseDouble(lineSplitList.get(offset + 2));
				y1 = Double.parseDouble(lineSplitList.get(offset + 1));
				y2 = Double.parseDouble(lineSplitList.get(offset + 3));
				rectangle = new Envelope(x1, x2, y1, y2);
				break;
			case "geojson":
				GeoJSONReader reader = new GeoJSONReader();
				Geometry result = reader.read(line);
				rectangle =result.getEnvelopeInternal();
				break;
			case "wtk":
				WKTReader wtkreader = new WKTReader();
				try {
					rectangle = wtkreader.read(line).getEnvelopeInternal();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				break;
			default:
				throw new Exception("Input type not recognized, ");
		}
		return rectangle;
	}
}

/**
 * The Class RectangleRDD.
 */
public class RectangleRDD implements Serializable {



	public long totalNumberOfRecords;
	/** The rectangle rdd. */
	public JavaRDD<Envelope> rawRectangleRDD;

	ArrayList<Double> grid;

	Double[] boundary = new Double[4];

	public Envelope boundaryEnvelope;

	public JavaPairRDD<Integer, Envelope> gridRectangleRDD;

	public ArrayList<EnvelopeWithGrid> grids;


	//todo, replace this STRtree to be more generalized, such as QuadTree.
	public JavaPairRDD<Integer, STRtree> indexedRDD;

	/**
	 * Instantiates a new rectangle rdd.
	 *
	 * @param rawRectangleRDD the rectangle rdd
	 */
	public RectangleRDD(JavaRDD<Envelope> rawRectangleRDD)
	{
		this.setRawRectangleRDD(rawRectangleRDD.cache());
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
		this.setRawRectangleRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter)).cache());
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
		this.setRawRectangleRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter)).cache());
	}

	public RectangleRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType, Integer numPartitions) {
		this.rawRectangleRDD = sc.textFile(inputLocation).map(new RectangleFormatMapper(offSet, splitter));

		totalNumberOfRecords = this.rawRectangleRDD.count();

		int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

		ArrayList<Envelope> rectangleSampleList = new ArrayList<Envelope> (rawRectangleRDD.takeSample(false, sampleNumberOfRecords));

		this.boundary();

		//Sort
		Comparator<Envelope> comparator = null;
		switch (gridType) {
			case "X":
				comparator = new RectangleXMinComparator();
				break;
			case "Y":
				comparator = new RectangleYMinComparator();
				break;
			case "X-Y":
				//Will first sort based on X, then partition, then sort and partition based on Y.
				comparator = new RectangleXMinComparator();
				break;
			case "STRtree":
				throw new IllegalArgumentException("STRtree is currently now implemented, to be finished in version 0.3");
			default:
				throw new IllegalArgumentException("Grid type not recognied, please check again.");
		}



		GeometryFactory geometryFactory = new GeometryFactory();
		
		rectangleSampleList.set(0, new Envelope(boundary[0], boundary[0],boundary[2], boundary[2] ));
		rectangleSampleList.set(sampleNumberOfRecords-1, new Envelope(boundary[1], boundary[1],boundary[3], boundary[3] ));
		JavaPairRDD<Integer, Envelope> unPartitionedGridPointRDD;
		
		if(sampleNumberOfRecords == 0) {
			//If the sample Number is too small, we will just use one grid instead.
			System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
			System.err.println("The sample size is " + totalNumberOfRecords /100);
			System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
			System.err.println("we will just build one grid for all input");
			grids = new ArrayList<EnvelopeWithGrid>();
			grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
		} else if (gridType.equals("X")) {

			Collections.sort(rectangleSampleList, comparator);
			//Below is for X and Y grid.
			int curLocation = 0;
			int step = sampleNumberOfRecords / numPartitions;
			//If step = 0, which  means data set is really small, choose the


			//Find Mid Point
			ArrayList<Point> xAxisBar = new ArrayList<Point>();
			while (curLocation < sampleNumberOfRecords) {
				Double x = rectangleSampleList.get(curLocation).getMinX();
				xAxisBar.add(geometryFactory.createPoint(new Coordinate(x, x)));
				curLocation += step;
			}

			ArrayList<Double> midPointList = new ArrayList<Double>();

			midPointList.add(boundaryEnvelope.getMinX());
			for(int i = 1; i < xAxisBar.size() - 1; i++) {
				midPointList.add((xAxisBar.get(i).getX() + xAxisBar.get(i + 1).getX()) / 2);
			}
			midPointList.add(boundaryEnvelope.getMaxX());
			int index = 0;
			grids = new ArrayList<EnvelopeWithGrid>(midPointList.size() - 1);
			for(int i = 0; i < midPointList.size() - 1; i++) {
				grids.add(new EnvelopeWithGrid(midPointList.get(i), midPointList.get(i + 1), boundaryEnvelope.getMinY(), boundaryEnvelope.getMaxY(), index));
				index++;
			}
		} else if (gridType.equals("Y")) {
			Collections.sort(rectangleSampleList, comparator);
			//Below is for X and Y grid.
			int curLocation = 0;
			int step = sampleNumberOfRecords / numPartitions;
			//If step = 0, which  means data set is really small, choose the
			GeometryComparatorFactory geometryComparatorFactory = new GeometryComparatorFactory();

			//Find Mid Point
			ArrayList<Point> yAxisBar = new ArrayList<Point>();
			while (curLocation < sampleNumberOfRecords) {
				Double y = rectangleSampleList.get(curLocation).getMinX();
				yAxisBar.add(geometryFactory.createPoint(new Coordinate(y, y)));
				curLocation += step;
			}

			ArrayList<Double> midPointList = new ArrayList<Double>();

			midPointList.add(boundaryEnvelope.getMinX());
			for(int i = 1; i < yAxisBar.size() - 1; i++) {
				midPointList.add((yAxisBar.get(i).getY() + yAxisBar.get(i + 1).getY()) / 2);
			}
			midPointList.add(boundaryEnvelope.getMaxY());
			int index = 0;
			grids = new ArrayList<EnvelopeWithGrid>(midPointList.size() - 1);
			for(int i = 0; i < midPointList.size() - 1; i++) {
				grids.add(new EnvelopeWithGrid(boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX(),midPointList.get(i), midPointList.get(i + 1),  index));
				index++;
			}
		} else if (gridType.equals("X-Y")) {
			//Ideally we want, One partition => One Grid. And each partition have equal size.
			//We use upper bound of sqrt(# of partition) for X and Y.
			Collections.sort(rectangleSampleList, comparator);

			//Expand the sampleList to include boundary.

			//todo: [Verify] Duplicate should not be a problem right?

			//We need to create upper(n) envelope...
			//todo: May be use bar instead of vertices? This variable name need to refactor

			Integer stepInXAxis = sampleNumberOfRecords / numPartitions;
			Integer stepInYAxis = stepInXAxis / numPartitions;

			if(stepInYAxis == 0 || stepInXAxis == 0) {
				throw new IllegalArgumentException("[Error]" +
						sampleNumberOfRecords +
						"The number of partitions you provided is too large. I'm unable to build" +
						"that Grid by sampling 1 percent of total record");
			}
			int index = 0;

			grids = new ArrayList<EnvelopeWithGrid>();

			//XAxis
			ArrayList<Double> xAxisMidPointList = new ArrayList<Double>();
			ArrayList<Point> xAxisSubList = new ArrayList<Point>();

			for (int i = 0; i < sampleNumberOfRecords; i += stepInXAxis) {
				Double x = rectangleSampleList.get(i).getMinX();
				xAxisSubList.add(geometryFactory.createPoint(new Coordinate(x, x)));
			}

			//Build MidPoint
			xAxisMidPointList.add(boundaryEnvelope.getMinX());
			for(int j = 0; j < xAxisSubList.size() - 1; j++) {
				Double mid = (xAxisSubList.get(j).getX() + xAxisSubList.get(j+1).getX())/2;
				xAxisMidPointList.add(mid);
			}
			xAxisMidPointList.add(boundaryEnvelope.getMaxX());

			//yAxis

			for(int j = 0; j < xAxisSubList.size() ;j++) {
				//Fetch the X bar.

				ArrayList<Envelope> xAxisBar = new ArrayList<Envelope>(rectangleSampleList.subList(j*stepInXAxis, (j+1)*stepInXAxis));

				Collections.sort(xAxisBar, new RectangleYMinComparator());

				//Pick y Axis.
				ArrayList<Point> yAxisSubList = new ArrayList<Point>();

				for(int k = 0; k < xAxisBar.size(); k+=stepInYAxis) {
					Double y = rectangleSampleList.get(k).getMinY();
					yAxisSubList.add(geometryFactory.createPoint(new Coordinate(y, y)));
				}
				
				//Calculate midpoint.


				ArrayList<Double> yAxisMidPointList = new ArrayList<Double>(yAxisSubList.size() - 1);
				yAxisMidPointList.add((boundaryEnvelope.getMinY()));
				for(int k = 0; k < yAxisSubList.size() - 1; k++) {
					Double mid = (yAxisSubList.get(k).getY() + yAxisSubList.get(k+1).getY()) / 2;
					yAxisMidPointList.add(mid);
				}
				yAxisMidPointList.add(boundaryEnvelope.getMaxY());

				//Build Grid.
				//x1,x2,y1,y2
				for(int k = 0; k < yAxisMidPointList.size() - 1; k++ ) {
					grids.add(new EnvelopeWithGrid(xAxisMidPointList.get(j), xAxisMidPointList.get(j + 1), yAxisMidPointList.get(k), yAxisMidPointList.get(k + 1), index));
					index++;
				}
			}
		}

		final Broadcast<ArrayList<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
		JavaPairRDD<Integer,Envelope> unPartitionedGridRectangleRDD = this.rawRectangleRDD.flatMapToPair(
				new PairFlatMapFunction<Envelope, Integer, Envelope>() {
					@Override
					public Iterable<Tuple2<Integer, Envelope>> call(Envelope envelope) throws Exception {
						ArrayList<Tuple2<Integer, Envelope>> result = new ArrayList<Tuple2<Integer, Envelope>>();
						for (EnvelopeWithGrid e : gridEnvelopBroadcasted.getValue()) {
							if (e.intersects(envelope)) {
								result.add(new Tuple2<Integer, Envelope>(e.grid, envelope));
							}
						}

						if (result.size() == 0) {
							//Should never goes here..
							throw new Exception("[Error]" +
									envelope.toString() +
									"The grid must have errors, it should at least have one grid contain this point");
						}
						return result;
					}
				}
		);

		this.gridRectangleRDD = unPartitionedGridRectangleRDD.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.DISK_ONLY());
	}


	/**
	 * @author Jinxuan Wu
	 *
	 * Create an IndexedRDD and cached it. Need to have a grided RDD first.
	 */
	public void buildIndex(String indexType) {

		if (this.gridRectangleRDD == null) {
			throw new IllegalClassException("To build index, you must build grid first");
		}

		//Use GroupByKey, since I have repartition data, it should be much faster.
		//todo: Need to test performance here...
		JavaPairRDD<Integer, Iterable<Envelope>> gridedRectangleListRDD = this.gridRectangleRDD.groupByKey();

		this.indexedRDD = gridedRectangleListRDD.flatMapValues(new Function<Iterable<Envelope>, Iterable<STRtree>>() {
			@Override
			public Iterable<STRtree> call(Iterable<Envelope> envelopes) throws Exception {
				STRtree rt = new STRtree();
				GeometryFactory geometryFactory = new GeometryFactory();
				for (Envelope e : envelopes)
					try {
						rt.insert(e, geometryFactory.toGeometry(e));
					} catch (ClassCastException e1) {
					}
				ArrayList<STRtree> result = new ArrayList<STRtree>();
				result.add(rt);
				return result;
			}
		});
		this.indexedRDD.cache();
	}
	/**
	 * Gets the rectangle rdd.
	 *
	 * @return the rectangle rdd
	 */
	public JavaRDD<Envelope> getRawRectangleRDD() {
		return rawRectangleRDD;
	}
	
	/**
	 * Sets the rectangle rdd.
	 *
	 * @param rawRectangleRDD the new rectangle rdd
	 */
	public void setRawRectangleRDD(JavaRDD<Envelope> rawRectangleRDD) {
		this.rawRectangleRDD = rawRectangleRDD;
	}
	
	/**
	 * Re partition.
	 *
	 * @param partitions the partitions
	 * @return the java rdd
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

}
