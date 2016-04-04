/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

import org.apache.commons.lang.IllegalClassException;
import org.apache.log4j.Logger;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PointXComparator;
import org.datasyslab.geospark.utils.PointYComparator;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONReader;
import org.wololo.jts2geojson.GeoJSONWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

class PointFormatMapper implements Serializable, Function<String, Point> {
    Integer offset = 0;
    String splitter = "csv";

    public PointFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public PointFormatMapper( String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public Point call(String line) throws Exception {
        Point point = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                break;
            case "tsv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                break;
            case "geojson":
                GeoJSONReader reader = new GeoJSONReader();
                point = (Point)reader.read(line);
                break;
            case "wtk":
                WKTReader wtkreader = new WKTReader();
                try {
                    point = (Point)wtkreader.read(line);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return point;
    }
}

/**
 * The Class PointRDD.
 */
public class PointRDD implements Serializable {

    static Logger log = Logger.getLogger(PointFormatMapper.class.getName());
    public long totalNumberOfRecords;
    /**
     * The boundary of this RDD, calculated in constructor method,
     */
    public Double[] boundary = new Double[4];
    /**
     * The boundary of this RDD, calculated in constructor method.
     */
    public Envelope boundaryEnvelope;
    //Define a JavaPairRDD<Int, Point>
    public JavaPairRDD<Integer, Point> gridPointRDD;
    //Define a JavaPairRDD<Int, STRTREE>
    public JavaPairRDD<Integer, STRtree> indexedRDD;
    //Raw point RDD
    public JavaRDD<Point> rawPointRDD;

    public ArrayList<EnvelopeWithGrid> grids;

    /**
     * Instantiates a new point rdd.
     *
     * @param rawPointRDD the point rdd
     */
    public PointRDD(JavaRDD<Point> rawPointRDD) {
        this.setRawPointRDD(rawPointRDD);
    }

    /**
     * Instantiates a new point rdd.
     *
     * @param spark         the spark
     * @param InputLocation the input location
     * @param Offset        the offset
     * @param Splitter      the splitter
     * @param partitions    the partitions
     */
    public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {
        // final Integer offset=Offset;
        this.setRawPointRDD(
                spark.textFile(InputLocation, partitions).map(new PointFormatMapper(Offset, Splitter)).cache());
    }

    /**
     * New  constructor crate by Jinxuan Wu.
     *
     * This constructor is capable of build grided RDD.
     */
    public PointRDD(JavaSparkContext sc, String InputLocation, Integer offset, String splitter, String gridType, Integer numPartitions) {
        this.rawPointRDD = sc.textFile(InputLocation).map(new PointFormatMapper(offset, splitter));

        this.totalNumberOfRecords = this.rawPointRDD.count();

        //Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

        if(sampleNumberOfRecords == -1) {
            throw new IllegalArgumentException("The input size is smaller the number of grid, please reduce the grid size!");
        }
        //Take Sample
        //todo: This creates troubles.. In fact it should be List interface. But List reports problems in Scala Shell, seems that it can not implement polymorphism and Still use List.
        ArrayList<Point> pointSampleList = new ArrayList<Point>(this.rawPointRDD.takeSample(false, sampleNumberOfRecords));
        //Sort
        Comparator<Point> comparator = null;
        switch (gridType) {
            case "X":
                comparator = new PointXComparator();
                break;
            case "Y":
                comparator = new PointYComparator();
                break;
            case "X-Y":
                //Will first sort based on X, then partition, then sort and partition based on Y.
                comparator = new PointXComparator();
                break;
            case "STRtree":
                throw new IllegalArgumentException("STRtree is currently now implemented, to be finished in version 0.3");
            default:
                throw new IllegalArgumentException("Grid type not recognied, please check again.");
        }

        //Get minX and minY;
        this.boundary();

        GeometryFactory geometryFactory = new GeometryFactory();

        pointSampleList.set(0, geometryFactory.createPoint(new Coordinate(boundary[0], boundary[1])));
        pointSampleList.set(sampleNumberOfRecords-1, geometryFactory.createPoint(new Coordinate(boundary[2], boundary[3])));

        //Pick and create boundary;

        JavaPairRDD<Integer, Point> unPartitionedGridPointRDD;

        //This is the case where data size is too small. We will just create one grid
        if(sampleNumberOfRecords == 0) {
            //If the sample Number is too small, we will just use one grid instead.
            System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
            System.err.println("The sample size is " + totalNumberOfRecords/100);
            System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
            System.err.println("we will just build one grid for all input");
            grids = new ArrayList<EnvelopeWithGrid>();
            grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
        } else if (gridType.equals("X-Y")) {
            //Ideally we want, One partition => One Grid. And each partition have equal size.
            //We use upper bound of sqrt(# of partition) for X and Y.
            Collections.sort(pointSampleList, comparator);

            //todo: [Verify] Duplicate should not be a problem right?

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
                xAxisSubList.add(pointSampleList.get(i + stepInXAxis - 1));
            }

            //Build MidPoint
            xAxisMidPointList.add(boundaryEnvelope.getMinX());
            for(int j = 0; j < xAxisSubList.size() - 1; j++) {
                Double mid = (xAxisSubList.get(j).getX() + xAxisSubList.get(j+1).getX())/2;
                xAxisMidPointList.add(mid);
            }
            xAxisMidPointList.add(boundaryEnvelope.getMaxX());

            //yAxis

            for(int j = 0; j < xAxisSubList.size();j++) {
                //Fetch the X bar.

                ArrayList<Point> xAxisBar = new ArrayList<Point>(pointSampleList.subList(j*stepInXAxis, (j+1)*stepInXAxis));

                Collections.sort(xAxisBar, new PointYComparator());

                //Pick y Axis.
                ArrayList<Point> yAxisSubList = new ArrayList<Point>();

                for(int k = 0; k < xAxisBar.size(); k+=stepInYAxis) {
                    yAxisSubList.add(xAxisBar.get(k + stepInYAxis - 1));
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
        } else if (gridType.equals("X")) {
            Collections.sort(pointSampleList, comparator);
            //Below is for X and Y grid.
            int curLocation = 0;
            int step = sampleNumberOfRecords / numPartitions;
            //If step = 0, which  means data set is really small, choose the
            GeometryComparatorFactory geometryComparatorFactory = new GeometryComparatorFactory();

            //Find Mid Point
            ArrayList<Point> xAxisBar = new ArrayList<Point>();
            while (curLocation < sampleNumberOfRecords) {
                xAxisBar.add(pointSampleList.get(curLocation));
                curLocation += step;
            }

            ArrayList<Double> midPointList = new ArrayList<Double>();

            midPointList.add(boundaryEnvelope.getMinX());
            for(int i = 0; i < xAxisBar.size() - 1; i++) {
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
            Collections.sort(pointSampleList, comparator);
            //Below is for X and Y grid.
            int curLocation = 0;
            int step = sampleNumberOfRecords / numPartitions;
            //If step = 0, which  means data set is really small, choose the
            GeometryComparatorFactory geometryComparatorFactory = new GeometryComparatorFactory();

            //Find Mid Point
            ArrayList<Point> yAxisBar = new ArrayList<Point>();
            while (curLocation < sampleNumberOfRecords) {
                yAxisBar.add(pointSampleList.get(curLocation));
                curLocation += step;
            }

            ArrayList<Double> midPointList = new ArrayList<Double>();

            midPointList.add(boundaryEnvelope.getMinX());
            for(int i = 0; i < yAxisBar.size() - 1; i++) {
                midPointList.add((yAxisBar.get(i).getY() + yAxisBar.get(i + 1).getY()) / 2);
            }
            midPointList.add(boundaryEnvelope.getMaxY());
            int index = 0;
            grids = new ArrayList<EnvelopeWithGrid>(midPointList.size() - 1);
            for(int i = 0; i < midPointList.size() - 1; i++) {
                grids.add(new EnvelopeWithGrid(boundaryEnvelope.getMinX(), boundaryEnvelope.getMaxX(),midPointList.get(i), midPointList.get(i + 1),  index));
                index++;
            }
        }

        //Note, we don't need cache this RDD in memory, store it in the disk is enough.
        //todo, may be refactor this so that user have choice.
        //todo, measure this part's time. Using log4j debug level.
        //todo, This hashPartitioner could be improved by a simple %, becuase we know they're parition number.
        final Broadcast<ArrayList<EnvelopeWithGrid>> gridEnvelopBroadcasted = sc.broadcast(grids);
        unPartitionedGridPointRDD = this.rawPointRDD.mapToPair(
                new PairFunction<Point, Integer, Point>() {
                    @Override
                    public Tuple2<Integer, Point> call(Point point) throws Exception {
                        for (EnvelopeWithGrid e : gridEnvelopBroadcasted.getValue()) {
                            if (e.contains(point.getCoordinate())) {
                                return new Tuple2<Integer, Point>(e.grid, point);
                            }
                        }
                        //Should never goes here..
                        throw new Exception("[Error]" +
                                point.toString() +
                                "The grid must have errors, it should at least have one grid contain this point");
                    }
                }
        );
        this.gridPointRDD = unPartitionedGridPointRDD.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.DISK_ONLY());
    }

    /**
     * Instantiates a new point rdd.
     *
     * @param spark         the spark
     * @param InputLocation the input location
     * @param Offset        the offset
     * @param Splitter      the splitter
     */
    public PointRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {
        // final Integer offset=Offset;
        this.setRawPointRDD(spark.textFile(InputLocation).map(new PointFormatMapper(Offset, Splitter)).cache());
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
                for (Point p : points)
                    rt.insert(p.getEnvelopeInternal(), p);
                ArrayList<STRtree> result = new ArrayList<STRtree>();
                result.add(rt);
                return result;
            }
        });

        this.indexedRDD.cache();
    }

    /**
     * Gets the point rdd.
     *
     * @return the point rdd
     */
    public JavaRDD<Point> getRawPointRDD() {
        return rawPointRDD;
    }

    /**
     * Sets the point rdd.
     *
     * @param rawPointRDD the new point rdd
     */
    public void setRawPointRDD(JavaRDD<Point> rawPointRDD) {
        this.rawPointRDD = rawPointRDD;
    }

    /**
     * Boundary.
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
    /*
        Output the raw RDD as GeoJSON
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawPointRDD.mapPartitions(new FlatMapFunction<Iterator<Point>, String>() {
            @Override
            public Iterable<String> call(Iterator<Point> pointIterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (pointIterator.hasNext()) {
                    GeoJSON json = writer.write(pointIterator.next());
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result;
            }
        }).saveAsTextFile(outputLocation);
    }

    //Som

}
