/*
 *
 */
package org.datasyslab.geospark.spatialRDD;

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

import org.apache.commons.lang.IllegalClassException;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.gemotryObjects.EnvelopeWithGrid;
import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.PointXComparator;
import org.datasyslab.geospark.utils.PointYComparator;
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
import java.util.List;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
class PolygonFormatMapper implements Function<String, Polygon>, Serializable {
    Integer offset = 0;
    String splitter = "csv";

    public PolygonFormatMapper(Integer Offset, String Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public PolygonFormatMapper(String Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public Polygon call(String line) throws Exception {
        Polygon polygon = null;
        GeometryFactory fact = new GeometryFactory();
        Coordinate coordinate;
        List<String> lineSplitList;
        ArrayList<Coordinate> coordinatesList;
        Coordinate[] coordinates;
        LinearRing linear;
        switch (splitter) {
            case "csv":
                lineSplitList = Arrays.asList(line.split(","));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                linear = fact.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                polygon = new Polygon(linear, null, fact);
                break;
            case "tsv":
                lineSplitList = Arrays.asList(line.split("\t"));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i = i + 2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                coordinates = new Coordinate[coordinatesList.size()];
                coordinates = coordinatesList.toArray(coordinates);
                linear = fact.createLinearRing(coordinates);
                polygon = new Polygon(linear, null, fact);
                break;
            case "geojson":
                GeoJSONReader reader = new GeoJSONReader();
                polygon = (Polygon) reader.read(line);
                break;
            case "wtk":
                WKTReader wtkreader = new WKTReader();
                try {
                    polygon = (Polygon) wtkreader.read(line);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return polygon;
    }
}

/**
 * The Class PolygonRDD.
 */
public class PolygonRDD implements Serializable {

    public long totalNumberOfRecords;
    /** The rectangle rdd. */
    public JavaRDD<Polygon> rawPolygonRDD;

    ArrayList<Double> grid;

    Double[] boundary = new Double[4];

    public Envelope boundaryEnvelope;

    public JavaPairRDD<Integer, Polygon> gridPolygonRDD;

    public ArrayList<EnvelopeWithGrid> grids;


    //todo, replace this STRtree to be more generalized, such as QuadTree.
    public JavaPairRDD<Integer, STRtree> indexedRDD;
    private Envelope minXEnvelope;
    private Envelope minYEnvelope;
    private Envelope maxXEnvelope;
    private Envelope maxYEnvelope;

    /**
     * Instantiates a new polygon rdd.
     *
     * @param rawPolygonRDD the polygon rdd
     */
    public PolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.setRawPolygonRDD(rawPolygonRDD.cache());
    }

    /**
     * Instantiates a new polygon rdd.
     *
     * @param spark         the spark
     * @param InputLocation the input location
     * @param Offset        the offset
     * @param Splitter      the splitter
     * @param partitions    the partitions
     */
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter, Integer partitions) {

        this.setRawPolygonRDD(spark.textFile(InputLocation, partitions).map(new PolygonFormatMapper(Offset, Splitter)).cache());
    }

    /**
     * Instantiates a new polygon rdd.
     *
     * @param spark         the spark
     * @param InputLocation the input location
     * @param Offset        the offset
     * @param Splitter      the splitter
     */
    public PolygonRDD(JavaSparkContext spark, String InputLocation, Integer Offset, String Splitter) {

        this.setRawPolygonRDD(spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset, Splitter)).cache());
    }

    /**
     * Gets the polygon rdd.
     *
     * @return the polygon rdd
     */
    public JavaRDD<Polygon> getRawPolygonRDD() {
        return rawPolygonRDD;
    }

    //todo: remove offset.
    public PolygonRDD(JavaSparkContext sc, String inputLocation, Integer offSet, String splitter, String gridType, Integer numPartitions) {
        this.rawPolygonRDD = sc.textFile(inputLocation).map(new PolygonFormatMapper(offSet, splitter));

        totalNumberOfRecords = this.rawPolygonRDD.count();

        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, totalNumberOfRecords);

        ArrayList<Polygon> polygonSampleList = new ArrayList<Polygon> (rawPolygonRDD.takeSample(false, sampleNumberOfRecords));

        //Sort
        Comparator<Polygon> comparator = null;
        switch (gridType) {
            case "X":
                comparator = new PolygonXMinComparator();
                break;
            case "Y":
                comparator = new PolygonYMinComparator();
                break;
            case "X-Y":
                //Will first sort based on X, then partition, then sort and partition based on Y.
                comparator = new PolygonXMinComparator();
                break;
            case "STRtree":
                throw new IllegalArgumentException("STRtree is currently now implemented, to be finished in version 0.3");
            default:
                throw new IllegalArgumentException("Grid type not recognied, please check again.");
        }

        this.boundary();

        GeometryFactory geometryFactory = new GeometryFactory();

        //If we don't +1, it will be recognized as point instead of rectangle
        polygonSampleList.set(0, (Polygon)geometryFactory.toGeometry(minXEnvelope));
        polygonSampleList.set(1, (Polygon)geometryFactory.toGeometry(minYEnvelope));
        polygonSampleList.set(sampleNumberOfRecords-2, (Polygon)geometryFactory.toGeometry(maxYEnvelope));
        polygonSampleList.set(sampleNumberOfRecords-1, (Polygon)geometryFactory.toGeometry(maxXEnvelope));

        if(sampleNumberOfRecords == 0) {
            //If the sample Number is too small, we will just use one grid instead.
            System.err.println("The grid size is " + numPartitions * numPartitions + "for 2-dimension X-Y grid" + numPartitions + " for 1-dimension grid");
            System.err.println("The sample size is " + totalNumberOfRecords /100);
            System.err.println("input size is too small, we can not guarantee one grid have at least one record in it");
            System.err.println("we will just build one grid for all input");
            grids = new ArrayList<EnvelopeWithGrid>();
            grids.add(new EnvelopeWithGrid(this.boundaryEnvelope, 0));
        } else if (gridType.equals("X")) {

            Collections.sort(polygonSampleList, comparator);
            //Below is for X and Y grid.
            int curLocation = 0;
            int step = sampleNumberOfRecords / numPartitions;
            //If step = 0, which  means data set is really small, choose the


            //Find Mid Point
            ArrayList<Point> xAxisBar = new ArrayList<Point>();
            while (curLocation < sampleNumberOfRecords) {
                Double x = polygonSampleList.get(curLocation).getEnvelopeInternal().getMinX();
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
            Collections.sort(polygonSampleList, comparator);
            //Below is for X and Y grid.
            int curLocation = 0;
            int step = sampleNumberOfRecords / numPartitions;
            //If step = 0, which  means data set is really small, choose the
            GeometryComparatorFactory geometryComparatorFactory = new GeometryComparatorFactory();

            //Find Mid Point
            ArrayList<Point> yAxisBar = new ArrayList<Point>();
            while (curLocation < sampleNumberOfRecords) {
                Double y = polygonSampleList.get(curLocation).getEnvelopeInternal().getMinX();
                yAxisBar.add(geometryFactory.createPoint(new Coordinate(y, y)));
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
        } else if (gridType.equals("X-Y")) {
            //Ideally we want, One partition => One Grid. And each partition have equal size.
            //We use upper bound of sqrt(# of partition) for X and Y.
            Collections.sort(polygonSampleList, comparator);

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
                Double x = polygonSampleList.get(i).getEnvelopeInternal().getMinX();
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

            for(int j = 0; j < xAxisSubList.size();j++) {
                //Fetch the X bar.

                ArrayList<Polygon> xAxisBar = new ArrayList<Polygon>(polygonSampleList.subList(j*stepInXAxis, (j+1)*stepInXAxis));

                Collections.sort(xAxisBar, new PolygonYMinComparator());

                //Pick y Axis.
                ArrayList<Point> yAxisSubList = new ArrayList<Point>();

                for(int k = 0; k < xAxisBar.size(); k+=stepInYAxis) {
                    Double y = polygonSampleList.get(k).getEnvelopeInternal().getMinY();
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
        JavaPairRDD<Integer,Polygon> unPartitionedGridPolygonRDD = this.rawPolygonRDD.flatMapToPair(
                new PairFlatMapFunction<Polygon, Integer, Polygon>() {
                    @Override
                    public Iterable<Tuple2<Integer, Polygon>> call(Polygon polygon) throws Exception {
                        ArrayList<Tuple2<Integer, Polygon>> result = new ArrayList<Tuple2<Integer, Polygon>>();
                        //todo.. This is is really not efficient way of doing this.
                        GeometryFactory geometryFactory = new GeometryFactory();
                        for (EnvelopeWithGrid e : gridEnvelopBroadcasted.getValue()) {
                            if (polygon.intersects(geometryFactory.toGeometry(e))) {
                                result.add(new Tuple2<Integer, Polygon>(e.grid, polygon));
                            }
                        }

                        if (result.size() == 0) {
                            //Should never goes here..
                            throw new Exception("[Error]" +
                                    polygon.toString() +
                                    "The grid must have errors, it should at least have one grid contain this point");
                        }
                        return result;
                    }
                }
        );

        this.gridPolygonRDD = unPartitionedGridPolygonRDD.partitionBy(new HashPartitioner(numPartitions)).persist(StorageLevel.DISK_ONLY());
    }

    public void buildIndex(String indexType) {

        if (this.gridPolygonRDD == null) {
            throw new IllegalClassException("To build index, you must build grid first");
        }

        //Use GroupByKey, since I have repartition data, it should be much faster.
        //todo: Need to test performance here...
        JavaPairRDD<Integer, Iterable<Polygon>> gridedRectangleListRDD = this.gridPolygonRDD.groupByKey();

        this.indexedRDD = gridedRectangleListRDD.flatMapValues(new Function<Iterable<Polygon>, Iterable<STRtree>>() {
            @Override
            public Iterable<STRtree> call(Iterable<Polygon> polygons) throws Exception {
                STRtree rt = new STRtree();
                for (Polygon p : polygons)
                    rt.insert(p.getEnvelopeInternal(), p);
                ArrayList<STRtree> result = new ArrayList<STRtree>();
                result.add(rt);
                return result;
            }
        });
        this.indexedRDD.cache();
    }
    /**
     * Sets the polygon rdd.
     *
     * @param rawPolygonRDD the new polygon rdd
     */
    public void setRawPolygonRDD(JavaRDD<Polygon> rawPolygonRDD) {
        this.rawPolygonRDD = rawPolygonRDD;
    }

    /**
     * Re partition.
     *
     * @param number the number
     */
    public void rePartition(Integer number) {
        this.rawPolygonRDD = this.rawPolygonRDD.repartition(number);
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
     * @return the rectangle rdd
     */
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Envelope> rectangleRDD = this.rawPolygonRDD.map(new Function<Polygon, Envelope>() {

            public Envelope call(Polygon s) {
                Envelope MBR = s.getEnvelopeInternal();
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
}
