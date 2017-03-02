/**
 * FILE: PolygonRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.PolygonRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.formatMapper.PolygonFormatMapper;

import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

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
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;


/**
 * The Class PolygonRDD.
 */
public class PolygonRDD extends SpatialRDD {
    


    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaRDD<Polygon> rawSpatialRDD) {
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Polygon,Object>()
		{
			@Override
			public Object call(Polygon spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.analyze();
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(new PolygonFormatMapper(startOffset,endOffset, splitter,carryInputData)));
        this.analyze();
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new PolygonFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.analyze();
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData, Integer partitions) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(new PolygonFormatMapper(splitter,carryInputData)));
        this.analyze();
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, FileDataSplitter splitter, boolean carryInputData) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new PolygonFormatMapper(splitter, carryInputData)));
        this.analyze();
    }
    


    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(userSuppliedMapper));
        this.analyze();
    }
    
    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @deprecated Please append RDD Storage Level after all the existing parameters
     */
	@Deprecated
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(userSuppliedMapper));
        this.analyze();
    }
        
    /**
     * Instantiates a new polygon RDD.
     *
     * @param rawSpatialRDD the raw spatial RDD
     * @param newLevel the new level
     */
    public PolygonRDD(JavaRDD<Polygon> rawSpatialRDD, StorageLevel newLevel) {
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Polygon,Object>()
		{
			@Override
			public Object call(Polygon spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(new PolygonFormatMapper(startOffset,endOffset, splitter,carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer startOffset, Integer endOffset,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new PolygonFormatMapper(startOffset, endOffset, splitter, carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param partitions the partitions
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation,
    		FileDataSplitter splitter, boolean carryInputData, Integer partitions, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(new PolygonFormatMapper(splitter,carryInputData)));
        this.analyze(newLevel);
    }

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param splitter the splitter
     * @param carryInputData the carry input data
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation,
    		FileDataSplitter splitter, boolean carryInputData, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(new PolygonFormatMapper(splitter, carryInputData)));
        this.analyze(newLevel);
    }
    

    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param partitions the partitions
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, Integer partitions, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation, partitions).flatMap(userSuppliedMapper));
        this.analyze(newLevel);
    }
    
    /**
     * Instantiates a new polygon RDD.
     *
     * @param sparkContext the spark context
     * @param InputLocation the input location
     * @param userSuppliedMapper the user supplied mapper
     * @param newLevel the new level
     */
    public PolygonRDD(JavaSparkContext sparkContext, String InputLocation, FlatMapFunction userSuppliedMapper, StorageLevel newLevel) {
        this.setRawSpatialRDD(sparkContext.textFile(InputLocation).flatMap(userSuppliedMapper));
        this.analyze(newLevel);
    }
    
    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    @Deprecated
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Envelope> rectangleRDD = this.rawSpatialRDD.map(new Function<Object, Envelope>() {

            public Envelope call(Object spatialObject) {
                Envelope MBR = ((Polygon)spatialObject).getEnvelope().getEnvelopeInternal();//.getEnvelopeInternal();
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
        Object result = this.rawSpatialRDD.reduce(new Function2<Object, Object, Object>() {
            public Polygon call(Object v1, Object v2) {
                //Reduce precision in JTS to avoid TopologyException
                PrecisionModel pModel = new PrecisionModel();
                GeometryPrecisionReducer pReducer = new GeometryPrecisionReducer(pModel);
                Geometry p1 = pReducer.reduce((Polygon) v1);
                Geometry p2 = pReducer.reduce((Polygon) v2);
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
        return (Polygon)result;
    }
    
    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterator<String> call(Iterator<Object> iterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                	Geometry spatialObject = (Geometry)iterator.next();
                    GeoJSON json = writer.write(spatialObject);
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }
}

