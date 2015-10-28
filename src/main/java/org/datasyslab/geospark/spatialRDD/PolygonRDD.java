/*
 * 
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.precision.GeometryPrecisionReducer;

import org.datasyslab.geospark.utils.*;
import org.datasyslab.geospark.boundryFilter.*;
import org.datasyslab.geospark.rangeFilter.*;
import org.datasyslab.geospark.partition.*;

// TODO: Auto-generated Javadoc
class PolygonFormatMapper implements Function<String,Polygon>,Serializable
{
	Integer offset=0;
	String splitter="csv";
	public PolygonFormatMapper(Integer Offset,String Splitter)
	{
		this.offset=Offset;
		this.splitter=Splitter;
	}
	public Polygon call(String s)
	{	
		String seperater=",";
		if(this.splitter.contains("tsv"))
		{
			seperater="\t";
		}
		else
		{
			seperater=",";
		}
		List<String> input=Arrays.asList(s.split(seperater));
		ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
		for(int i=this.offset;i<input.size();i=i+2)
		{
			coordinatesList.add(new Coordinate(Double.parseDouble(input.get(i)),Double.parseDouble(input.get(i+1))));
		}
		coordinatesList.add(coordinatesList.get(0));
		Coordinate[] coordinates=new Coordinate[coordinatesList.size()];
		coordinates=coordinatesList.toArray(coordinates);
		GeometryFactory fact = new GeometryFactory();
		 LinearRing linear = new GeometryFactory().createLinearRing(coordinates);
		 Polygon polygon = new Polygon(linear, null, fact);
		 return polygon;
	}
}

/**
 * The Class PolygonRDD.
 */
public class PolygonRDD implements Serializable{
	
	/** The polygon rdd. */
	private JavaRDD<Polygon> polygonRDD;
	
	/**
	 * Instantiates a new polygon rdd.
	 *
	 * @param polygonRDD the polygon rdd
	 */
	public PolygonRDD(JavaRDD<Polygon> polygonRDD)
	{
		this.setPolygonRDD(polygonRDD.cache());
	}
	
	/**
	 * Instantiates a new polygon rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 * @param partitions the partitions
	 */
	public PolygonRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions)
	{
		
		this.setPolygonRDD(spark.textFile(InputLocation,partitions).map(new PolygonFormatMapper(Offset,Splitter)).cache());
	}
	
	/**
	 * Instantiates a new polygon rdd.
	 *
	 * @param spark the spark
	 * @param InputLocation the input location
	 * @param Offset the offset
	 * @param Splitter the splitter
	 */
	public PolygonRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter)
	{
		
		this.setPolygonRDD(spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset,Splitter)).cache());
	}
	
	/**
	 * Gets the polygon rdd.
	 *
	 * @return the polygon rdd
	 */
	public JavaRDD<Polygon> getPolygonRDD() {
		return polygonRDD;
	}
	
	/**
	 * Sets the polygon rdd.
	 *
	 * @param polygonRDD the new polygon rdd
	 */
	public void setPolygonRDD(JavaRDD<Polygon> polygonRDD) {
		this.polygonRDD = polygonRDD;
	}
	
	/**
	 * Re partition.
	 *
	 * @param number the number
	 */
	public void rePartition(Integer number)
	{
		this.polygonRDD=this.polygonRDD.repartition(number);
	}
	
	/**
	 * Boundary.
	 *
	 * @return the envelope
	 */
	public Envelope boundary()
	{
		
		Double[] boundary = new Double[4];
		Double minLongtitude1=this.polygonRDD.min((PolygonXMinComparator)GeometryComparatorFactory.createComparator("polygon", "x", "min")).getEnvelopeInternal().getMinX();
		Double maxLongtitude1=this.polygonRDD.max((PolygonXMinComparator)GeometryComparatorFactory.createComparator("polygon", "x", "min")).getEnvelopeInternal().getMinX();
		Double minLatitude1=this.polygonRDD.min((PolygonYMinComparator)GeometryComparatorFactory.createComparator("polygon", "y", "min")).getEnvelopeInternal().getMinY();
		Double maxLatitude1=this.polygonRDD.max((PolygonYMinComparator)GeometryComparatorFactory.createComparator("polygon", "y", "min")).getEnvelopeInternal().getMinY();
		Double minLongtitude2=this.polygonRDD.min((PolygonXMaxComparator)GeometryComparatorFactory.createComparator("polygon", "x", "max")).getEnvelopeInternal().getMaxX();
		Double maxLongtitude2=this.polygonRDD.max((PolygonXMaxComparator)GeometryComparatorFactory.createComparator("polygon", "x", "max")).getEnvelopeInternal().getMaxX();
		Double minLatitude2=this.polygonRDD.min((PolygonYMaxComparator)GeometryComparatorFactory.createComparator("polygon", "y", "max")).getEnvelopeInternal().getMaxY();
		Double maxLatitude2=this.polygonRDD.max((PolygonYMaxComparator)GeometryComparatorFactory.createComparator("polygon", "y", "max")).getEnvelopeInternal().getMaxY(); 
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
	
	/**
	 * Minimum bounding rectangle.
	 *
	 * @return the rectangle rdd
	 */
	public RectangleRDD MinimumBoundingRectangle()
	{
	JavaRDD<Envelope> rectangleRDD=this.polygonRDD.map(new Function<Polygon,Envelope>(){
			
			public Envelope call(Polygon s)
			{
				Envelope MBR= s.getEnvelopeInternal();
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
public Polygon PolygonUnion()
	{
		Polygon result=this.polygonRDD.reduce(new Function2<Polygon,Polygon,Polygon>()
				{

					public Polygon call(Polygon v1, Polygon v2){
						
						//Reduce precision in JTS to avoid TopologyException
						PrecisionModel pModel=new PrecisionModel();
						GeometryPrecisionReducer pReducer=new GeometryPrecisionReducer(pModel);
						Geometry p1=pReducer.reduce(v1);
						Geometry p2=pReducer.reduce(v2);
						//Union two polygons
						Geometry polygonGeom=p1.union(p2);
						Coordinate[] coordinates=polygonGeom.getCoordinates();
						ArrayList<Coordinate> coordinateList=new ArrayList<Coordinate>(Arrays.asList(coordinates));
						Coordinate lastCoordinate=coordinateList.get(0);
						coordinateList.add(lastCoordinate);
						Coordinate[] coordinatesClosed=new Coordinate[coordinateList.size()];
						coordinatesClosed=coordinateList.toArray(coordinatesClosed);
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
