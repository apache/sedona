/**
 * FILE: RectangleRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.RectangleRDD.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.datasyslab.geospark.enums.FileDataSplitter;

import org.datasyslab.geospark.formatMapper.RectangleFormatMapper;

import org.datasyslab.geospark.utils.GeometryComparatorFactory;
import org.datasyslab.geospark.utils.RectangleXMaxComparator;
import org.datasyslab.geospark.utils.RectangleXMinComparator;
import org.datasyslab.geospark.utils.RectangleYMaxComparator;
import org.datasyslab.geospark.utils.RectangleYMinComparator;

import com.vividsolutions.jts.geom.Envelope;


// TODO: Auto-generated Javadoc

/**
 * The Class RectangleRDD.
 */

public class RectangleRDD extends SpatialRDD {

    
    
	/**
	 * Instantiates a new rectangle RDD.
	 *
	 * @param rawSpatialRDD the raw spatial RDD
	 */
	public RectangleRDD(JavaRDD<Envelope> rawSpatialRDD)
	{
		this.rawSpatialRDD = rawSpatialRDD.map(new Function<Envelope,Object>()
		{

			@Override
			public Object call(Envelope spatialObject) throws Exception {
				return spatialObject;
			}
			
		});
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();

	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     * @param partitions the partitions
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,FileDataSplitter splitter,Integer partitions)
	{
		this.setRawSpatialRDD(spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	
    /**
     * Instantiates a new rectangle RDD.
     *
     * @param spark the spark
     * @param InputLocation the input location
     * @param Offset the offset
     * @param splitter the splitter
     */
	public RectangleRDD(JavaSparkContext spark, String InputLocation,Integer Offset,FileDataSplitter splitter)
	{
		//final Integer offset=Offset;
		this.setRawSpatialRDD(spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,splitter)));
        this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
	}
	

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.spatialRDD.SpatialRDD#boundary()
     */
	public Envelope boundary()
	{
		Double minLongtitude1=((Envelope) this.rawSpatialRDD.min((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min"))).getMinX();
		Double maxLongtitude1=((Envelope) this.rawSpatialRDD.max((RectangleXMinComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "min"))).getMinX();
		Double minLatitude1=((Envelope) this.rawSpatialRDD.min((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min"))).getMinY();
		Double maxLatitude1=((Envelope) this.rawSpatialRDD.max((RectangleYMinComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "min"))).getMinY();
		Double minLongtitude2=((Envelope) this.rawSpatialRDD.min((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max"))).getMaxX();
		Double maxLongtitude2=((Envelope) this.rawSpatialRDD.max((RectangleXMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "x", "max"))).getMaxX();
		Double minLatitude2=((Envelope) this.rawSpatialRDD.min((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max"))).getMaxY();
		Double maxLatitude2=((Envelope) this.rawSpatialRDD.max((RectangleYMaxComparator)GeometryComparatorFactory.createComparator("rectangle", "y", "max"))).getMaxY();
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
