/**
 * FILE: CircleRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.CircleRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.geometryObjects.Circle;


import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;;

// TODO: Auto-generated Javadoc

/**
 * The Class CircleRDD.
 */
public class CircleRDD extends SpatialRDD {


	/**
	 * Instantiates a new circle RDD.
	 *
	 * @param circleRDD the circle RDD
	 */
	public CircleRDD(JavaRDD<Circle> circleRDD) {
		this.rawSpatialRDD = circleRDD.map(new Function<Circle,Object>()
		{

			@Override
			public Object call(Circle circle) throws Exception {
				return circle;
			}
			
		});
	}

	/**
	 * Instantiates a new circle RDD.
	 *
	 * @param pointRDD the point RDD
	 * @param Radius the radius
	 */
	public CircleRDD(PointRDD pointRDD, Double Radius) {
		final Double radius = Radius;
		JavaRDD<Circle> circleRDD = pointRDD.rawSpatialRDD.map(new Function<Object, Circle>() {

			public Circle call(Object v1) {

				return new Circle((Point)v1, radius);
			}
		});
		this.rawSpatialRDD = circleRDD.map(new Function<Circle,Object>()
		{

			@Override
			public Object call(Circle circle) throws Exception {
				return circle;
			}
			
		});
	}


    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
	public RectangleRDD MinimumBoundingRectangle() {
		return new RectangleRDD(this.rawSpatialRDD.map(new Function<Object, Envelope>() {

			public Envelope call(Object spatialObject) {

				Circle circle = (Circle) spatialObject;
				return circle.getMBR();
			}

		}));
	}

	/**
	 * Center.
	 *
	 * @return the point RDD
	 */
	public PointRDD Center() {
		return new PointRDD(this.rawSpatialRDD.map(new Function<Object, Point>() {

			public Point call(Object spatialObject) {

				return ((Circle)spatialObject).getCenter();
			}

		}));
	}
}
