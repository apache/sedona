/*
 * 
 */
package org.datasyslab.geospark.utilstest;

import static org.junit.Assert.*;

import org.datasyslab.geospark.utils.*;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;

// TODO: Auto-generated Javadoc
/**
 * A factory for creating TestComparator objects.
 */
public class TestComparatorFactory {

	/**
	 * Test point x comparator.
	 */
	@Test
	public void testPointXComparator() {
		
		double lon = -105.0;
		double lat = 40.0;
		Coordinate[] coordinates = new Coordinate[] {new Coordinate(lon,lat)};
		CoordinateSequence coordinateSequence = new CoordinateArraySequence(coordinates);
		GeometryFactory geometryFactory = new GeometryFactory();
		Point point1  =  new Point(coordinateSequence,geometryFactory);
		Point point2  =  new Point(coordinateSequence,geometryFactory);
		PointXComparator pcomp =  (PointXComparator)ComparatorFactory.createComparator("Point", "X");
		assertEquals(0, pcomp.compare(point1, point2));
	}
	
	/**
	 * Test gemotry comparator factory.
	 */
	@Test
	public void testGemotryComparatorFactory() {
		double lon1 = -105.0;
		double lat1 = 40.0;
		double lon2 = -1.0;
		double lat2 = 45.0;
		Coordinate[] coordinates = new Coordinate[] {new Coordinate(lon1,lat1)};
		CoordinateSequence coordinateSequence = new CoordinateArraySequence(coordinates);
		GeometryFactory geometryFactory = new GeometryFactory();
		Point point1  =  new Point(coordinateSequence,geometryFactory);
		
		coordinates = new Coordinate[] {new Coordinate(lon2,lat2)};
		coordinateSequence = new CoordinateArraySequence(coordinates);
		Point point2  =  new Point(coordinateSequence,geometryFactory);
		
		PointXComparator pcomp =  (PointXComparator)ComparatorFactory.createComparator("Point", "X");
		assertEquals(-1, pcomp.compare(point1, point2));
	}
	
	
}
