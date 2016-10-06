package org.datasyslab.geospark.utils;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

/**
 * A factory for creating Comparator objects.
 */
public class GeometryComparatorFactory {
	
	/**
	 * Creates a new Comparator object.
	 *
	 * @param Two strings as input, should be in format like ("point", "x");
	 * @param axis the axis
	 * @return the geometry comparator
	 */
	public static GeometryComparator createComparator(String geometryType, String axis){

		GeometryComparator comp = null;
		try {
			switch(geometryType.toUpperCase()) {
			case "POINT":
				if(axis.toUpperCase().equals("X")) {
					comp = new PointXComparator();
				}
				else if(axis.toUpperCase().equals("Y")){
					comp = new PointYComparator();
				}
				else {
					throw new Exception("Input axis string not recognized, should be either x or y");
				}
				break;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return comp;
	}
	
	/**
	 * Creates a new Comparator object.
	 *
	 * @param Two strings as input, should be in format like ("point", "x");
	 * @param axis the axis
	 * @param extrema, whether we want the max or the min
	 * @return the geometry comparator
	 */
	public static GeometryComparator createComparator(String geometryType, String axis, String extrema){
		GeometryComparator comp = null;
		switch(geometryType.toUpperCase()) {
			case "CIRCLE":
				if(axis.toUpperCase().equals("X")) {
					return extrema.toUpperCase().equals("MAX")?new CircleXMaxComparator():new CircleXMinComparator();
				}
				else if (axis.toUpperCase().equals("Y")){
					return extrema.toUpperCase().equals("MAX")?new CircleYMaxComparator():new CircleYMinComparator();
				}
				
				break;
			case "POLYGON":
				if(axis.toUpperCase().equals("X")) {
					return extrema.toUpperCase().equals("MAX")?new PolygonXMaxComparator():new PolygonXMinComparator();
				}
				else if (axis.toUpperCase().equals("Y")){
					return extrema.toUpperCase().equals("MAX")?new PolygonYMaxComparator():new PolygonYMinComparator();
				}
				break;
			case "RECTANGLE":
				if(axis.toUpperCase().equals("X")) {
					return extrema.toUpperCase().equals("MAX")?new RectangleXMaxComparator():new RectangleXMinComparator();
				}
				else if (axis.toUpperCase().equals("Y")){
					return extrema.toUpperCase().equals("MAX")?new RectangleYMaxComparator():new RectangleYMinComparator();
				}
				break;
			default:
				//TODO: add exception later
				break;
		}
		return comp;
	}
}

