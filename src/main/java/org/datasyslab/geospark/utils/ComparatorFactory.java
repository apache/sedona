/*
 * 
 */
package org.datasyslab.geospark.utils;

// TODO: Auto-generated Javadoc
/**
 * A factory for creating Comparator objects.
 */
public class ComparatorFactory {
	
	/**
	 * Creates a new Comparator object.
	 *
	 * @param gemotryType the gemotry type
	 * @param axis the axis
	 * @return the gemotry comparator
	 */
	public static GemotryComparator createComparator(String gemotryType, String axis){
		GemotryComparator comp = null;
		switch(gemotryType.toUpperCase()) {
		case "POINT":
			if(axis.toUpperCase().equals("X"))
				comp = new PointXComparator();
			break;
		}
		return comp;
	}
}

