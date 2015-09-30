package org.datasyslab.geospark.utils;

public class ComparatorFactory {
	
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

