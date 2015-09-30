package org.datasyslab.geospark.utils;

import java.io.Serializable;
import java.util.Comparator;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import org.datasyslab.geospark.gemotryObjects.*;

public class ComparatorCollection {
	
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

