package org.datasyslab.geospark.joinJudgement;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

public class PolygonByPolygonJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>>, Polygon, HashSet<Polygon>>, Serializable{

	int gridNumber;
	public PolygonByPolygonJudgement(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	
	@Override
	 public Iterator<Tuple2<Polygon, HashSet<Polygon>>> call(Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Polygon>>> result = new HashSet<Tuple2<Polygon, HashSet<Polygon>>>();
        Iterator<Polygon> iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Polygon window=iteratorWindow.next();
            HashSet<Polygon> objectHashSet = new HashSet<Polygon>();
            Iterator<Polygon> iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Polygon object = iteratorObject.next();
                if (window.contains(object)) {
                	objectHashSet.add(object);
                }
            }
            result.add(new Tuple2<Polygon, HashSet<Polygon>>(window, objectHashSet));
        }
        return result.iterator();
    }

}
