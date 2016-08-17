package org.datasyslab.geospark.joinJudgement;

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
	 public Iterable<Tuple2<Polygon, HashSet<Polygon>>> call(Tuple2<Integer, Tuple2<Iterable<Polygon>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Polygon>>> result = new HashSet<Tuple2<Polygon, HashSet<Polygon>>>();
		if(cogroup._1()>=gridNumber)
		{
			//Ok. We found this partition contains missing objects. Lets ignore this part.
			return result;
		}
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
        return result;
    }

}
