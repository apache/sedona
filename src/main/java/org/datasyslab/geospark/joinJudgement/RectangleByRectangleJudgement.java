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

public class RectangleByRectangleJudgement implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>>, Envelope, HashSet<Envelope>>, Serializable{

	int gridNumber;
	public RectangleByRectangleJudgement(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	
	@Override
	 public Iterator<Tuple2<Envelope, HashSet<Envelope>>> call(Tuple2<Integer, Tuple2<Iterable<Envelope>, Iterable<Envelope>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Envelope>>> result = new HashSet<Tuple2<Envelope, HashSet<Envelope>>>();
        Iterator<Envelope> iteratorWindow=cogroup._2()._2().iterator();
        while(iteratorWindow.hasNext()) {
        	Envelope window=iteratorWindow.next();
            HashSet<Envelope> objectHashSet = new HashSet<Envelope>();
            Iterator<Envelope> iteratorObject=cogroup._2()._1().iterator();
            while (iteratorObject.hasNext()) {
            	Envelope object = iteratorObject.next();
                if (window.contains(object)) {
                	objectHashSet.add(object);
                }
            }
            result.add(new Tuple2<Envelope, HashSet<Envelope>>(window, objectHashSet));
        }
        return result.iterator();
    }

}
