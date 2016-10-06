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
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.strtree.STRtree;

public class PolygonByPolygonJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>>, Polygon, HashSet<Polygon>>, Serializable{

	int gridNumber;
	public PolygonByPolygonJudgementUsingIndex(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	@Override
    public Iterator<Tuple2<Polygon, HashSet<Polygon>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Polygon>>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Polygon>>> result = new HashSet<Tuple2<Polygon, HashSet<Polygon>>>();
		Iterator<Polygon> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<STRtree> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result.iterator();
        }
        STRtree strtree = iteratorTree.next();
        while(iteratorWindow.hasNext()) {
        	Polygon window=iteratorWindow.next();  
        	List<Polygon> queryResult=new ArrayList<Polygon>();
            queryResult=strtree.query(window.getEnvelopeInternal());
            HashSet<Polygon> resultHashSet = new HashSet<Polygon>(queryResult);
            result.add(new Tuple2<Polygon, HashSet<Polygon>>(window, resultHashSet));    
        }
        return result.iterator();
    }

}
