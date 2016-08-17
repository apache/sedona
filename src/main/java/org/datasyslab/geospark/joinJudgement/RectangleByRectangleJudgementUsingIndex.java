package org.datasyslab.geospark.joinJudgement;

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
import com.vividsolutions.jts.index.strtree.STRtree;

public class RectangleByRectangleJudgementUsingIndex implements PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>>, Envelope, HashSet<Envelope>>, Serializable{

	int gridNumber;
	public RectangleByRectangleJudgementUsingIndex(int gridNumber)
	{
		this.gridNumber=gridNumber;
	}

	@Override
    public Iterable<Tuple2<Envelope, HashSet<Envelope>>> call(Tuple2<Integer, Tuple2<Iterable<STRtree>, Iterable<Envelope>>> cogroup) throws Exception {
		HashSet<Tuple2<Envelope, HashSet<Envelope>>> result = new HashSet<Tuple2<Envelope, HashSet<Envelope>>>();
		if(cogroup._1()>=gridNumber)
		{
			//Ok. We found this partition contains missing objects. Lets ignore this part.
			return result;
		}
		Iterator<Envelope> iteratorWindow=cogroup._2()._2().iterator();
        Iterator<STRtree> iteratorTree=cogroup._2()._1().iterator();
        if(!iteratorTree.hasNext())
        {
        	return result;
        }
        STRtree strtree = iteratorTree.next();
        while(iteratorWindow.hasNext()) {
        	Envelope window=iteratorWindow.next();  
        	List<Envelope> queryResult=new ArrayList<Envelope>();
            queryResult=strtree.query(window);
            HashSet<Envelope> resultHashSet = new HashSet<Envelope>(queryResult);
            result.add(new Tuple2<Envelope, HashSet<Envelope>>(window, resultHashSet));    
        }
        return result;
    }

}
