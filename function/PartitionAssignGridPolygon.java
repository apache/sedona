package Functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.Polygon;

public class PartitionAssignGridPolygon implements PairFlatMapFunction<java.util.Iterator<Polygon>,Integer,Polygon>, Serializable{
	//This function is to assign grid index to each rectangle in large dataset.
			int gridNumberHorizontal;
			int gridNumberVertical;
			Double[] gridHorizontalBorder;
			Double[] gridVerticalBorder;

			public PartitionAssignGridPolygon(int gridNumberHorizontal, int gridNumberVertical, Double[] gridHorizontalBorder, Double[] gridVerticalBorder) 
			{
				this.gridNumberHorizontal=gridNumberHorizontal;
				this.gridNumberVertical=gridNumberVertical;
				this.gridHorizontalBorder=gridHorizontalBorder;
				this.gridVerticalBorder=gridVerticalBorder;
			}

			public Iterable<Tuple2<Integer, Polygon>> call(Iterator<Polygon> s) throws Exception 	
			{
				//int id=-1;
				ArrayList<Tuple2<Integer, Polygon>> list=new ArrayList<Tuple2<Integer, Polygon>>();
				
				while(s.hasNext())
				{
							
					Polygon currentElement=s.next();
					Integer id=0;
					for(int j=0;j<gridNumberVertical;j++)
							{
								for(int i=0;i<gridNumberHorizontal;i++)
								{
									Envelope currentGrid=new Envelope(gridHorizontalBorder[i],gridHorizontalBorder[i+1],gridVerticalBorder[j],gridVerticalBorder[j+1]);
									/*Boolean Vertex1Condition=currentElement.getEnvelopeInternal().getMinX()>=gridHorizontalBorder[i] && currentElement.getEnvelopeInternal().getMinX()<=gridHorizontalBorder[i+1]&&currentElement.getEnvelopeInternal().getMaxX()>=gridHorizontalBorder[i] && currentElement.getEnvelopeInternal().getMaxX()<=gridHorizontalBorder[i+1] ;
									Boolean Vertex2Condition=currentElement.getEnvelopeInternal().getMinY()>=gridVerticalBorder[j] && currentElement.getEnvelopeInternal().getMinY()<=gridVerticalBorder[j+1]&&currentElement.getEnvelopeInternal().getMaxY()>=gridVerticalBorder[j] && currentElement.getEnvelopeInternal().getMaxY()<=gridVerticalBorder[j+1];
									
									if(Vertex1Condition && Vertex2Condition )
									{
										//Fully contain
										id=i*gridNumberHorizontal+j;
										list.add(new Tuple2(id,currentElement));
									}
									else if(!Vertex1Condition && !Vertex2Condition)
										{
											//Fully disjoint
											continue;
										}
										else
										{
											//Overlap
											id=i*gridNumberHorizontal+j;
											list.add(new Tuple2(id,currentElement));
										}*/
									if(currentGrid.intersects(currentElement.getEnvelopeInternal())||currentGrid.contains(currentElement.getEnvelopeInternal())||currentElement.getEnvelopeInternal().contains(currentGrid))
									{
										//id=j*gridNumberHorizontal+i;
										list.add(new Tuple2(id,currentElement));
									}
									id++;
								}
							}
				}
				
				
				return list;
			}
}
