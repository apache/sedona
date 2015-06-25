package GeoSpark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
class RectangleRangeQueryWithSTRtree implements FlatMapFunction<Iterator<STRtree>,Envelope>,Serializable
{	
	private Envelope queryWindow=null;
	public RectangleRangeQueryWithSTRtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Envelope> call(Iterator<STRtree> strtree) {
		List<Envelope> targets=strtree.next().query(queryWindow);
		return targets;
	}
	
}
class RectangleRangeQueryWithQuadtree implements FlatMapFunction<Iterator<Quadtree>,Envelope>,Serializable
{	
	private Envelope queryWindow=null;
	public RectangleRangeQueryWithQuadtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Envelope> call(Iterator<Quadtree> quadtree) {

		List<Envelope> targets=quadtree.next().query(queryWindow);
		return targets;

	}
	
}
public class RectangleIndexRDD {
	private JavaRDD<STRtree> rectangleIndexRDDrtree;
	private JavaRDD<Quadtree> rectangleIndexRDDquadtree;
	private String tree;


	public RectangleIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions,String Tree)
	{

		JavaRDD<Envelope> targetRDDWithoutTree=spark.textFile(InputLocation,partitions).map(new RectangleFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.rectangleIndexRDDrtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Envelope>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Envelope> targets){
					Iterator<Envelope> targetIterator=targets;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Envelope currentTarget=targetIterator.next();
						strtree.insert(currentTarget, currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.rectangleIndexRDDquadtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Envelope>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Envelope> targets){
						Iterator<Envelope> targetIterator=targets;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Envelope currentTarget=targetIterator.next();
							quadtree.insert(currentTarget, currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
		
		
	}
	public RectangleIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,String Tree)
	{
		JavaRDD<Envelope> targetRDDWithoutTree=spark.textFile(InputLocation).map(new RectangleFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.rectangleIndexRDDrtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Envelope>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Envelope> targets){
					Iterator<Envelope> targetIterator=targets;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Envelope currentTarget=targetIterator.next();
						strtree.insert(currentTarget, currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.rectangleIndexRDDquadtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Envelope>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Envelope> targets){
						Iterator<Envelope> targetIterator=targets;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Envelope currentTarget=targetIterator.next();
							quadtree.insert(currentTarget, currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
	}


	public RectangleRDD SpatialRangeQuery(Envelope envelope)
	{
		if(tree=="rtree"){
		JavaRDD<Envelope> result=this.rectangleIndexRDDrtree.mapPartitions(new RectangleRangeQueryWithSTRtree(envelope));
		return new RectangleRDD(result);
		}
		else
		{
			JavaRDD<Envelope> result=this.rectangleIndexRDDquadtree.mapPartitions(new RectangleRangeQueryWithQuadtree(envelope));
			return new RectangleRDD(result);

		}
	}
	public RectangleRDD SpatialRangeQuery(Polygon polygon)
	{
		if(tree=="rtree"){
		JavaRDD<Envelope> result=this.rectangleIndexRDDrtree.mapPartitions(new RectangleRangeQueryWithSTRtree(polygon.getEnvelopeInternal()));
		return new RectangleRDD(result);
		}
		else
		{
			JavaRDD<Envelope> result=this.rectangleIndexRDDquadtree.mapPartitions(new RectangleRangeQueryWithQuadtree(polygon.getEnvelopeInternal()));
			return new RectangleRDD(result);

		}
	}
}
