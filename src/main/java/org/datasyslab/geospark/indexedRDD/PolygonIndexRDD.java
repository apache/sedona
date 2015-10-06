package org.datasyslab.geospark.indexedRDD;

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
class PolygonRangeQueryWithSTRtree implements FlatMapFunction<Iterator<STRtree>,Polygon>,Serializable
{	
	private Envelope queryWindow=null;
	public PolygonRangeQueryWithSTRtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Polygon> call(Iterator<STRtree> strtree) {
		List<Polygon> targets=strtree.next().query(queryWindow);
		return targets;
	}
	
}
class PolygonRangeQueryWithQuadtree implements FlatMapFunction<Iterator<Quadtree>,Polygon>,Serializable
{	
	private Envelope queryWindow=null;
	public PolygonRangeQueryWithQuadtree(Envelope QueryWindow)
	{
		this.queryWindow=QueryWindow;
	}
	public Iterable<Polygon> call(Iterator<Quadtree> quadtree) {

		List<Polygon> targets=quadtree.next().query(queryWindow);
		return targets;

	}
	
}
public class PolygonIndexRDD {
	private JavaRDD<STRtree> polygonIndexRDDrtree;
	private JavaRDD<Quadtree> polygonIndexRDDquadtree;
	private String tree;


	public PolygonIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,Integer partitions,String Tree)
	{

		JavaRDD<Polygon> targetRDDWithoutTree=spark.textFile(InputLocation,partitions).map(new PolygonFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.polygonIndexRDDrtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Polygon>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Polygon> targets){
					Iterator<Polygon> targetIterator=targets;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Polygon currentTarget=targetIterator.next();
						strtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.polygonIndexRDDquadtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Polygon>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Polygon> targets){
						Iterator<Polygon> targetIterator=targets;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Polygon currentTarget=targetIterator.next();
							quadtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
		
		
	}
	public PolygonIndexRDD(JavaSparkContext spark, String InputLocation,Integer Offset,String Splitter,String Tree)
	{

		JavaRDD<Polygon> targetRDDWithoutTree=spark.textFile(InputLocation).map(new PolygonFormatMapper(Offset,Splitter));
		if(Tree=="rtree"){
		this.polygonIndexRDDrtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Polygon>,STRtree>()
				{

					public Iterable<STRtree> call(Iterator<Polygon> targets){
					Iterator<Polygon> targetIterator=targets;
					STRtree strtree= new STRtree();
					ArrayList<STRtree> strtrees=new ArrayList<STRtree>(1);
					while(targetIterator.hasNext())
					{
						Polygon currentTarget=targetIterator.next();
						strtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
					}
					strtrees.add(strtree);
					return strtrees;
					}
				});
			this.tree="rtree";
		}
		else 
		{
			this.polygonIndexRDDquadtree=targetRDDWithoutTree.mapPartitions(new FlatMapFunction<Iterator<Polygon>,Quadtree>()
					{

						public Iterable<Quadtree> call(Iterator<Polygon> targets){
						Iterator<Polygon> targetIterator=targets;
						Quadtree quadtree= new Quadtree();
						ArrayList<Quadtree> quadtrees=new ArrayList<Quadtree>(1);
						while(targetIterator.hasNext())
						{
							Polygon currentTarget=targetIterator.next();
							quadtree.insert(currentTarget.getEnvelopeInternal(), currentTarget);
						}
						quadtrees.add(quadtree);
						return quadtrees;
						}
					});
			this.tree="quadtree";
		}
		
		
	}

	public PolygonRDD SpatialRangeQuery(Envelope envelope)
	{
		if(tree=="rtree"){
		JavaRDD<Polygon> result=this.polygonIndexRDDrtree.mapPartitions(new PolygonRangeQueryWithSTRtree(envelope));
		return new PolygonRDD(result);
		}
		else
		{
			JavaRDD<Polygon> result=this.polygonIndexRDDquadtree.mapPartitions(new PolygonRangeQueryWithQuadtree(envelope));
			return new PolygonRDD(result);

		}
	}
	public PolygonRDD SpatialRangeQuery(Polygon polygon)
	{
		if(tree=="rtree"){
		JavaRDD<Polygon> result=this.polygonIndexRDDrtree.mapPartitions(new PolygonRangeQueryWithSTRtree(polygon.getEnvelopeInternal()));
		return new PolygonRDD(result);
		}
		else
		{
			JavaRDD<Polygon> result=this.polygonIndexRDDquadtree.mapPartitions(new PolygonRangeQueryWithQuadtree(polygon.getEnvelopeInternal()));
			return new PolygonRDD(result);

		}
	}
}
