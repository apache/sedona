package org.datasyslab.geospark.spatialPartitioning;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.triangulate.VoronoiDiagramBuilder;

public class VoronoiPartitioning implements Serializable{
	HashSet<EnvelopeWithGrid> grids=new HashSet<EnvelopeWithGrid>();
	public VoronoiPartitioning(Point[] SampleList,Envelope boundary,int partitions)
	{
		GeometryFactory fact = new GeometryFactory();
		ArrayList<Point> subSampleList=new ArrayList<Point>();
		MultiPoint mp;
		//Take a subsample accoring to the partitions
		for(int i=0;i<SampleList.length;i=i+SampleList.length/partitions)
		{
			subSampleList.add(SampleList[i]);
		}
		mp=fact.createMultiPoint(subSampleList.toArray(new Point[subSampleList.size()]));
		VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
		voronoiBuilder.setSites(mp);
		Geometry voronoiDiagram=voronoiBuilder.getDiagram(fact);
		for(int i=0;i<voronoiDiagram.getNumGeometries();i++)
		{
			Polygon poly=(Polygon)voronoiDiagram.getGeometryN(i);
			grids.add(new EnvelopeWithGrid(poly.getEnvelopeInternal(),i));
		}
		//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	public VoronoiPartitioning(Envelope[] SampleList,Envelope boundary,int partitions)
	{
		GeometryFactory fact = new GeometryFactory();
		MultiPoint mp;
		List<Point> points=new ArrayList<Point>();
		for(int i=0;i<SampleList.length;i=i+SampleList.length/partitions)
		{
			Envelope envelope=SampleList[i];
			Coordinate coordinate = new Coordinate((envelope.getMinX()+envelope.getMaxX())/2.0,(envelope.getMinY()+envelope.getMaxY())/2.0);
			points.add(fact.createPoint(coordinate));
		}
		mp=fact.createMultiPoint(points.toArray(new Point[points.size()]));
		VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
		voronoiBuilder.setSites(mp);
		Geometry voronoiDiagram=voronoiBuilder.getDiagram(fact);
		for(int i=0;i<voronoiDiagram.getNumGeometries();i++)
		{
			Polygon poly=(Polygon)voronoiDiagram.getGeometryN(i);
			grids.add(new EnvelopeWithGrid(poly.getEnvelopeInternal(),i));
		}
		//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	public VoronoiPartitioning(Polygon[] SampleList,Envelope boundary,int partitions)
	{
		GeometryFactory fact = new GeometryFactory();
		MultiPoint mp;
		List<Point> points=new ArrayList<Point>();
		for(int i=0;i<SampleList.length;i=i+SampleList.length/partitions)
		{
			Envelope envelope=SampleList[i].getEnvelopeInternal();
			Coordinate coordinate = new Coordinate((envelope.getMinX()+envelope.getMaxX())/2.0,(envelope.getMinY()+envelope.getMaxY())/2.0);
			points.add(fact.createPoint(coordinate));
		}
		mp=fact.createMultiPoint(points.toArray(new Point[points.size()]));
		VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
		voronoiBuilder.setSites(mp);
		Geometry voronoiDiagram=voronoiBuilder.getDiagram(fact);
		for(int i=0;i<voronoiDiagram.getNumGeometries();i++)
		{
			Polygon poly=(Polygon)voronoiDiagram.getGeometryN(i);
			grids.add(new EnvelopeWithGrid(poly.getEnvelopeInternal(),i));
		}
		//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	/**
	 * @return Return the generated grid file
	 */
	public HashSet<EnvelopeWithGrid> getGrids() {
		
		return this.grids;
		
	}
}
