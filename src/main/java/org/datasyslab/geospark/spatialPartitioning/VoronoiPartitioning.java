/**
 * FILE: VoronoiPartitioning.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.triangulate.VoronoiDiagramBuilder;

// TODO: Auto-generated Javadoc
/**
 * The Class VoronoiPartitioning.
 */
public class VoronoiPartitioning implements Serializable{
	
	/** The grids. */
	List<Envelope> grids=new ArrayList<Envelope>();

	/**
	 * Instantiates a new voronoi partitioning.
	 *
	 * @param SampleList the sample list
	 * @param boundary the boundary
	 * @param partitions the partitions
	 * @throws Exception the exception
	 */
	public VoronoiPartitioning(List SampleList,Envelope boundary,int partitions) throws Exception
	{
		GeometryFactory fact = new GeometryFactory();
		ArrayList<Point> subSampleList=new ArrayList<Point>();
		MultiPoint mp;
		
		//Take a subsample accoring to the partitions
		if(SampleList.get(0) instanceof Envelope)
		{
			for(int i=0;i<SampleList.size();i=i+SampleList.size()/partitions)
			{
				Envelope envelope=(Envelope)SampleList.get(i);
				Coordinate coordinate = new Coordinate((envelope.getMinX()+envelope.getMaxX())/2.0,(envelope.getMinY()+envelope.getMaxY())/2.0);
				subSampleList.add(fact.createPoint(coordinate));
			}
		}
		else if(SampleList.get(0) instanceof Geometry)
		{
			for(int i=0;i<SampleList.size();i=i+SampleList.size()/partitions)
			{
				Envelope envelope=((Geometry)SampleList.get(i)).getEnvelopeInternal();
				Coordinate coordinate = new Coordinate((envelope.getMinX()+envelope.getMaxX())/2.0,(envelope.getMinY()+envelope.getMaxY())/2.0);
				subSampleList.add(fact.createPoint(coordinate));
			}
		}
		else
		{
	    	throw new Exception("[VoronoiPartitioning][Constrcutor] Unsupported spatial object type");
		}
		mp=fact.createMultiPoint(subSampleList.toArray(new Point[subSampleList.size()]));
		VoronoiDiagramBuilder voronoiBuilder = new VoronoiDiagramBuilder();
		voronoiBuilder.setSites(mp);
		Geometry voronoiDiagram=voronoiBuilder.getDiagram(fact);
		for(int i=0;i<voronoiDiagram.getNumGeometries();i++)
		{
			Polygon poly=(Polygon)voronoiDiagram.getGeometryN(i);
			grids.add(poly.getEnvelopeInternal());
		}
		//grids.add(new EnvelopeWithGrid(boundary,grids.size()));
	}
	
	/**
	 * Gets the grids.
	 *
	 * @return the grids
	 */
	public List<Envelope> getGrids() {
		
		return this.grids;
		
	}
}
