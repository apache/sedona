/**
 * FILE: RasterizationUtils.java
 * PATH: org.datasyslab.babylon.utils.RasterizationUtils.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.babylon.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class RasterizationUtils.
 */
public class RasterizationUtils implements Serializable{
	
	

	/**
	 * Find one pixel coordinate.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundaryOriginal the dataset boundary original
	 * @param spatialCoordinateOriginal the spatial coordinate original
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the tuple 2
	 */
	public static Tuple2<Integer,Integer> FindOnePixelCoordinate(int resolutionX, int resolutionY, Envelope datasetBoundaryOriginal, Coordinate spatialCoordinateOriginal,boolean reverseSpatialCoordinate)
	{
		Coordinate spatialCoordinate;
		Envelope datasetBoundary;
		if(reverseSpatialCoordinate)
		{
			spatialCoordinate = new Coordinate(spatialCoordinateOriginal.y,spatialCoordinateOriginal.x);
			datasetBoundary = new Envelope(datasetBoundaryOriginal.getMinY(),datasetBoundaryOriginal.getMaxY(),datasetBoundaryOriginal.getMinX(),datasetBoundaryOriginal.getMaxX());
		}
		else
		{
			spatialCoordinate = spatialCoordinateOriginal;
			datasetBoundary = datasetBoundaryOriginal;
		}
		Double pixelXDouble = ((spatialCoordinate.x - datasetBoundary.getMinX()) / (datasetBoundary.getMaxX() - datasetBoundary.getMinX()))*resolutionX;
		Double xRemainder = (spatialCoordinate.x - datasetBoundary.getMinX()) % (datasetBoundary.getMaxX() - datasetBoundary.getMinX());
		Double pixelYDouble = ((spatialCoordinate.y - datasetBoundary.getMinY()) / (datasetBoundary.getMaxY() - datasetBoundary.getMinY()))*resolutionY;
		Double yRemainder = (spatialCoordinate.y - datasetBoundary.getMinY()) / (datasetBoundary.getMaxY() - datasetBoundary.getMinY());
		int pixelX = pixelXDouble.intValue();
		int pixelY = pixelYDouble.intValue();
		if(xRemainder==0.0&&pixelXDouble!=0.0)
		{
			pixelX--;
		}
		if(pixelX>=resolutionX)
		{
			pixelX--;
		}
		if(yRemainder==0.0&&pixelYDouble!=0)
		{
			pixelY--;
		}
		if(pixelY>=resolutionY)
		{
			pixelY--;
		}
		return new Tuple2<Integer,Integer>(pixelX,pixelY);
	}
	

	/**
	 * Gets the width from height.
	 *
	 * @param y the y
	 * @param boundary the boundary
	 * @return the int
	 */
	public static int GetWidthFromHeight(int y, Envelope boundary)
	{
		int x = (int)(y* (boundary.getWidth()/boundary.getHeight()));
		return x;
	}
	
	/**
	 * Encode 2 D to 1 D id.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param twoDimensionIdX the two dimension id X
	 * @param twoDimensionIdY the two dimension id Y
	 * @return the int
	 * @throws Exception the exception
	 */
	public static int Encode2DTo1DId(int resolutionX, int resolutionY, int twoDimensionIdX,int twoDimensionIdY) throws Exception
	{
		if((twoDimensionIdX+twoDimensionIdY*resolutionX)<0 ||(twoDimensionIdX+twoDimensionIdY*resolutionX)>(resolutionX*resolutionY-1))
		{
			throw new Exception("[RasterizationUtils][Encode2DTo1DId] This given 2 dimension coordinate is "+twoDimensionIdX+" "+twoDimensionIdY+". This coordinate is out of the given boundary and will be dropped.");
		}
		return twoDimensionIdX+twoDimensionIdY*resolutionX;
	}
	

	/**
	 * Decode 1 D to 2 D id.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param oneDimensionId the one dimension id
	 * @return the tuple 2
	 */
	public static Tuple2<Integer,Integer> Decode1DTo2DId(int resolutionX, int resolutionY, int oneDimensionId)
	{
		int twoDimensionIdX = oneDimensionId % resolutionX;
		int twoDimensionIdY = oneDimensionId / resolutionX;
		return new Tuple2<Integer,Integer>(twoDimensionIdX,twoDimensionIdY);
	}
	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Point spatialObject,boolean reverseSpatialCoordinate)
	{
		Tuple2<Integer,Integer> pixelCoordinate = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinate(),reverseSpatialCoordinate);
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		try {
			result.add(new Tuple2<Integer,Long>(Encode2DTo1DId(resolutionX,resolutionY,pixelCoordinate._1,pixelCoordinate._2),(long) 1));
		} catch (Exception e) {
			/*
			 * This spatial object is out of the given dataset boudanry. It is ignored here.
			 */
		}
		return result;
	}
	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Envelope spatialObject,boolean reverseSpatialCoordinate)
	{
		
		Tuple2<Integer,Integer> pixelMinBound = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,new Coordinate(spatialObject.getMinX(),spatialObject.getMinY()),reverseSpatialCoordinate);
		Tuple2<Integer,Integer> pixelMaxBound = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,new Coordinate(spatialObject.getMaxX(),spatialObject.getMaxY()),reverseSpatialCoordinate);
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		result.addAll(FindPixelCoordinates(resolutionX,resolutionY,pixelMinBound,new Tuple2<Integer,Integer>(pixelMinBound._1,pixelMaxBound._2),reverseSpatialCoordinate));
		result.addAll(FindPixelCoordinates(resolutionX,resolutionY,new Tuple2<Integer,Integer>(pixelMinBound._1,pixelMaxBound._2),pixelMaxBound,reverseSpatialCoordinate));
		result.addAll(FindPixelCoordinates(resolutionX,resolutionY,pixelMaxBound,new Tuple2<Integer,Integer>(pixelMaxBound._1,pixelMinBound._2),reverseSpatialCoordinate));
		result.addAll(FindPixelCoordinates(resolutionX,resolutionY,new Tuple2<Integer,Integer>(pixelMaxBound._1,pixelMinBound._2),pixelMinBound,reverseSpatialCoordinate));
		return result;
	} 
	
	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Polygon spatialObject,boolean reverseSpatialCoordinate)
	{
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		for(int i=0;i<spatialObject.getCoordinates().length-1;i++)
		{
			Tuple2<Integer,Integer> pixelCoordinate1 = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinates()[i],reverseSpatialCoordinate);
			Tuple2<Integer,Integer> pixelCoordinate2 = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinates()[i+1],reverseSpatialCoordinate);
			result.addAll(FindPixelCoordinates(resolutionX,resolutionY,pixelCoordinate1,pixelCoordinate2,reverseSpatialCoordinate));
		}
		return result;
	}
	

	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @param objectWeight the object weight
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Polygon spatialObject,boolean reverseSpatialCoordinate,Long objectWeight)
	{
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		GeometryFactory geometryfactory = new GeometryFactory();
		ArrayList<Coordinate> coordinatesList = new ArrayList<Coordinate>();
		LinearRing linear;
		for(int i=0;i<spatialObject.getCoordinates().length;i++)
		{
			Tuple2<Integer,Integer> pixelCoordinate = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinates()[i],reverseSpatialCoordinate);
			coordinatesList.add(new Coordinate(pixelCoordinate._1,pixelCoordinate._2));
		}
		coordinatesList.add(coordinatesList.get(0));
		linear = geometryfactory.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
		Polygon polygon = new Polygon(linear, null, geometryfactory);
		int minPixelX = (int)polygon.getEnvelopeInternal().getMinX();
		int maxPixelX = (int)polygon.getEnvelopeInternal().getMaxX();
		int minPixelY = (int)polygon.getEnvelopeInternal().getMinY();
		int maxPixelY = (int)polygon.getEnvelopeInternal().getMaxY();
		for(int j = minPixelY;j<=maxPixelY;j++)
		{
			for(int i=minPixelX;i<=maxPixelX;i++)
			{
				if(polygon.contains(geometryfactory.createPoint(new Coordinate(i,j))))
				{
					int serialId;
					try {
						serialId = Encode2DTo1DId(resolutionX,resolutionY,i,j);
						result.add(new Tuple2<Integer,Long>(serialId,new Long(objectWeight)));
					} catch (Exception e) {
						/*
						 * This spatial object is out of the given dataset boundary. It is ignored here.
						 */
					}
				}
			}
		}
		return result;
	}

	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @param objectWeight the object weight
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, Envelope spatialObject,boolean reverseSpatialCoordinate,Long objectWeight)
	{
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		Coordinate minBound = new Coordinate(spatialObject.getMinX(),spatialObject.getMinY());
		Coordinate maxBound = new Coordinate(spatialObject.getMaxX(),spatialObject.getMaxY());
		Tuple2<Integer,Integer> minPixelBound = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,minBound,reverseSpatialCoordinate);
		Tuple2<Integer,Integer> maxPixelBound = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,maxBound,reverseSpatialCoordinate);
		int minPixelX = minPixelBound._1;
		int maxPixelX = maxPixelBound._1;
		int minPixelY = minPixelBound._2;
		int maxPixelY = maxPixelBound._2;
		for(int j = minPixelY;j<=maxPixelY;j++)
		{
			for(int i=minPixelX;i<=maxPixelX;i++)
			{
				int serialId;
				try {
					serialId = Encode2DTo1DId(resolutionX,resolutionY,i,j);
					result.add(new Tuple2<Integer,Long>(serialId,new Long(objectWeight)));
				} catch (Exception e) {
					/*
					 * This spatial object is out of the given dataset boundary. It is ignored here.
					 */
				}

			}
		}
		return result;
	}
	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param pixelCoordinate1 the pixel coordinate 1
	 * @param pixelCoordinate2 the pixel coordinate 2
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Tuple2<Integer,Integer> pixelCoordinate1, Tuple2<Integer,Integer> pixelCoordinate2,boolean reverseSpatialCoordinate)
	{
		/*
		 * This function uses Bresenham's line algorithm to plot pixels touched by a given line segment.
		 */
	    int x1 = pixelCoordinate1._1;
	    int y1 = pixelCoordinate1._2;
	    int x2 = pixelCoordinate2._1;
	    int y2 = pixelCoordinate2._2;
		int dx = x2 - x1;
		int dy = y2 - y1;
		int ux = dx > 0 ? 1: -1; // x direction
		int uy = dy > 0 ? 1: -1; // y direction
		int x = x1, y = y1;
		int eps = 0; //cumulative errors
		dx = Math.abs(dx); 
		dy = Math.abs(dy);
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		if (dx > dy) 
		{
			for (x = x1; x != x2; x += ux)
			{
				try {
					result.add(new Tuple2<Integer,Long>(Encode2DTo1DId(resolutionX,resolutionY,x,y),(long) 1));
				} catch (Exception e) {
					/*
					 * This spatial object is out of the given dataset boudanry. It is ignored here.
					 */
				}
				eps += dy;
				if ((eps << 1) >= dx)
				{
					y += uy; eps -= dx;
				}
			}
		}
		else
		{
			for (y = y1; y != y2; y += uy)
			{
				try {
					result.add(new Tuple2<Integer,Long>(Encode2DTo1DId(resolutionX,resolutionY,x,y),(long) 1));
				} catch (Exception e) {
					/*
					 * This spatial object is out of the given dataset boudanry. It is ignored here.
					 */
				}
				eps += dx;
				if ((eps << 1) >= dy)
				{
					x += ux; eps -= dy;
				}
			}
		}   
		return result;
	}
	
	/**
	 * Find pixel coordinates.
	 *
	 * @param resolutionX the resolution X
	 * @param resolutionY the resolution Y
	 * @param datasetBoundary the dataset boundary
	 * @param spatialObject the spatial object
	 * @param reverseSpatialCoordinate the reverse spatial coordinate
	 * @return the list
	 */
	public static List<Tuple2<Integer,Long>> FindPixelCoordinates(int resolutionX, int resolutionY, Envelope datasetBoundary, LineString spatialObject,boolean reverseSpatialCoordinate)
	{	
		List<Tuple2<Integer,Long>> result = new ArrayList<Tuple2<Integer,Long>>();
		for(int i=0;i<spatialObject.getCoordinates().length-1;i++)
		{
			Tuple2<Integer,Integer> pixelCoordinate1 = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinates()[i],reverseSpatialCoordinate);
			Tuple2<Integer,Integer> pixelCoordinate2 = FindOnePixelCoordinate(resolutionX,resolutionY,datasetBoundary,spatialObject.getCoordinates()[i+1],reverseSpatialCoordinate);
			result.addAll(FindPixelCoordinates(resolutionX,resolutionY,pixelCoordinate1,pixelCoordinate2,reverseSpatialCoordinate));
		}
		return result;
		
	}
}
