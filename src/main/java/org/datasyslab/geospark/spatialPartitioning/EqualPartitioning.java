package org.datasyslab.geospark.spatialPartitioning;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid;

import com.vividsolutions.jts.geom.Envelope;

/**
 * This class generates a grid file. Each grid in this file has the same length, width and area.
 * @author GeoSpark Team
 *
 */
public class EqualPartitioning implements Serializable{

	HashSet<EnvelopeWithGrid> grids=new HashSet<EnvelopeWithGrid>();

	
	/**
	 * @param boundary The boundary of the entire dataset
	 * @param partitions Grids number in the grid file
	 */
	public  EqualPartitioning(Envelope boundary,int partitions)
	{
		//Local variable should be declared here
		Double root=Math.sqrt(partitions);
		int partitionsAxis;
		double intervalX;
		double intervalY;
		List<Double> boundsX=new ArrayList<Double>();
		List<Double> boundsY=new ArrayList<Double>();
		//Calculate how many bounds should be on each axis
		partitionsAxis=root.intValue();
		intervalX=(boundary.getMaxX()-boundary.getMinX())/partitionsAxis;
		intervalY=(boundary.getMaxY()-boundary.getMinY())/partitionsAxis;
		for(int i=0;i<partitionsAxis;i++)
		{
			boundsX.add(boundary.getMinX()+i*intervalX);
			boundsY.add(boundary.getMinY()+i*intervalY);
		}
		boundsX.add(boundary.getMaxX());
		boundsY.add(boundary.getMaxY());
		for(int i=0;i<boundsX.size()-1;i++)
		{
			for(int j=0;j<boundsY.size()-1;j++)
			{
				Envelope grid=new Envelope(boundsX.get(i),boundsX.get(i+1),boundsY.get(j),boundsY.get(j+1));
				grids.add(new EnvelopeWithGrid(grid,i*boundsY.size()+j));
			}
		}

		
	}


	/**
	 * @return Return the generated grid file
	 */
	public HashSet<EnvelopeWithGrid> getGrids() {
		
		return this.grids;
		
	}
}
