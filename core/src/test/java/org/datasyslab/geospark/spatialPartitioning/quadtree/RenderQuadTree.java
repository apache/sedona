/**
 * FILE: RenderQuadTree.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.quadtree.RenderQuadTree.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("serial")
public class RenderQuadTree extends QuadTreePanel {

	int resolutionX = 600;
	int resolutionY = 600;
	protected StandardQuadTree<QuadRectangle> standardQuadTree = new StandardQuadTree<QuadRectangle>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0);;

	public static void main(String[] args) {
		new RenderQuadTree();
	}

	protected RenderQuadTree() {
		createQuadTree();
		setupGui();
	}

	/**
	 * Create the StandardQuadTree and add some random points
	 * 
	 * @return
	 */
	protected StandardQuadTree<QuadRectangle> createQuadTree() {
		StandardQuadTree.maxItemByNode = 5;
		StandardQuadTree.maxLevel = 10;
		standardQuadTree.forceGrowUp(3);
		
		for(int i = 0;i< 10000;i++)
		{
			int x = ThreadLocalRandom.current().nextInt(0, resolutionX-200);
			int y = ThreadLocalRandom.current().nextInt(0, resolutionY-200);
			QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
			standardQuadTree.insert(newR, newR);
		}
		

		return standardQuadTree;
	}

	@Override
	protected void paintComponent(Graphics g) {
		drawCells(g);
	}

	private void drawCells(Graphics g) {
		ArrayList<QuadRectangle> zoneList = new ArrayList<QuadRectangle>();
		standardQuadTree.getAllZones(zoneList);
		g.setColor(Color.BLACK);
		for(QuadRectangle r:zoneList)
		{
			g.drawRect((int)r.x,(int)r.y,(int)r.width,(int)r.height);
			//System.out.println(r);
		}
		g.setColor(Color.RED);

		int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
		int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
		QuadRectangle matchedResult = standardQuadTree.getZone(x,y);
		QuadRectangle parentZone = null;
		try {
			parentZone = standardQuadTree.getParentZone(x,y,3);
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		g.drawRect((int)parentZone.x,(int)parentZone.y,(int)parentZone.width+5,(int)parentZone.height+5);
		g.drawRect((int)matchedResult.x,(int)matchedResult.y,(int)matchedResult.width,(int)matchedResult.height);
		g.drawRect(x,y,5,5);
		*/
		int x1 = ThreadLocalRandom.current().nextInt(0, resolutionX);
		int y1 = ThreadLocalRandom.current().nextInt(0, resolutionY);
		g.setColor(Color.RED);
		QuadRectangle quadRectangle = new QuadRectangle(x1,y1,80,80);

		g.drawRect((int)quadRectangle.x,(int)quadRectangle.y,(int)quadRectangle.width,(int)quadRectangle.height);


		HashSet<Integer> uniqueIdList = new HashSet<Integer>();
		standardQuadTree.getAllLeafNodeUniqueId(uniqueIdList);
		HashMap<Integer,Integer> serialIdMapping = standardQuadTree.getSeriaIdMapping(uniqueIdList);
		standardQuadTree.decidePartitionSerialId(serialIdMapping);


		g.setColor(Color.ORANGE);

		ArrayList<QuadRectangle> matchedPartitions = new ArrayList<QuadRectangle>();
		standardQuadTree.getZone(matchedPartitions,quadRectangle);
		for(QuadRectangle q:matchedPartitions)
		{
			g.drawRect((int)q.x,(int)q.y,(int)q.width,(int)q.height);
			System.out.println(q.partitionId);
		}
	}


}
