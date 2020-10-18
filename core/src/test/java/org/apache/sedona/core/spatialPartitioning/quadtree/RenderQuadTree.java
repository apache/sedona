/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.core.spatialPartitioning.quadtree;

import java.awt.Color;
import java.awt.Graphics;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("serial")
public class RenderQuadTree
        extends QuadTreePanel
{

    protected StandardQuadTree<QuadRectangle> standardQuadTree;
    int resolutionX = 600;
    int resolutionY = 600;

    protected RenderQuadTree()
    {
        createQuadTree();
        setupGui();
    }

    public static void main(String[] args)
    {
        new RenderQuadTree();
    }

    /**
     * Create the StandardQuadTree and add some random points
     *
     * @return
     */
    protected StandardQuadTree<QuadRectangle> createQuadTree()
    {
        standardQuadTree = new StandardQuadTree<>(new QuadRectangle(0, 0, resolutionX, resolutionY), 0, 5, 10);
        standardQuadTree.forceGrowUp(3);

        for (int i = 0; i < 10000; i++) {
            int x = ThreadLocalRandom.current().nextInt(0, resolutionX - 200);
            int y = ThreadLocalRandom.current().nextInt(0, resolutionY - 200);
            QuadRectangle newR = new QuadRectangle(x, y, 1, 1);
            standardQuadTree.insert(newR, newR);
        }

        return standardQuadTree;
    }

    @Override
    protected void paintComponent(Graphics g)
    {
        drawCells(g);
    }

    private void drawCells(Graphics g)
    {
        List<QuadRectangle> zoneList = standardQuadTree.getAllZones();
        g.setColor(Color.BLACK);
        for (QuadRectangle r : zoneList) {
            g.drawRect((int) r.x, (int) r.y, (int) r.width, (int) r.height);
            //System.out.println(r);
        }
        g.setColor(Color.RED);

        int x = ThreadLocalRandom.current().nextInt(0, resolutionX);
        int y = ThreadLocalRandom.current().nextInt(0, resolutionY);
        QuadRectangle matchedResult = standardQuadTree.getZone(x, y);
        QuadRectangle parentZone = null;
        try {
            parentZone = standardQuadTree.getParentZone(x, y, 3);
        }
        catch (Exception e) {
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
        QuadRectangle quadRectangle = new QuadRectangle(x1, y1, 80, 80);

        g.drawRect((int) quadRectangle.x, (int) quadRectangle.y, (int) quadRectangle.width, (int) quadRectangle.height);

        standardQuadTree.assignPartitionIds();

        g.setColor(Color.ORANGE);

        List<QuadRectangle> matchedPartitions = standardQuadTree.findZones(quadRectangle);
        for (QuadRectangle q : matchedPartitions) {
            g.drawRect((int) q.x, (int) q.y, (int) q.width, (int) q.height);
            System.out.println(q.partitionId);
        }
    }
}
