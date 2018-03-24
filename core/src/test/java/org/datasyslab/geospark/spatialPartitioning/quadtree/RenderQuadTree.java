/*
 * FILE: RenderQuadTree
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import java.awt.Color;
import java.awt.Graphics;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@SuppressWarnings("serial")
public class RenderQuadTree
        extends QuadTreePanel
{

    int resolutionX = 600;
    int resolutionY = 600;
    protected StandardQuadTree<QuadRectangle> standardQuadTree;

    public static void main(String[] args)
    {
        new RenderQuadTree();
    }

    protected RenderQuadTree()
    {
        createQuadTree();
        setupGui();
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
