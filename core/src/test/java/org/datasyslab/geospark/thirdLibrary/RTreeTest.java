package org.datasyslab.geospark.thirdLibrary;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.datasyslab.geospark.thirdlibraray.rtree3d.geometry.Point2d;
import org.datasyslab.geospark.thirdlibraray.rtree3d.geometry.Rect2d;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.RTree;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.Stats;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.Leaf;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class RTreeTest {

    @Test
    public void pointSearchTest() {

        final RTree<Point2d> pTree = new RTree<>(new Point2d.Builder(), 2, 8, RTree.Split.AXIAL);

        for(int i=0; i<10; i++) {
            pTree.add(new Point2d(i, i));
        }

        final Rect2d rect = new Rect2d(new Point2d(2,2), new Point2d(8,8));
        final Point2d[] result = new Point2d[10];

        final int n = pTree.search(rect, result);
        Assert.assertEquals(7, n);

        for(int i=0; i<n; i++) {
            Assert.assertTrue(result[i].getCoord(Point2d.X) >= 2);
            Assert.assertTrue(result[i].getCoord(Point2d.X) <= 8);
            Assert.assertTrue(result[i].getCoord(Point2d.Y) >= 2);
            Assert.assertTrue(result[i].getCoord(Point2d.Y) <= 8);
        }
    }

    /**
     * Use an small bounding box to ensure that only expected rectangles are returned.
     * Verifies the count returned from search AND the number of rectangles results.
     */
    @Test
    public void rect2DSearchTest() {

        final int entryCount = 20;

        for (RTree.Split type : RTree.Split.values()) {
            RTree<Rect2d> rTree = createRect2DTree(2, 8, type);
            for (int i = 0; i < entryCount; i++) {
                rTree.add(new Rect2d(i, i, i+3, i+3));
            }

            final Rect2d searchRect = new Rect2d(5, 5, 10, 10);
            Rect2d[] results = new Rect2d[entryCount];

            final int foundCount = rTree.search(searchRect, results);
            int resultCount = 0;
            for(int i = 0; i < results.length; i++) {
                if(results[i] != null) {
                    resultCount++;
                }
            }

            final int expectedCount = 3;
            Assert.assertEquals("[" + type + "] Search returned incorrect search result count - expected: " + expectedCount + " actual: " + foundCount, expectedCount, foundCount);
            Assert.assertEquals("[" + type + "] Search returned incorrect number of rectangles - expected: " + expectedCount + " actual: " + resultCount, expectedCount, resultCount);


        }
    }

    /**
     * Use an small bounding box to ensure that only expected rectangles are returned.
     * Verifies the count returned from search AND the number of rectangles results.
     */
    @Test
    public void rect2DIntersectTest() {

        final int entryCount = 20;

        for (RTree.Split type : RTree.Split.values()) {
            RTree<Rect2d> rTree = createRect2DTree(2, 8, type);
            for (int i = 0; i < entryCount; i++) {
                rTree.add(new Rect2d(i, i, i+3, i+3));
            }

            final Rect2d searchRect = new Rect2d(5, 5, 10, 10);
            Rect2d[] results = new Rect2d[entryCount];

            final int foundCount = rTree.intersects(searchRect, results);
            int resultCount = 0;
            for(int i = 0; i < results.length; i++) {
                if(results[i] != null) {
                    resultCount++;
                }
            }

            final int expectedCount = 9;
            Assert.assertEquals("[" + type + "] Search returned incorrect search result count - expected: " + expectedCount + " actual: " + foundCount, expectedCount, foundCount);
            Assert.assertEquals("[" + type + "] Search returned incorrect number of rectangles - expected: " + expectedCount + " actual: " + resultCount, expectedCount, resultCount);

        }
    }



    /**
     * Use an enormous bounding box to ensure that every rectangle is returned.
     * Verifies the count returned from search AND the number of rectangles results.
     */
    @Test
    public void rect2DSearchAllTest() {

        final int entryCount = 1000;
        final Rect2d[] rects = generateRandomRects(entryCount);

        for (RTree.Split type : RTree.Split.values()) {
            type = RTree.Split.QUADRATIC;
            RTree<Rect2d> rTree = createRect2DTree(2, 8, type);
            for (int i = 0; i < rects.length; i++) {
                rTree.add(rects[i]);
            }

            final Rect2d searchRect = new Rect2d(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
            Rect2d[] results = new Rect2d[entryCount];

            final int foundCount = rTree.search(searchRect, results);
            int resultCount = 0;
            for(int i = 0; i < results.length; i++) {
                if(results[i] != null) {
                    resultCount++;
                }
            }

            List<Leaf> leafs = new ArrayList<Leaf>();
            rTree.search(searchRect, leafs);

            int error = 0;
            for (int i = 0; i < leafs.size(); i++) {
                for (int j = i+1; j < leafs.size(); j++) {
                    if (leafs.get(i).getBound().intersects(leafs.get(j).getBound())) {
                        error++;
                    }
                }
            }

            final AtomicInteger visitCount = new AtomicInteger();
            rTree.search(searchRect, (n) -> {visitCount.incrementAndGet();});
            Assert.assertEquals(entryCount, visitCount.get());

            final AtomicInteger partitionId = new AtomicInteger();
            rTree.assignPartitionId();


            final int expectedCount = entryCount;
            Assert.assertEquals("[" + type + "] Search returned incorrect search result count - expected: " + expectedCount + " actual: " + foundCount, expectedCount, foundCount);
            Assert.assertEquals("[" + type + "] Search returned incorrect number of rectangles - expected: " + expectedCount + " actual: " + resultCount, expectedCount, resultCount);
        }
    }

    /**
     * Collect stats making the structure of trees of each split type
     * more visible.
     */
    @Ignore
    // This test ignored because output needs to be manually evaluated.
    public void treeStructureStatsTest() {

        final int entryCount = 50_000;

        final Rect2d[] rects = generateRandomRects(entryCount);
        for (RTree.Split type : RTree.Split.values()) {
            RTree<Rect2d> rTree = createRect2DTree(2, 8, type);
            for (int i = 0; i < rects.length; i++) {
                rTree.add(rects[i]);
            }

            Stats stats = rTree.collectStats();
            stats.print(System.out);
        }
    }


    @Test
    public void treeContainsTest() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        final Rect2d[] rects = new Rect2d[5];
        for (int i = 0; i < rects.length; i++) {
            rects[i] = new Rect2d(i, i, i + 1, i + 1);
            rTree.add(rects[i]);
        }

        for (int i = 0; i < rects.length; i++) {
            Assert.assertTrue(rTree.contains(rects[i]));
        }
    }


    @Test
    public void treeRemovalTest5Entries() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        final Rect2d[] rects = new Rect2d[5];
        for(int i = 0; i < rects.length; i++){
            rects[i] = new Rect2d(i, i, i+1, i+1);
            rTree.add(rects[i]);
        }

        for(int i = 1; i < rects.length; i++) {
            rTree.remove(rects[i]);
            Assert.assertEquals(rects.length-i, rTree.getEntryCount());
        }

        Assert.assertTrue("Missing hyperRect that should  be found " + rects[0], rTree.contains(rects[0]));

        for(int i = 1; i < rects.length; i++) {
            Assert.assertFalse("Found hyperRect that should have been removed on search " + rects[i], rTree.contains(rects[i]));
        }

        final Rect2d hr = new Rect2d(0,0,5,5);
        rTree.add(hr);
        Assert.assertTrue(rTree.contains(hr));
        Assert.assertTrue("Found hyperRect that should have been removed on search", rTree.getEntryCount() != 0);
    }

    @Test
    public void treeGetEntryCount() {

        final int NENTRY = 500;

        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        for(int i = 0; i < NENTRY; i++){
            final Rect2d rect = new Rect2d(i, i, i+1, i+1);
            rTree.add(rect);
        }

        Assert.assertEquals(NENTRY, rTree.getEntryCount());
    }


    @Test
    public void treeRemovalTestDuplicates() {

        final int NENTRY = 50;

        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        final Rect2d[] rect = new Rect2d[2];
        for(int i = 0; i < rect.length; i++){
            rect[i] = new Rect2d(i, i, i+1, i+1);
            rTree.add(rect[i]);
        }

        for(int i = 0; i< NENTRY; i++) {
            rTree.add(rect[1]);
        }

        Assert.assertEquals(NENTRY+2, rTree.getEntryCount());

        for(int i = 0; i < rect.length; i++) {
            rTree.remove(rect[i]);
        }

        for(int i = 0; i < rect.length; i++) {
            Assert.assertFalse("Found hyperRect that should have been removed " + rect[i], rTree.contains(rect[i]));
        }
    }

    @Test
    public void treeRemovalTest1000Entries() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        final Rect2d[] rect = new Rect2d[1000];
        for(int i = 0; i < rect.length; i++){
            rect[i] = new Rect2d(i, i, i+1, i+1);
            rTree.add(rect[i]);
        }

        for(int i = 0; i < rect.length; i++) {
            rTree.remove(rect[i]);
        }

        for(int i = 0; i < rect.length; i++) {
            Assert.assertFalse("Found hyperRect that should have been removed" + rect[i], rTree.contains(rect[i]));
        }

        Assert.assertFalse("Found hyperRect that should have been removed on search ", rTree.getEntryCount() > 0);
    }

    @Test
    public void treeSingleRemovalTest() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        Rect2d rect = new Rect2d(0,0,2,2);
        rTree.add(rect);
        Assert.assertTrue("Did not add HyperRect to Tree", rTree.getEntryCount() > 0);
        rTree.remove(rect);
        Assert.assertTrue("Did not remove HyperRect from Tree", rTree.getEntryCount() == 0);
        rTree.add(rect);
        Assert.assertTrue("Tree nulled out and could not add HyperRect back in", rTree.getEntryCount() > 0);
    }

    @Ignore
    // This test ignored because output needs to be manually evaluated.
    public void treeRemoveAndRebalanceTest() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        Rect2d[] rect = new Rect2d[65];
        for(int i = 0; i < rect.length; i++){
            if(i < 4){ rect[i] = new Rect2d(0,0,1,1); }
            else if(i < 8) { rect[i] = new Rect2d(2, 2, 4, 4); }
            else if(i < 12) { rect[i] = new Rect2d(4,4,5,5); }
            else if(i < 16) { rect[i] = new Rect2d(5,5,6,6); }
            else if(i < 20) { rect[i] = new Rect2d(6,6,7,7); }
            else if(i < 24) { rect[i] = new Rect2d(7,7,8,8); }
            else if(i < 28) { rect[i] = new Rect2d(8,8,9,9); }
            else if(i < 32) { rect[i] = new Rect2d(9,9,10,10); }
            else if(i < 36) { rect[i] = new Rect2d(2,2,4,4); }
            else if(i < 40) { rect[i] = new Rect2d(4,4,5,5); }
            else if(i < 44) { rect[i] = new Rect2d(5,5,6,6); }
            else if(i < 48) { rect[i] = new Rect2d(6,6,7,7); }
            else if(i < 52) { rect[i] = new Rect2d(7,7,8,8); }
            else if(i < 56) { rect[i] = new Rect2d(8,8,9,9); }
            else if(i < 60) { rect[i] = new Rect2d(9,9,10,10); }
            else if(i < 65) { rect[i] = new Rect2d(1,1,2,2); }
        }
        for(int i = 0; i < rect.length; i++){
            rTree.add(rect[i]);
        }
        Stats stat = rTree.collectStats();
        stat.print(System.out);
        for(int i = 0; i < 5; i++){
            rTree.remove(rect[64]);
        }
        Stats stat2 = rTree.collectStats();
        stat2.print(System.out);
    }

    @Test
    public void treeUpdateTest() {
        final RTree<Rect2d> rTree = createRect2DTree(RTree.Split.QUADRATIC);

        Rect2d rect = new Rect2d(0, 1, 2, 3);
        rTree.add(rect);
        Rect2d oldRect = new Rect2d(0,1,2,3);
        Rect2d newRect = new Rect2d(1,2,3,4);
        rTree.update(oldRect, newRect);
        Rect2d[] results = new Rect2d[2];
        final int num = rTree.search(newRect, results);
        Assert.assertTrue("Did not find the updated HyperRect", num == 1);
        System.out.print(results[0]);
    }

    @Test
    public void testAddsubtreeWithSideTree() {
        final RTree<Rect2d> rTree = createRect2DTree(3, 6, RTree.Split.QUADRATIC);

        final Rect2d search;

        rTree.add(new Rect2d(2, 2, 4, 4));
        rTree.add(search = new Rect2d(5, 2, 6, 3));

        // now make sure root node is a branch
        for(int i=0; i<5; i++) {
            rTree.add(new Rect2d(3.0 - 1.0/(10.0+i),3.0 - 1.0/(10.0+i), 3.0 + 1.0/(10.0+i),3.0 + 1.0/(10.0+i)));
        }

        // add subtree/child on first rectangle - fully contained
        rTree.add(new Rect2d(2.5, 2.5, 3.5, 3.5));

        Assert.assertEquals(8, rTree.getEntryCount());

        final AtomicInteger hitCount = new AtomicInteger();
        // but 5, 2, 6, 3 must still be found!
        rTree.search(search, (closure) -> { hitCount.incrementAndGet();});

        Assert.assertEquals(1, hitCount.get());

    }

    /**
     * Generate 'count' random rectangles with fixed ranges.
     *
     * @param count - number of rectangles to generate
     * @return array of generated rectangles
     */
    @Ignore
    static Rect2d[] generateRandomRects(int count) {
        final Random rand = new Random(13);

        // changing these values changes the rectangle sizes and consequently the distribution density
        final int minX = 500;
        final int minY = 500;
        final int maxXRange = 25;
        final int maxYRange = 25;

        final double hitProb = 1.0 * count * maxXRange * maxYRange / (minX * minY);

        final Rect2d[] rects = new Rect2d[count];
        for (int i = 0; i < count; i++) {
            final int x1 = rand.nextInt(minX);
            final int y1 = rand.nextInt(minY);
            final int x2 = x1 + rand.nextInt(maxXRange);
            final int y2 = y1 + rand.nextInt(maxYRange);
            rects[i] = new Rect2d(x1, y1, x2, y2);
        }

        return rects;
    }

    /**
     * Create a tree capable of holding rectangles with default minM (2) and maxM (8) values.
     *
     * @param splitType - type of leaf to use (affects how full nodes get split)
     * @return tree
     */
    @Ignore
    static RTree<Rect2d> createRect2DTree(RTree.Split splitType) {
        return createRect2DTree(2, 8, splitType);
    }

    /**
     * Create a tree capable of holding rectangles with specified m and M values.
     *
     * @param minM - minimum number of entries in each leaf
     * @param maxM - maximum number of entries in each leaf
     * @param splitType - type of leaf to use (affects how full nodes get split)
     * @return tree
     */
    @Ignore
    static RTree<Rect2d> createRect2DTree(int minM, int maxM, RTree.Split splitType) {
        return new RTree<>(new Rect2d.Builder(), minM, maxM, splitType);
    }

}
