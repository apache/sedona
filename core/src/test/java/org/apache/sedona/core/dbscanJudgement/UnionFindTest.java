package org.apache.sedona.core.dbscanJudgement;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UnionFindTest {
    @Test
    public void testUnion() {
        UnionFindImpl unionFind = new UnionFindImpl(10);
        unionFind.union(0, 7);
        unionFind.union(3, 2);
        unionFind.union(8, 7);
        unionFind.union(1, 2);

        List<Integer> expectedFinalIds = Arrays.asList(0, 2, 2, 2, 4, 5, 6, 0, 0, 9);
        List<Integer> expectedFinalSizes = Arrays.asList(3, 0, 3, 0, 1, 1, 1, 0, 0, 1);

        Assert.assertEquals(10, unionFind.getN());
        Assert.assertEquals(6, unionFind.getNumClusters());
        Assert.assertThat(unionFind.getClusters(), CoreMatchers.is(expectedFinalIds));
        Assert.assertThat(unionFind.getClusterSizes(), CoreMatchers.is(expectedFinalSizes));
    }

    @Test
    public void testOrderedByCluster() {
        List<Integer> finalClusters = Arrays.asList(0, 2, 2, 2, 4, 5, 6, 0, 0, 2);
        List<Integer> finalSizes = Arrays.asList(3, 0, 4, 0, 1, 1, 1, 0, 0, 0);

        UnionFindImpl unionFind = new UnionFindImpl(10);
        unionFind.setNumClusters(5);
        unionFind.setClusters(finalClusters);
        unionFind.setClusterSizes(finalSizes);

        List<Integer> idsByCluster = Arrays.asList(unionFind.orderedByCluster());
        List<Boolean> encounteredCluster = Arrays.asList(false, false, false, false, false, false, false, false, false, false);

        for (int i = 0; i < unionFind.getN(); i++) {
            int c = finalClusters.get(idsByCluster.get(i));
            if (!encounteredCluster.get(c)) {
                encounteredCluster.set(c, true);
            } else {
                /* If we've seen an element of this cluster before, then the
                 * current cluster must be the same as the previous cluster. */
                int cPrev = finalClusters.get(idsByCluster.get(i - 1));
                Assert.assertEquals(c, cPrev);
            }
        }
    }

    @Test
    public void testCollapseClusterIds() {
        UnionFindImpl unionFind = new UnionFindImpl(10);

        List<Integer> clusters = Arrays.asList(8, 5, 5, 5, 7, 5, 8, 7, 8, 7);
        List<Integer> clusterSizes = Arrays.asList(3, 4, 4, 4, 3, 4, 3, 3, 3, 3);
        unionFind.setClusters(clusters);
        unionFind.setClusterSizes(clusterSizes);

        /* 5 -> 0
         * 7 -> 1
         * 8 -> 2
         *
         */
        List<Integer> expectedCollapsedIds = Arrays.asList(2, 0, 0, 0, 1, 0, 2, 1, 2, 1);
        List<Integer> collapsedIds = Arrays.asList(unionFind.getCollapsedClusterIds(new HashSet<>()));

        Assert.assertThat(collapsedIds, CoreMatchers.is(expectedCollapsedIds));

        Set<Integer> isInCluster = new HashSet<>(Arrays.asList(1, 2, 3, 5));
        List<Integer> expectedCollapsedIds2 = Arrays.asList(8, 0, 0, 0, 7, 0, 8, 7, 8, 7);
        List<Integer> collapsedIds2 = Arrays.asList(unionFind.getCollapsedClusterIds(isInCluster));

        for (int i = 0; i < unionFind.getN(); i++) {
            if (isInCluster.contains(i)) {
                Assert.assertEquals(expectedCollapsedIds2.get(i), collapsedIds2.get(i));
            }
        }
    }
}