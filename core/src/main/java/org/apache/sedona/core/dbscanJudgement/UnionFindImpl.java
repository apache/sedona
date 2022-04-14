package org.apache.sedona.core.dbscanJudgement;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class UnionFindImpl implements UnionFind {
    private List<Integer> clusters;
    private List<Integer> clusterSizes;
    private int numClusters;
    private final int n;

    public UnionFindImpl(int n) {
        this.n = n;
        this.numClusters = n;
        this.clusters = new ArrayList<>();
        this.clusterSizes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            clusters.add(i);
            clusterSizes.add(1);
        }
    }

    @Override
    public int find(int i) {
        int base = i;
        while (clusters.get(base) != base) {
            base = clusters.get(base);
        }
        while (i != base) {
            int next = clusters.get(i);
            clusters.set(i, base);
            i = next;
        }
        return i;
    }

    @Override
    public int size(int i) {
        return clusterSizes.get(find(i));
    }

    @Override
    public void union(int i, int j) {
        int a = find(i);
        int b = find(j);
        if (a == b) {
            return;
        }
        if (clusterSizes.get(a) < clusterSizes.get(b) ||
                (Objects.equals(clusterSizes.get(a), clusterSizes.get(b)) && a > b)) {
            clusters.set(a, clusters.get(b));
            clusterSizes.set(b, clusterSizes.get(a) + clusterSizes.get(b));
            clusterSizes.set(a, 0);
        } else {
            clusters.set(b, clusters.get(a));
            clusterSizes.set(a, clusterSizes.get(a) + clusterSizes.get(b));
            clusterSizes.set(b, 0);
        }
        numClusters--;
    }

    @Override
    public Integer[] orderedByCluster() {
        Integer[] clusterIdByElemId = new Integer[n];
        for (int i = 0; i < n; i++) {
            find(i);
            clusterIdByElemId[i] = i;
        }
        Arrays.sort(clusterIdByElemId, Comparator.comparing(index -> clusters.get(index)));
        return clusterIdByElemId;
    }

    @Override
    public Integer[] getCollapsedClusterIds(Set<Integer> isInCluster) {
        Integer[] orderedComponents = orderedByCluster();
        Integer[] newIds = new Integer[n];
        int lastOldId = 0, currentNewId = 0;
        boolean encounteredCluster = false;

        System.arraycopy(orderedComponents, 0, newIds, 0, orderedComponents.length);

        if (isInCluster == null) {
            isInCluster = new HashSet<>();
        }

        for (int i = 0; i < n; i++) {
            int j = orderedComponents[i];
            if (isInCluster.isEmpty() || isInCluster.contains(j)) {
                int currentOldId = find(j);
                if (!encounteredCluster) {
                    encounteredCluster = true;
                    lastOldId = currentOldId;
                }
                if (currentOldId != lastOldId) {
                    currentNewId++;
                }
                newIds[j] = currentNewId;
                lastOldId = currentOldId;
            }
        }
        return newIds;
    }

    public List<Integer> getClusters() {
        return clusters;
    }

    public void setClusters(List<Integer> clusters) {
        this.clusters = clusters;
    }

    public List<Integer> getClusterSizes() {
        return clusterSizes;
    }

    public void setClusterSizes(List<Integer> clusterSizes) {
        this.clusterSizes = clusterSizes;
    }

    public int getNumClusters() {
        return numClusters;
    }

    public void setNumClusters(int numClusters) {
        this.numClusters = numClusters;
    }

    public int getN() {
        return n;
    }
}