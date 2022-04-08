package org.apache.sedona.core.dbscanJudgement;

import java.util.Set;

public interface UnionFind {
    int find(int i);
    int size(int i);
    void union(int i, int j);
    Integer[] orderedByCluster();
    Integer[] getCollapsedClusterIds();
}
