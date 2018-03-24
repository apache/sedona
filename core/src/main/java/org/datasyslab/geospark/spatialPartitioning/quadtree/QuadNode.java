/**
 * FILE: QuadNode.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.quadtree.QuadNode.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning.quadtree;

import java.io.Serializable;

public class QuadNode<T> implements Serializable{
    QuadRectangle r;
    T element;

    QuadNode(QuadRectangle r, T element) {
        this.r = r;
        this.element = element;
    }

    @Override
    public String toString() {
        return r.toString();
    }
}
