/**
 * FILE: EnvelopeWithGrid.java
 * PATH: org.datasyslab.geospark.geometryObjects.EnvelopeWithGrid.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Envelope;


// TODO: Auto-generated Javadoc
/**
 * The Class EnvelopeWithGrid.
 */
public class EnvelopeWithGrid extends Envelope{
    
    /** The grid. */
    public int grid;

    /**
     * Instantiates a new envelope with grid.
     *
     * @param x1 the x 1
     * @param x2 the x 2
     * @param y1 the y 1
     * @param y2 the y 2
     * @param grid the grid
     */
    public EnvelopeWithGrid(double x1, double x2, double y1, double y2, int grid) {
        super(x1, x2, y1, y2);
        this.grid = grid;
    }

    /**
     * Instantiates a new envelope with grid.
     *
     * @param e the e
     * @param grid the grid
     */
    public EnvelopeWithGrid(Envelope e, int grid) {
        super(e);
        this.grid = grid;
    }
}
