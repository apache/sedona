package org.datasyslab.geospark.geometryObjects;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;


/**
 * The class extends JTS Envelope with one additional data field
 *
 */
public class EnvelopeWithGrid extends Envelope{
    public int grid;

    /**
     * Initialize an envelope with a grid 
     * @param x1 Xmin	
     * @param x2 Xmax
     * @param y1 Ymin
     * @param y2 Ymax
     * @param grid id
     */
    public EnvelopeWithGrid(double x1, double x2, double y1, double y2, int grid) {
        super(x1, x2, y1, y2);
        this.grid = grid;
    }

    /**
     * Initialize an envelope with a grid 
     * @param e Envelope
     * @param grid id
     */
    public EnvelopeWithGrid(Envelope e, int grid) {
        super(e);
        this.grid = grid;
    }
}
