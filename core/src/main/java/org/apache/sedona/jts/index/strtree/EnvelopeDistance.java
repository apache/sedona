/*
 * Copyright (c) 2019 Martin Davis.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License 2.0
 * and Eclipse Distribution License v. 1.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *
 * http://www.eclipse.org/org/documents/edl-v10.php.
 */
package org.apache.sedona.jts.index.strtree;

import org.locationtech.jts.geom.Envelope;

/**
 * Functions for computing distances between {@link Envelope}s.
 * 
 * @author mdavis
 *
 */
public class EnvelopeDistance 
{
  /**
   * Computes the maximum distance between the points defining two envelopes.
   * It is equal to the length of the diagonal of 
   * the envelope containing both input envelopes.
   * This is a coarse upper bound on the distance between 
   * geometries bounded by the envelopes.
   * 
   * @param env1 an envelope
   * @param env2 an envelope
   * @return the maximum distance between the points defining the envelopes
   */
  public static double maximumDistance(Envelope env1, Envelope env2)
  {
    double minx = Math.min(env1.getMinX(), env2.getMinX());
    double miny = Math.min(env1.getMinY(), env2.getMinY());
    double maxx = Math.max(env1.getMaxX(), env2.getMaxX());
    double maxy = Math.max(env1.getMaxY(), env2.getMaxY());
    return distance(minx, miny, maxx, maxy);
  }
  
  private static double distance(double x1, double y1, double x2, double y2) {
    double dx = x2 - x1;
    double dy = y2 - y1;
    return Math.sqrt(dx * dx + dy * dy);    
  }
  
  /**
   * Computes the Min-Max Distance between two {@link Envelope}s.
   * It is equal to the minimum of the maximum distances between all pairs of
   * edge segments from the two envelopes.
   * This is the tight upper bound on the distance between 
   * geometric items bounded by the envelopes.
   * <p>
   * Theoretically this bound can be used in the R-tree nearest-neighbour branch-and-bound search
   * instead of {@link #maximumDistance(Envelope, Envelope)}.
   * However, little performance improvement is observed in practice.
   * 
   * @param a an envelope
   * @param b an envelope
   * @return the min-max-distance between the envelopes
   */
  public static double minMaxDistance(Envelope a, Envelope b)
  {
    double aminx = a.getMinX();
    double aminy = a.getMinY();
    double amaxx = a.getMaxX();
    double amaxy = a.getMaxY();
    double bminx = b.getMinX();
    double bminy = b.getMinY();
    double bmaxx = b.getMaxX();
    double bmaxy = b.getMaxY();
    
    double dist =         maxDistance(aminx, aminy, aminx, amaxy, bminx, bminy, bminx, bmaxy);
    dist = Math.min(dist, maxDistance(aminx, aminy, aminx, amaxy, bminx, bminy, bmaxx, bminy));
    dist = Math.min(dist, maxDistance(aminx, aminy, aminx, amaxy, bmaxx, bmaxy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(aminx, aminy, aminx, amaxy, bmaxx, bmaxy, bmaxx, bminy));
  
    dist = Math.min(dist, maxDistance(aminx, aminy, amaxx, aminy, bminx, bminy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(aminx, aminy, amaxx, aminy, bminx, bminy, bmaxx, bminy));
    dist = Math.min(dist, maxDistance(aminx, aminy, amaxx, aminy, bmaxx, bmaxy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(aminx, aminy, amaxx, aminy, bmaxx, bmaxy, bmaxx, bminy));
    
    dist = Math.min(dist, maxDistance(amaxx, amaxy, aminx, amaxy, bminx, bminy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, aminx, amaxy, bminx, bminy, bmaxx, bminy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, aminx, amaxy, bmaxx, bmaxy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, aminx, amaxy, bmaxx, bmaxy, bmaxx, bminy));
    
    dist = Math.min(dist, maxDistance(amaxx, amaxy, amaxx, aminy, bminx, bminy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, amaxx, aminy, bminx, bminy, bmaxx, bminy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, amaxx, aminy, bmaxx, bmaxy, bminx, bmaxy));
    dist = Math.min(dist, maxDistance(amaxx, amaxy, amaxx, aminy, bmaxx, bmaxy, bmaxx, bminy));
    
    return dist;
  }

  /**
   * Computes the maximum distance between two line segments.
   * 
   * @param ax1 x ordinate of first endpoint of segment 1
   * @param ay1 y ordinate of first endpoint of segment 1
   * @param ax2 x ordinate of second endpoint of segment 1
   * @param ay2 y ordinate of second endpoint of segment 1
   * @param bx1 x ordinate of first endpoint of segment 2
   * @param by1 y ordinate of first endpoint of segment 2
   * @param bx2 x ordinate of second endpoint of segment 2
   * @param by2 y ordinate of second endpoint of segment 2
   * @return maximum distance between the segments
   */
  private static double maxDistance(double ax1, double ay1, double ax2, double ay2, 
      double bx1, double by1, double bx2, double by2) {
    double dist = distance(ax1, ay1, bx1, by1);
    dist = Math.max(dist, distance(ax1, ay1, bx2, by2));
    dist = Math.max(dist, distance(ax2, ay2, bx1, by1));
    dist = Math.max(dist, distance(ax2, ay2, bx2, by2));
    return dist;
  }
}
