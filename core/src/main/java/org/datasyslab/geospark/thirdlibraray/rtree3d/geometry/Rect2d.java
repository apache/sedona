package org.datasyslab.geospark.thirdlibraray.rtree3d.geometry;

/*
 * #%L
 * Conversant RTree
 * ~~
 * Conversantmedia.com © 2016, Conversant, Inc. Conversant® is a trademark of Conversant, Inc.
 * ~~
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.HyperPoint;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.HyperRect;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.RectBuilder;

/**
 * Created by jcovert on 6/15/15.
 */
public final class Rect2d implements HyperRect {
    final Point2d min, max;

    public Rect2d(final Point2d p) {
        min = new Point2d(p.x, p.y);
        max = new Point2d(p.x, p.y);
    }

    public Rect2d(final double x1, final double y1, final double x2, final double y2) {
        min = new Point2d(x1, y1);
        max = new Point2d(x2, y2);
    }

    public Rect2d(final Point2d p1, final Point2d p2) {
        final double minX, minY, maxX, maxY;

        if(p1.x < p2.x) {
            minX = p1.x;
            maxX = p2.x;
        } else {
            minX = p2.x;
            maxX = p2.x;
        }

        if(p1.y < p2.y) {
            minY = p1.y;
            maxY = p2.y;
        } else {
            minY = p2.y;
            maxY = p2.y;
        }

        min = new Point2d(minX, minY);
        max = new Point2d(maxX, maxY);
    }

    @Override
    public HyperRect getMbr(final HyperRect r) {
        final Rect2d r2 = (Rect2d)r;
        final double minX = Math.min(min.x, r2.min.x);
        final double minY = Math.min(min.y, r2.min.y);
        final double maxX = Math.max(max.x, r2.max.x);
        final double maxY = Math.max(max.y, r2.max.y);

        return new Rect2d(minX, minY, maxX, maxY);

    }

    @Override
    public int getNDim() {
        return 2;
    }

    @Override
    public HyperPoint getCentroid() {
        final double dx = min.x + (max.x - min.x)/2.0;
        final double dy = min.y + (max.y - min.y)/2.0;

        return new Point2d(dx, dy);
    }

    @Override
    public HyperPoint getMin() {
        return min;
    }

    @Override
    public HyperPoint getMax() {
        return max;
    }

    @Override
    public double getRange(final int d) {
        if(d == 0) {
            return max.x - min.x;
        } else if(d == 1) {
            return max.y - min.y;
        } else {
            throw new IllegalArgumentException("Invalid dimension");
        }
    }

    @Override
    public boolean contains(final HyperRect r) {
        final Rect2d r2 = (Rect2d)r;

        return min.x <= r2.min.x &&
                max.x >= r2.max.x &&
                min.y <= r2.min.y &&
                max.y >= r2.max.y;
    }

    @Override
    public boolean intersects(final HyperRect r) {
        final Rect2d r2 = (Rect2d)r;

        if(min.x > r2.max.x ||
                r2.min.x > max.x ||
                min.y > r2.max.y ||
                r2.min.y > max.y) {
            return false;
        }

        return true;
    }

    @Override
    public double cost() {
        final double dx = max.x - min.x;
        final double dy = max.y - min.y;
        return Math.abs(dx*dy);
    }

    @Override
    public double perimeter() {
        double p = 0.0;
        final int nD = this.getNDim();
        for(int d = 0; d<nD; d++) {
            p += 2.0 * this.getRange(d);
        }
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Rect2d rect2D = (Rect2d) o;

        return min.equals(rect2D.min) &&
                max.equals(rect2D.max);
    }

    @Override
    public int hashCode() {
        return min.hashCode() ^ 31*max.hashCode();
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append('(');
        sb.append(Double.toString(min.x));
        sb.append(',');
        sb.append(Double.toString(min.y));
        sb.append(')');
        sb.append(' ');
        sb.append('(');
        sb.append(Double.toString(max.x));
        sb.append(',');
        sb.append(Double.toString(max.y));
        sb.append(')');

        return sb.toString();
    }

    public final static class Builder implements RectBuilder<Rect2d> {

        @Override
        public HyperRect getBBox(final Rect2d rect2D) {
            return rect2D;
        }

        @Override
        public HyperRect getMbr(final HyperPoint p1, final HyperPoint p2) {
            return new Rect2d(p1.getCoord(0), p1.getCoord(1), p2.getCoord(0), p2.getCoord(1));
        }
    }
}