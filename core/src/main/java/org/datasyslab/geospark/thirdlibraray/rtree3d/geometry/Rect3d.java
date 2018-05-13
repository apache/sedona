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

public final class Rect3d implements HyperRect {
    private final Point3d min, max;

    Rect3d(final Point3d p) {
        min = new Point3d(p.x, p.y, p.z);
        max = new Point3d(p.x, p.y, p.z);
    }

    public Rect3d(final double x1, final double y1, final double z1, final double x2, final double y2, final double z2) {
        min = new Point3d(x1, y1, z1);
        max = new Point3d(x2, y2, z2);
    }

    Rect3d(final Point3d point1, final Point3d point2) {

        final double minx, maxx, miny, maxy, minz, maxz;

        if(point1.x < point2.x) {
            minx = point1.x;
            maxx = point2.x;
        } else {
            minx = point2.x;
            maxx = point1.x;
        }

        if(point1.y < point2.y) {
            miny = point1.y;
            maxy = point2.y;
        } else {
            miny = point2.y;
            maxy = point1.y;
        }

        if(point1.z < point2.z) {
            minz = point1.z;
            maxz = point2.z;
        } else {
            minz = point2.z;
            maxz = point1.z;
        }

        min = new Point3d(minx, miny, minz);
        max = new Point3d(maxx, maxy, maxz);
    }

    @Override
    public HyperRect getMbr(final HyperRect r) {
        final Rect3d r2 = (Rect3d)r;
        final double minX = Math.min(min.x, r2.min.x);
        final double minY = Math.min(min.y, r2.min.y);
        final double minZ = Math.min(min.z, r2.min.z);
        final double maxX = Math.max(max.x, r2.max.x);
        final double maxY = Math.max(max.y, r2.max.y);
        final double maxZ = Math.max(max.z, r2.max.z);

        return new Rect3d(minX, minY, minZ, maxX, maxY, maxZ);

    }

    @Override
    public int getNDim() {
        return 3;
    }

    @Override
    public HyperPoint getCentroid() {
        final double dx = min.x + (max.x - min.x)/2.0;
        final double dy = min.y + (max.y - min.y)/2.0;
        final double dz = min.z + (max.z - min.z)/2.0;

        return new Point3d(dx, dy, dz);
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
        } else if(d == 2) {
            return max.z - min.z;
        } else {
            throw new IllegalArgumentException("Invalid dimension");
        }
    }

    @Override
    public boolean contains(final HyperRect r) {
        final Rect3d r2 = (Rect3d)r;

        return min.x <= r2.min.x &&
                max.x >= r2.max.x &&
                min.y <= r2.min.y &&
                max.y >= r2.max.y &&
                min.z <= r2.min.z &&
                max.z >= r2.max.z;
    }

    @Override
    public boolean intersects(final HyperRect r) {
        final Rect3d r2 = (Rect3d)r;

        boolean res =  !(min.x > r2.max.x) &&
                !(r2.min.x > max.x) &&
                !(min.y > r2.max.y) &&
                !(r2.min.y > max.y) &&
                !(min.z > r2.max.z) &&
                !(r2.min.z > max.z);

        boolean a = !(min.x > r2.max.x);
        boolean b = !(r2.min.x > max.x);
        boolean c = !(min.y > r2.max.y);
        boolean d = !(r2.min.y > max.y);
        boolean e = !(min.z > r2.max.z);
        boolean f = !(r2.min.z > max.z);
        return res;
    }

    @Override
    public double cost() {
        final double dx = max.x - min.x;
        final double dy = max.y - min.y;
        final double dz = max.z - min.z;
        return Math.abs(dx*dy*dz);
    }

    @Override
    public double perimeter() {
        double p = 0.0;
        final int nD = this.getNDim();
        for(int d = 0; d<nD; d++) {
            p += 4.0 * this.getRange(d);
        }
        return p;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Rect3d rect3d = (Rect3d) o;

        return min.equals(rect3d.min) &&
                max.equals(rect3d.max);
    }

    @Override
    public int hashCode() {
        return min.hashCode() ^ 31*max.hashCode();
    }

    public String toString() {

        return "(" +
                Double.toString(min.x) +
                ',' +
                Double.toString(min.y) +
                ',' +
                Double.toString(min.z) +
                ')' +
                ' ' +
                '(' +
                Double.toString(max.x) +
                ',' +
                Double.toString(max.y) +
                ',' +
                Double.toString(max.z) +
                ')';
    }

    public final static class Builder implements RectBuilder<Rect3d> {

        @Override
        public HyperRect getBBox(final Rect3d rect2D) {
            return rect2D;
        }

        @Override
        public HyperRect getMbr(final HyperPoint p1, final HyperPoint p2) {
            return new Rect3d(p1.getCoord(0), p1.getCoord(1), p1.getCoord(2), p2.getCoord(0), p2.getCoord(1),
                    p2.getCoord(2));
        }
    }
}