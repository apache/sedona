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
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.RTree;
import org.datasyslab.geospark.thirdlibraray.rtree3d.spatial.RectBuilder;

public final class Point3d implements HyperPoint {
    public final static int X = 0;
    public final static int Y = 1;
    public final static int Z = 2;


    final double x, y, z;

    public Point3d(final double x, final double y, final double z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    @Override
    public int getNDim() {
        return 3;
    }

    @Override
    public Double getCoord(final int d) {
        if(d==X) {
            return x;
        } else if(d==Y) {
            return y;
        } else if(d==Z) {
            return z;
        } else {
            throw new IllegalArgumentException("Invalid dimension");
        }
    }

    @Override
    public double distance(final HyperPoint p) {
        final Point3d p2 = (Point3d)p;

        final double dx = p2.x-x;
        final double dy = p2.y-y;
        final double dz = p2.z-z;
        return Math.sqrt(dx*dx + dy*dy + dz*dz);
    }

    @Override
    public double distance(final HyperPoint p, final int d) {
        final Point3d p2 = (Point3d)p;
        if(d == X) {
            return Math.abs(p2.x - x);
        } else if (d == Y) {
            return Math.abs(p2.y - y);
        } else if (d == Z) {
            return Math.abs(p2.z - z);
        } else {
            throw new IllegalArgumentException("Invalid dimension");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Point3d p = (Point3d)o;
        return RTree.isEqual(x, p.x) &&
                RTree.isEqual(y, p.y) &&
                RTree.isEqual(z, p.z);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(x) ^
                31*Double.hashCode(y) ^
                31*31*Double.hashCode(z);
    }

    public final static class Builder implements RectBuilder<Point3d> {

        @Override
        public HyperRect getBBox(final Point3d point) {
            return new Rect3d(point);
        }

        @Override
        public HyperRect getMbr(final HyperPoint p1, final HyperPoint p2) {
            return new Rect3d((Point3d)p1, (Point3d)p2);
        }
    }
}