/*
 * FILE: Point
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datasyslab.geospark.jts.geom;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.util.Objects;

/**
 * Wraps {@link com.vividsolutions.jts.geom.Point}
 */
public class Point extends com.vividsolutions.jts.geom.Point {

    /**
     * {@link com.vividsolutions.jts.geom.Point#Point(CoordinateSequence, GeometryFactory)}
     */
    public Point(CoordinateSequence coordinates, GeometryFactory factory) {
        super(coordinates, factory);
        if (getUserData() == null) setUserData("");
    }

    /**
     * Compares to given geometry using {@link com.vividsolutions.jts.geom.Point#equals(Geometry)}
     * Also compares userData
     */
    @Override
    public boolean equals(Geometry g) {
        return super.equals(g) && Objects.equals(getUserData(), g.getUserData());
    }

    /**
     * Compares to given geometry using {@link com.vividsolutions.jts.geom.Point#equals(Object)}
     * Also compares userData
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(getUserData(), ((Geometry) o).getUserData());
    }

    /**
     * Produces {@link Point#toString()} (a WKT representation of the geometry)
     * , concatenated with the userData string (if exists) as a TSV
     */
    @Override
    public String toString() {
        if (!Objects.equals(getUserData(), ""))
            return super.toString() + "\t" + getUserData();
        return super.toString();
    }
}
