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

import com.vividsolutions.jts.geom.Geometry;

import java.util.Objects;

/**
 * Wraps {@link org.datasyslab.geospark.geometryObjects.Circle}
 */
public class Circle extends org.datasyslab.geospark.geometryObjects.Circle {

    public Circle(org.datasyslab.geospark.geometryObjects.Circle original) {
        super(original.getCenterGeometry(), original.getRadius());
        setUserData(original.getUserData());
    }

    /**
     * {@link org.datasyslab.geospark.geometryObjects.Circle#Circle(Geometry, Double)}
     */
    public Circle(Geometry centerGeometry, Double givenRadius) {
        super(centerGeometry, givenRadius);
        if (getUserData() == null) setUserData("");
    }

    /**
     * Compares to given geometry using {@link org.datasyslab.geospark.geometryObjects.Circle#equals(Geometry)}
     * Also compares userData
     */
    @Override
    public boolean equals(Geometry g) {
        return super.equals(g) && Objects.equals(getUserData(), g.getUserData());
    }

    /**
     * Compares to given geometry using {@link org.datasyslab.geospark.geometryObjects.Circle#equals(Object)}
     * Also compares userData
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o) && Objects.equals(getUserData(), ((Geometry) o).getUserData());
    }

    /**
     * Produces {@link Circle#toString()} (a WKT representation of the geometry)
     * , concatenated with the userData string (if exists) as a TSV
     */
    @Override
    public String toString() {
        if (!Objects.equals(getUserData(), ""))
            return super.toString() + "\t" + getUserData();
        return super.toString();
    }
}
