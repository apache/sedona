/*
 * FILE: GeometryCollection
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

import org.locationtech.jts.geom.Geometry;

import java.util.Objects;

/**
 * Wraps {@link org.locationtech.jts.geom.GeometryCollection}
 */
public class GeometryCollection extends org.locationtech.jts.geom.GeometryCollection {

    public static Geometry[] getGeometries(org.locationtech.jts.geom.GeometryCollection geometry) {
        Geometry[] res = new Geometry[geometry.getNumGeometries()];
        for (int i = 0; i < res.length; i++)
            res[i] = geometry.getGeometryN(i);
        return res;
    }

    private static Geometry[] convertGeometries(org.locationtech.jts.geom.Geometry[] geometries) {
        GeometryFactory factory = new GeometryFactory();
        Geometry[] res = new Geometry[geometries.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = factory.fromJTS(geometries[i]);
        }
        return res;
    }

    public GeometryCollection(Object original) {
        this((org.locationtech.jts.geom.GeometryCollection) original);
    }

    public GeometryCollection(org.locationtech.jts.geom.GeometryCollection original) {
        this(getGeometries(original), original.getFactory());
        GeometryCommonUtils.initUserDataFrom(this, original);
    }

    /**
     * {@link org.locationtech.jts.geom.GeometryCollection#GeometryCollection(Geometry[], org.locationtech.jts.geom.GeometryFactory)}
     */
    public GeometryCollection(Geometry[] geometries, org.locationtech.jts.geom.GeometryFactory factory) {
        super(convertGeometries(geometries), new GeometryFactory(factory));
        GeometryCommonUtils.initUserDataFrom(this, this);
    }

    /**
     * Compares to given geometry using {@link org.locationtech.jts.geom.GeometryCollection#equals(Geometry)}
     * Also compares userData
     */
    @Override
    public boolean equals(Geometry g) {
        return super.equals(g) && GeometryCommonUtils.userDataEquals(this, g);
    }

    /**
     * Compares to given geometry using {@link org.locationtech.jts.geom.GeometryCollection#equals(Object)}
     * Also compares userData
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o) && GeometryCommonUtils.userDataEquals(this, o);
    }

    /**
     * Produces {@link GeometryCollection#toString()} (a WKT representation of the geometry)
     * , concatenated with the userData string (if exists) as a TSV
     */
    @Override
    public String toString() {
        return GeometryCommonUtils.makeString(super.toString(), getUserData());
    }
}
