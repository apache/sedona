/*
 * FILE: MultiPolygon
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
 * Wraps {@link org.locationtech.jts.geom.MultiPolygon}
 */
public class MultiPolygon extends org.locationtech.jts.geom.MultiPolygon {

    public static org.locationtech.jts.geom.Polygon[] getPolygons(org.locationtech.jts.geom.GeometryCollection geometry) {
        org.locationtech.jts.geom.Polygon[] res = new org.locationtech.jts.geom.Polygon[geometry.getNumGeometries()];
        for (int i = 0; i < res.length; i++)
            res[i] = (org.locationtech.jts.geom.Polygon) geometry.getGeometryN(i);
        return res;
    }

    private static Polygon[] convertPolygons(org.locationtech.jts.geom.Polygon[] polygons) {
        GeometryFactory factory = new GeometryFactory();
        Polygon[] res = new Polygon[polygons.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = (Polygon)factory.fromJTS(polygons[i]);
        }
        return res;
    }

    public MultiPolygon(Object original) {
        this((org.locationtech.jts.geom.MultiPolygon) original);
    }

    public MultiPolygon(org.locationtech.jts.geom.MultiPolygon original) {
        this(getPolygons(original), original.getFactory());
        GeometryCommonUtils.initUserDataFrom(this, original);
    }

    /**
     * {@link org.locationtech.jts.geom.MultiPolygon#MultiPolygon(org.locationtech.jts.geom.Polygon[], org.locationtech.jts.geom.GeometryFactory)}
     */
    public MultiPolygon(org.locationtech.jts.geom.Polygon[] polygons, org.locationtech.jts.geom.GeometryFactory factory) {
        super(convertPolygons(polygons), new GeometryFactory(factory));
        GeometryCommonUtils.initUserDataFrom(this, this);
    }

    /**
     * Compares to given geometry using {@link org.locationtech.jts.geom.MultiPolygon#equals(Geometry)}
     * Also compares userData
     */
    @Override
    public boolean equals(Geometry g) {
        return super.equals(g) && GeometryCommonUtils.userDataEquals(this, g);
    }

    /**
     * Compares to given geometry using {@link org.locationtech.jts.geom.MultiPolygon#equals(Object)}
     * Also compares userData
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o) && GeometryCommonUtils.userDataEquals(this, o);
    }

    /**
     * Produces {@link MultiPolygon#toString()} (a WKT representation of the geometry)
     *  , concatenated with the userData string (if exists) as a TSV
     */
    @Override
    public String toString() {
        return GeometryCommonUtils.makeString(super.toString(), getUserData());
    }
}
