/*
 * FILE: MultiLineString
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
 * Wraps {@link com.vividsolutions.jts.geom.MultiLineString}
 */
public class MultiLineString extends com.vividsolutions.jts.geom.MultiLineString {

    public static com.vividsolutions.jts.geom.LineString[] getLineStrings(com.vividsolutions.jts.geom.GeometryCollection geometry) {
        com.vividsolutions.jts.geom.LineString[] res = new com.vividsolutions.jts.geom.LineString[geometry.getNumGeometries()];
        for (int i = 0; i < res.length; i++)
            res[i] = (com.vividsolutions.jts.geom.LineString) geometry.getGeometryN(i);
        return res;
    }

    private static LineString[] convertLineStrings(com.vividsolutions.jts.geom.LineString[] lineStrings) {
        GeometryFactory factory = new GeometryFactory();
        LineString[] res = new LineString[lineStrings.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = (LineString)factory.fromJTS(lineStrings[i]);
        }
        return res;
    }

    public MultiLineString(Object original) {
        this((com.vividsolutions.jts.geom.MultiLineString) original);
    }

    public MultiLineString(com.vividsolutions.jts.geom.MultiLineString original) {
        this(getLineStrings(original), original.getFactory());
        GeometryCommonUtils.initUserDataFrom(this, original);
    }

    /**
     * {@link com.vividsolutions.jts.geom.MultiLineString#MultiLineString(com.vividsolutions.jts.geom.LineString[], com.vividsolutions.jts.geom.GeometryFactory)}
     */
    public MultiLineString(com.vividsolutions.jts.geom.LineString[] lineStrings, com.vividsolutions.jts.geom.GeometryFactory factory) {
        super(convertLineStrings(lineStrings), new GeometryFactory(factory));
        GeometryCommonUtils.initUserDataFrom(this, this);
    }

    /**
     * Compares to given geometry using {@link com.vividsolutions.jts.geom.MultiLineString#equals(Geometry)}
     * Also compares userData
     */
    @Override
    public boolean equals(Geometry g) {
        return super.equals(g) && GeometryCommonUtils.userDataEquals(this, g);
    }

    /**
     * Compares to given geometry using {@link com.vividsolutions.jts.geom.MultiLineString#equals(Object)}
     * Also compares userData
     */
    @Override
    public boolean equals(Object o) {
        return super.equals(o) && GeometryCommonUtils.userDataEquals(this, o);
    }

    /**
     * Produces {@link MultiLineString#toString()} (a WKT representation of the geometry)
     * , concatenated with the userData string (if exists) as a TSV
     */
    @Override
    public String toString() {
        return GeometryCommonUtils.makeString(super.toString(), getUserData());
    }
}
