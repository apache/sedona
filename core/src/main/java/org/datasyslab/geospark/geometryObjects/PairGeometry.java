/**
 * FILE: PairGeometry.java
 * PATH: org.datasyslab.geospark.geometryObjects.PairGeometry.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Geometry;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;

public class PairGeometry<K extends Geometry, V extends Geometry> implements Serializable {
    public K keyGeometry;
    public HashSet<V> valueGeometries;
    public PairGeometry(K keyGeometry, HashSet<V> valueGeometries)
    {
        this.keyGeometry = keyGeometry;
        this.valueGeometries = valueGeometries;
    }
    public Tuple2<K, HashSet<V>> makeTuple2() {
        return new Tuple2<>(keyGeometry, valueGeometries);
    }

    @Override
    public boolean equals(Object o)
    {
        PairGeometry anotherPairGeometry = (PairGeometry)o;
        if (keyGeometry.equals(((PairGeometry) o).keyGeometry)&&valueGeometries.equals(((PairGeometry) o).valueGeometries))
        {
            return true;
        }
        return false;

    }

    @Override
    public int hashCode()
    {
        return keyGeometry.hashCode()*31+valueGeometries.hashCode();
    }
}
