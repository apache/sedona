package org.datasyslab.geospark.jts.geom;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.util.Objects;

public class GeometryCollection extends com.vividsolutions.jts.geom.GeometryCollection {

    public GeometryCollection(Geometry[] geometries, GeometryFactory factory) {
        super(geometries, factory);
        setUserData("");
    }

    @Override
    public boolean equals(com.vividsolutions.jts.geom.Geometry g) {
        return super.equals(g) && Objects.equals(getUserData(), g.getUserData());
    }

    @Override
    public String toString() {
        if (!Objects.equals(getUserData(), ""))
            return super.toString() + "\t" + getUserData();
        return super.toString();
    }
}
