package org.datasyslab.geospark.spatioTemporal.RDD;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.SpatioTemporalObjects.Cube;
import org.datasyslab.geospark.SpatioTemporalObjects.Point3D;


public class QueryFilter implements Function<Point3D, Boolean > {

    Cube queryCube;

    public QueryFilter(Cube queryCube) {
        this.queryCube = queryCube;
    }

    @Override
    public Boolean call(Point3D point3D) throws Exception {
        return queryCube.getMinX() <= point3D.getX()
                && queryCube.getMaxX() >= point3D.getX()
                && queryCube.getMinY() <= point3D.getY()
                && queryCube.getMaxY() >= point3D.getY()
                && queryCube.getMinZ() <= point3D.getZ()
                && queryCube.getMaxZ() >= point3D.getZ();
    }

}
