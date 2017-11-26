package org.datasyslab.geospark.serde;

import com.esotericsoftware.kryo.Kryo;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.log4j.Logger;
import org.apache.spark.serializer.KryoRegistrator;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.geometryObjects.GeometrySerde;

public class GeoSparkKryoRegistrator implements KryoRegistrator {

    final static Logger log = Logger.getLogger(GeoSparkKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        GeometrySerde serializer = new GeometrySerde();

        log.warn("Registering custom serializers for geometry types");

        kryo.register(Point.class, serializer);
        kryo.register(LineString.class, serializer);
        kryo.register(Polygon.class, serializer);
        kryo.register(MultiPoint.class, serializer);
        kryo.register(MultiLineString.class, serializer);
        kryo.register(MultiPolygon.class, serializer);
        kryo.register(GeometryCollection.class, serializer);
        kryo.register(Circle.class, serializer);
    }
}
