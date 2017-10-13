package org.datasyslab.geospark.formatMapper.shapefileParser.shapes;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.datasyslab.geospark.geometryObjects.Circle;
import org.datasyslab.geospark.geometryObjects.GeometrySerde;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class GeometrySerdeTest {
    private final Kryo kryo = new Kryo();
    private final WKTReader wktReader = new WKTReader();

    @Test
    public void test() throws Exception {
        test("POINT (1.3 4.5)");
        test("MULTIPOINT ((1 1), (1.3 4.5), (5.2 999))");
        test("LINESTRING (1 1, 1.3 4.5, 5.2 999)");
        test("MULTILINESTRING ((1 1, 1.3 4.5, 5.2 999), (0 0, 0 1))");
        test("POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0))");
        test("POLYGON ((0 0, 0 1, 1 1, 1 0.4, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2))");
        test("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0.4, 0 0)), " +
            "((0 0, 0 1, 1 1, 1 0.4, 0 0), (0.2 0.2, 0.5 0.2, 0.5 0.5, 0.2 0.5, 0.2 0.2)))");
        test("GEOMETRYCOLLECTION (POINT(4 6), LINESTRING(4 6,7 10))");
    }

    private void test(String wkt) throws Exception {
        Geometry geometry = parseWkt(wkt);
        Assert.assertEquals(geometry, serde(geometry));

        geometry.setUserData("This is a test");
        Assert.assertEquals(geometry, serde(geometry));

        if (geometry instanceof GeometryCollection) {
            return;
        }

        Circle circle = new Circle(geometry, 1.2);
        Assert.assertEquals(circle, serde(circle));
    }

    private Geometry parseWkt(String wkt) throws ParseException {
        return wktReader.read(wkt);
    }

    private Geometry serde(Geometry input) {
        byte[] ser = serialize(input);
        return kryo.readObject(new Input(ser), input.getClass());
    }

    private byte[] serialize(Geometry input) {
        kryo.register(input.getClass(), new GeometrySerde());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Output output = new Output(bos);
        kryo.writeObject(output, input);
        output.close();
        return bos.toByteArray();
    }
}
