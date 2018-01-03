package org.datasyslab.geospark.geometryObjects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class SpatialIndexSerdeTest {

    private final Kryo kryo = new Kryo();

    private final WKTReader wktReader = new WKTReader();

    private final GeometryFactory geometryFactory = new GeometryFactory();

    @Test
    public void test() throws Exception {
        SpatialIndexSerde serializer = new SpatialIndexSerde();
        kryo.register(Quadtree.class, serializer);

        Output output = new Output(new FileOutputStream("out.dat"));
        Quadtree tree = generateQuadTree(10000);
        kryo.writeClassAndObject(output, tree);
        output.close();


        Input input = new Input(new FileInputStream("out.dat"));
        Quadtree quadtree = (Quadtree) kryo.readClassAndObject(input);
        System.out.println(quadtree.queryAll().size());
        input.close();

        List sItems = tree.queryAll();
        List dItems = tree.queryAll();

        // assert size equals
        assertEquals(sItems.size(), dItems.size());
        // assert result equals
        assertThat(sItems, is(dItems));
    }

    private Quadtree generateQuadTree(int geomNum){
        Random random = new Random();
        Quadtree quadtree = new Quadtree();
        for(int i = 0;i < geomNum; ++i){
            Point point = geometryFactory.createPoint(new Coordinate(
                    random.nextDouble() % 180,
                    random.nextDouble() % 90
            ));
            quadtree.insert(point.getEnvelopeInternal(), point);
        }
        return quadtree;
    }

}