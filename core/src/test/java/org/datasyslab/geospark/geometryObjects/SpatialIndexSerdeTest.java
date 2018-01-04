package org.datasyslab.geospark.geometryObjects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
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
        //test empty tree
        testQuadTree(0);
        testSTRTree(0);
        // test non-empty tree
        testQuadTree(10000);
        testSTRTree(10000);
    }

    private void testQuadTree(int treeSize) throws Exception {
        SpatialIndexSerde serializer = new SpatialIndexSerde();
        kryo.register(Quadtree.class, serializer);

        Output output = new Output(new FileOutputStream("out.dat"));
        Quadtree exact = (Quadtree) generateQuadTree(treeSize, Quadtree.class);
        kryo.writeClassAndObject(output, exact);
        output.close();


        Input input = new Input(new FileInputStream("out.dat"));
        Quadtree expect = (Quadtree) kryo.readClassAndObject(input);
        input.close();

        List exactItems = exact.queryAll();
        List expectItems = expect.queryAll();

        // assert result equals
        assertThat(exactItems, is(expectItems));
    }

    private void testSTRTree(int treeSize) throws Exception {
        SpatialIndexSerde serializer = new SpatialIndexSerde();
        kryo.register(STRtree.class, serializer);

        Output output = new Output(new FileOutputStream("out.dat"));
        STRtree exact = (STRtree) generateQuadTree(treeSize, STRtree.class);
        kryo.writeClassAndObject(output, exact);
        output.close();


        Input input = new Input(new FileInputStream("out.dat"));
        STRtree expect = (STRtree) kryo.readClassAndObject(input);
        input.close();

        // query all points
        List exactRes = exact.query(new Envelope(-180,180,-90,90));
        List expectRes = expect.query(new Envelope(-180,180,-90,90));

        // assert size equal and content equal
        assertThat(exactRes, is(expectRes));

    }

    private SpatialIndex generateQuadTree(int geomNum, Class aClass){
        Random random = new Random();
        SpatialIndex quadtree;
        // initialize according to class pointed
        if(aClass == Quadtree.class) quadtree = new Quadtree();
        else quadtree = new STRtree();

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