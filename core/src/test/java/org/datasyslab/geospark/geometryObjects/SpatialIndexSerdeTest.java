/*
 * FILE: SpatialIndexSerdeTest
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Random;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SpatialIndexSerdeTest
{

    private final Kryo kryo = new Kryo();

    private final GeometryFactory geometryFactory = new GeometryFactory();

    private final SpatialIndexSerde spatialIndexSerde = new SpatialIndexSerde();

    @Test
    public void test()
            throws Exception
    {

        kryo.register(Quadtree.class, spatialIndexSerde);
        kryo.register(STRtree.class, spatialIndexSerde);

        // test correctness
        testCorrectness(Quadtree.class);
        testCorrectness(STRtree.class);

        // workbench of compare size
        compareSize(Quadtree.class);
        compareSize(STRtree.class);

//        // workbench of compare time
        compareTime(Quadtree.class);
        compareTime(STRtree.class);
    }

    public void testCorrectness(Class aClass)
            throws IOException
    {
        final int indexSize = 10000;

        SpatialIndex tree = generateIndex(indexSize, aClass);
        // get expect result
        SpatialIndex dtree = deserializeIndexKryo(serializeIndexKryo(tree));

        System.out.println("\n==== test correctness of " + aClass.toString() + "====");

        // test query all object
        assertThat(queryIndex(tree, null), is(queryIndex(dtree, null)));

        // test query window -90,90,-45,45
        Envelope envelope = new Envelope(-90, 90, -45, 45);
        assertThat(queryIndex(tree, envelope), is(queryIndex(dtree, envelope)));

        // test query window 0,90,-20,80
        envelope = new Envelope(0, 90, -20, 80);
        assertThat(queryIndex(tree, envelope), is(queryIndex(dtree, envelope)));
    }

    public void compareSize(Class aClass)
            throws IOException
    {
        final int indexSize = 10000;
        SpatialIndex tree = generateIndex(indexSize, aClass);

        // do without serde first
        byte[] noSerde = serializeIndexNoKryo(tree);

        // do with serde
        if (aClass == Quadtree.class) { kryo.register(Quadtree.class, new SpatialIndexSerde()); }
        else { kryo.register(STRtree.class, new SpatialIndexSerde()); }
        byte[] withSerde = serializeIndexKryo(tree);

        System.out.println("\n==== test size of " + aClass.toString() + "====");
        System.out.println("original size : " + noSerde.length);
        System.out.println("with serde kryo size : " + withSerde.length);
        System.out.println("percent : " + (double) withSerde.length / (double) noSerde.length);
    }

    public void compareTime(Class aClass)
            throws Exception
    {
        System.out.println("\n==== test Serialize time of " + aClass.toString() + "====");
        final int indexSize = 1000000;
        SpatialIndex tree = generateIndex(indexSize, aClass);
        double before, after;

        // do without serde first
        before = System.currentTimeMillis();
        byte[] noSerde = serializeIndexNoKryo(tree);
        after = System.currentTimeMillis();
        System.out.println("originalserialize time : " + (after - before) / 1000);

        before = System.currentTimeMillis();
        deserializeIndexNoKryo(noSerde);
        after = System.currentTimeMillis();
        System.out.println("original deserialize time : " + (after - before) / 1000);
        // do with serde
        if (aClass == Quadtree.class) { kryo.register(Quadtree.class, new SpatialIndexSerde()); }
        else { kryo.register(STRtree.class, new SpatialIndexSerde()); }

        before = System.currentTimeMillis();
        byte[] withSerde = serializeIndexKryo(tree);
        after = System.currentTimeMillis();
        System.out.println("with serde kryo serialize time : " + (after - before) / 1000);

        before = System.currentTimeMillis();
        deserializeIndexKryo(withSerde);
        after = System.currentTimeMillis();
        System.out.println("with serde kryo deserialize time : " + (after - before) / 1000);
    }

    private byte[] serializeIndexNoKryo(SpatialIndex index)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutput output = new ObjectOutputStream(outputStream);
        output.writeObject(index);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatialIndex deserializeIndexNoKryo(byte[] array)
            throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(array);
        ObjectInput input = new ObjectInputStream(inputStream);
        SpatialIndex res = (SpatialIndex) input.readObject();
        input.close();
        inputStream.close();
        return res;
    }

    private byte[] serializeIndexKryo(SpatialIndex index)
            throws IOException
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, index);
        output.close();
        outputStream.close();
        return outputStream.toByteArray();
    }

    private SpatialIndex deserializeIndexKryo(byte[] array)
            throws IOException
    {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(array);
        Input input = new Input(inputStream);
        input.close();
        SpatialIndex res = (SpatialIndex) kryo.readClassAndObject(input);
        inputStream.close();
        return res;
    }

    private List queryIndex(SpatialIndex index, Envelope envelope)
    {
        if (index instanceof Quadtree) {
            Quadtree quadtree = (Quadtree) index;
            if (envelope == null) {
                return quadtree.queryAll();
            }
            else { return quadtree.query(envelope); }
        }
        else if (index instanceof STRtree) {
            STRtree strtree = (STRtree) index;
            if (envelope == null) {
                envelope = new Envelope(-180, 180, -90, 90);
            }
            return strtree.query(envelope);
        }
        else { throw new UnsupportedOperationException("unsupport index type"); }
    }

    private SpatialIndex generateIndex(int geomNum, Class aClass)
    {
        Random random = new Random();
        SpatialIndex quadtree;
        // initialize according to class pointed
        if (aClass == Quadtree.class) { quadtree = new Quadtree(); }
        else { quadtree = new STRtree(); }

        for (int i = 0; i < geomNum; ++i) {
            Point point = geometryFactory.createPoint(new Coordinate(
                    random.nextDouble() * 360 - 180,
                    random.nextDouble() * 180 - 90
            ));
            quadtree.insert(point.getEnvelopeInternal(), point);
        }
        return quadtree;
    }
}