/*
 * FILE: SpatialIndexSerde
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

package org.datasyslab.geospark.geometryObjects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.quadtree.Node;
import org.locationtech.jts.index.quadtree.Quadtree;
import org.locationtech.jts.index.strtree.AbstractNode;
import org.locationtech.jts.index.strtree.ItemBoundable;
import org.locationtech.jts.index.strtree.STRtree;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides methods to efficiently serialize and deserialize spatialIndex types.
 * <p>
 * Support Quadtree, STRtree types
 * <p>
 * trees are serialized recursively.
 */
public class SpatialIndexSerde
        extends Serializer
{

    private static final Logger log = Logger.getLogger(SpatialIndexSerde.class);

    private GeometrySerde geometrySerde;

    public SpatialIndexSerde()
    {
        super();
        geometrySerde = new GeometrySerde();
    }

    public SpatialIndexSerde(GeometrySerde geometrySerde)
    {
        super();
        this.geometrySerde = geometrySerde;
    }

    private enum Type
    {

        QUADTREE(0),
        RTREE(1);

        private final int id;

        Type(int id)
        {
            this.id = id;
        }

        public static Type fromId(int id)
        {
            for (Type type : values()) {
                if (type.id == id) {
                    return type;
                }
            }

            return null;
        }
    }

    @Override
    public void write(Kryo kryo, Output output, Object o)
    {
        if (o instanceof Quadtree) {
            // serialize quadtree index
            writeType(output, Type.QUADTREE);
            Quadtree tree = (Quadtree) o;
            if (tree.isEmpty()) {
                output.writeByte(0);
            }
            else {
                output.writeByte(1);
                // write root
                List items = tree.getRoot().getItems();
                output.writeInt(items.size());
                for (Object item : items) {
                    geometrySerde.write(kryo, output, item);
                }
                Node[] subNodes = tree.getRoot().getSubnode();
                for (int i = 0; i < 4; ++i) {
                    writeQuadTreeNode(kryo, output, subNodes[i]);
                }
            }
        }
        else if (o instanceof STRtree) {
            //serialize rtree index
            writeType(output, Type.RTREE);
            STRtree tree = (STRtree) o;
            output.writeInt(tree.getNodeCapacity());
            if (tree.isEmpty()) {
                output.writeByte(0);
            }
            else {
                output.writeByte(1);
                // write head
                output.writeByte(tree.isBuilt() ? 1 : 0);
                if (!tree.isBuilt()) {
                    // if not built, itemBoundables will not be null, record it
                    ArrayList itemBoundables = tree.getItemBoundables();
                    output.writeInt(itemBoundables.size());
                    for (Object obj : itemBoundables) {
                        if (!(obj instanceof ItemBoundable)) { throw new UnsupportedOperationException(" itemBoundables should only contain ItemBoundable objects "); }
                        ItemBoundable itemBoundable = (ItemBoundable) obj;
                        // write envelope
                        writeItemBoundable(kryo, output, itemBoundable);
                    }
                }
                else {
                    // if built, write from root
                    writeSTRTreeNode(kryo, output, tree.getRoot());
                }
            }
        }
        else {
            throw new UnsupportedOperationException(" index type not supported ");
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass)
    {
        byte typeID = input.readByte();
        Type indexType = Type.fromId(typeID);
        switch (indexType) {
            case QUADTREE: {
                Quadtree index = new Quadtree();
                boolean notEmpty = (input.readByte() & 0x01) == 1;
                if (!notEmpty) { return index; }
                int itemSize = input.readInt();
                List items = new ArrayList();
                for (int i = 0; i < itemSize; ++i) {
                    items.add(geometrySerde.read(kryo, input, Geometry.class));
                }
                index.getRoot().setItems(items);
                for (int i = 0; i < 4; ++i) {
                    index.getRoot().getSubnode()[i] = readQuadTreeNode(kryo, input);
                }
                return index;
            }
            case RTREE: {
                int nodeCapacity = input.readInt();
                boolean notEmpty = (input.readByte() & 0x01) == 1;
                if (notEmpty) {
                    STRtree index = new STRtree(nodeCapacity);
                    boolean built = (input.readByte() & 0x01) == 1;
                    if (built) {
                        // if built, root is not null, set itemBoundables to null
                        index.setBuilt(true);
                        index.setItemBoundables(null);
                        index.setRoot(readSTRtreeNode(kryo, input));
                    }
                    else {
                        // if not built, just read itemBoundables
                        ArrayList itemBoundables = new ArrayList();
                        int itemSize = input.readInt();
                        for (int i = 0; i < itemSize; ++i) {
                            itemBoundables.add(readItemBoundable(kryo, input));
                        }
                        index.setItemBoundables(itemBoundables);
                    }
                    return index;
                }
                else { return new STRtree(nodeCapacity); }
            }
            default: {
                throw new UnsupportedOperationException("can't deserialize spatial index of type" + indexType);
            }
        }
    }

    private void writeQuadTreeNode(Kryo kryo, Output output, Node node)
    {
        // write head first
        if (node == null || node.isEmpty()) {
            output.writeByte(0);
        }
        else { // not empty
            output.writeByte(1);
            // write node information, envelope and level
            geometrySerde.write(kryo, output, node.getEnvelope());
            output.writeInt(node.getLevel());
            List items = node.getItems();
            output.writeInt(items.size());
            for (Object obj : items) {
                geometrySerde.write(kryo, output, obj);
            }
            Node[] subNodes = node.getSubnode();
            for (int i = 0; i < 4; ++i) {
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private Node readQuadTreeNode(Kryo kryo, Input input)
    {
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if (!notEmpty) { return null; }
        Envelope envelope = (Envelope) geometrySerde.read(kryo, input, Envelope.class);
        int level = input.readInt();
        Node node = new Node(envelope, level);
        int itemSize = input.readInt();
        List items = new ArrayList();
        for (int i = 0; i < itemSize; ++i) {
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        node.setItems(items);
        // read children
        for (int i = 0; i < 4; ++i) {
            node.getSubnode()[i] = readQuadTreeNode(kryo, input);
        }
        return node;
    }

    private void writeSTRTreeNode(Kryo kryo, Output output, AbstractNode node)
    {
        // write head
        output.writeInt(node.getLevel());
        // write children
        List children = node.getChildBoundables();
        int childrenSize = children.size();
        output.writeInt(childrenSize);
        // if children not empty, write children
        if (childrenSize > 0) {
            if (children.get(0) instanceof AbstractNode) {
                // write type as 0, non-leaf node
                output.writeByte(0);
                for (Object obj : children) {
                    AbstractNode child = (AbstractNode) obj;
                    writeSTRTreeNode(kryo, output, child);
                }
            }
            else if (children.get(0) instanceof ItemBoundable) {
                // write type as 1, leaf node
                output.writeByte(1);
                // for leaf node, write items
                for (Object obj : children) {
                    writeItemBoundable(kryo, output, (ItemBoundable) obj);
                }
            }
            else {
                throw new UnsupportedOperationException("wrong node type of STRtree");
            }
        }
    }

    private STRtree.STRtreeNode readSTRtreeNode(Kryo kryo, Input input)
    {
        int level = input.readInt();
        STRtree.STRtreeNode node = new STRtree.STRtreeNode(level);
        int childrenSize = input.readInt();
        boolean isLeaf = (input.readByte() & 0x01) == 1;
        ArrayList children = new ArrayList();
        if (isLeaf) {
            for (int i = 0; i < childrenSize; ++i) {
                children.add(readItemBoundable(kryo, input));
            }
        }
        else {
            for (int i = 0; i < childrenSize; ++i) {
                children.add(readSTRtreeNode(kryo, input));
            }
        }
        node.setChildBoundables(children);
        return node;
    }

    private void writeItemBoundable(Kryo kryo, Output output, ItemBoundable itemBoundable)
    {
        geometrySerde.write(kryo, output, itemBoundable.getBounds());
        geometrySerde.write(kryo, output, itemBoundable.getItem());
    }

    private ItemBoundable readItemBoundable(Kryo kryo, Input input)
    {
        return new ItemBoundable(
                geometrySerde.read(kryo, input, Envelope.class),
                geometrySerde.read(kryo, input, Geometry.class)
        );
    }

    private void writeType(Output output, Type type)
    {
        output.writeByte((byte) type.id);
    }
}
