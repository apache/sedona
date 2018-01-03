package org.datasyslab.geospark.geometryObjects;


import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Node;
import com.vividsolutions.jts.index.quadtree.NodeBase;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class SpatialIndexSerde extends Serializer{

    private static final Logger log = Logger.getLogger(SpatialIndexSerde.class);

    private GeometrySerde geometrySerde;

    public SpatialIndexSerde() {
        super();
        geometrySerde = new GeometrySerde();
    }

    public SpatialIndexSerde(GeometrySerde geometrySerde) {
        super();
        this.geometrySerde = geometrySerde;
    }

    private enum Type{

        QUADTREE(0),
        RTREE(1);

        private final int id;

        Type(int id) {
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
    public void write(Kryo kryo, Output output, Object o) {
        if( o instanceof Quadtree){
            // serialize quadtree index
            writeType(output, Type.QUADTREE);
            writeQuadTree(kryo,output, ((Quadtree) o));
        }else if(o instanceof STRtree){
            //serialize rtree index
            writeType(output, Type.RTREE);
        }
    }

    /**
     * write head and root node
     * byte  1byte  empty or not, 0 means empty, 1 means not empty
     *
     * @param kryo
     * @param output
     * @param tree
     */
    private void writeQuadTree(Kryo kryo, Output output, Quadtree tree){
        if(tree.isEmpty()){
            output.writeByte(0);
        }else {
            output.writeByte(1);
            // write root
            List items = tree.getRoot().getItems();
            output.writeInt(items.size());
            for(Object item : items){
                geometrySerde.write(kryo, output, item);
            }
            Node[] subNodes= tree.getRoot().getSubnode();
            for(int i = 0;i < 4; ++i){
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private void writeQuadTreeNode(Kryo kryo, Output output, Node node){
        // write head first
        if(node == null || node.isEmpty()){
            output.writeByte(0);
        }else{ // not empty
            output.writeByte(1);
            // write node information, envelope and level
            geometrySerde.write(kryo, output, node.getEnvelope());
            output.writeInt(node.getLevel());
            List items = node.getItems();
            output.writeInt(items.size());
            for( Object obj : items){
                geometrySerde.write(kryo, output, obj);
            }
            Node[] subNodes = node.getSubnode();
            for(int i = 0;i < 4; ++i){
                writeQuadTreeNode(kryo, output, subNodes[i]);
            }
        }
    }

    private void writeType(Output output, Type type){
        output.writeByte((byte)type.id);
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        byte typeID = input.readByte();
        Type indexType = Type.fromId(typeID);
        switch (indexType){
            case QUADTREE:{
                Quadtree index = new Quadtree();
                boolean notEmpty = (input.readByte() & 0x01) == 1;
                if(!notEmpty) return index;
                int itemSize = input.readInt();
                List items = new ArrayList();
                for(int i = 0;i < itemSize; ++i){
                    items.add(geometrySerde.read(kryo, input, Geometry.class));
                }
                index.getRoot().setItems(items);
                for(int i = 0;i < 4; ++i){
                    index.getRoot().getSubnode()[i] = readQuadTreeNode(kryo, input);
                }
                return index;
            }
            case RTREE:{
                break;
            }
            default:{
                throw new UnsupportedOperationException("can't deserialize spatial index of type" + indexType);
            }
        }
        return null;
    }

    private Node readQuadTreeNode(Kryo kryo, Input input){
        boolean notEmpty = (input.readByte() & 0x01) == 1;
        if(!notEmpty) return null;
        Envelope envelope = (Envelope) geometrySerde.read(kryo, input, Envelope.class);
        int level = input.readInt();
        Node node = new Node(envelope, level);
        int itemSize = input.readInt();
        List items = new ArrayList();
        for(int i = 0;i < itemSize; ++i){
            items.add(geometrySerde.read(kryo, input, Geometry.class));
        }
        node.setItems(items);
        // read children
        for(int i = 0;i < 4; ++i){
            node.getSubnode()[i] = readQuadTreeNode(kryo, input);
        }
        return node;
    }
}
