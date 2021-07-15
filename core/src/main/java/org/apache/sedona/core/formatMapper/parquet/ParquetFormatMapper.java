package org.apache.sedona.core.formatMapper.parquet;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class ParquetFormatMapper<T extends Geometry> implements Serializable, FlatMapFunction<Iterator<GenericRecord>, T> {
    final static Logger logger = Logger.getLogger(FormatMapper.class);
    protected GeometryFactory factory = new GeometryFactory();
    private GeometryType geometryType;
    private final String geometryColumn;
    private final List<String> userColumns;
    
    public ParquetFormatMapper(GeometryType geometryType, String geometryColumn, List<String> userColumns) {
        this.geometryType = geometryType;
        this.geometryColumn = geometryColumn;
        this.userColumns = userColumns;
    }
    
    private static Coordinate getCoordinate(GenericRecord record) {
        Double x = (Double) record.get(CoordinateSchema.X_COORDINATE);
        Double y = (Double) record.get(CoordinateSchema.Y_COORDINATE);
        return new Coordinate(x, y);
    }
    
    private static Coordinate getCoordinate(GenericContainer val) {
        if (val instanceof GenericRecord) {
            return getCoordinate((GenericRecord) val);
        }
        return getCoordinate((GenericArray) val);
    }
    
    private static Coordinate getCoordinate(GenericArray array) {
        return getCoordinate((GenericRecord) array.get(0));
    }
    
    private static Coordinate[] getCoordinates(GenericArray array) {
        return (Coordinate[]) array.stream().map(record -> getCoordinate((GenericRecord) record)).toArray();
    }
    
    private Point getPoint(Coordinate coordinate) {
        return factory.createPoint(coordinate);
    }
    
    private Circle getCircle(GenericRecord record) {
        Coordinate center = getCoordinate((GenericRecord) record.get(CircleSchema.CENTER));
        Double radius = (Double) record.get(CircleSchema.RADIUS);
        return new Circle(getPoint(center), radius);
    }
    
    private Polygon getPolygon(GenericRecord record) {
        LinearRing exteriorRing =
                factory.createLinearRing(getCoordinates((GenericArray) record.get(PolygonSchema.EXTERIOR_RING)));
        GenericArray holes = (GenericArray) record.get(PolygonSchema.HOLES);
        List<LinearRing> interiorRings = IntStream.range(0, holes.size())
                                                  .mapToObj(holes::get)
                                                  .map(hole -> getCoordinates((GenericArray) (hole)))
                                                  .map(c -> factory.createLinearRing(c))
                                                  .collect(Collectors.toList());
        
        return factory.createPolygon(exteriorRing, (LinearRing[]) interiorRings.toArray());
    }
    
    private Geometry getGeometry(Coordinate[] coordinates, GeometryType type) {
        Geometry geometry = null;
        switch (type) {
            case POINT:
                geometry = factory.createPoint(coordinates[0]);
                break;
            case POLYGON:
                geometry = factory.createPolygon(coordinates);
                break;
            case LINESTRING:
                geometry = factory.createLineString(coordinates);
                break;
            case RECTANGLE:
                // The rectangle mapper reads two coordinates from the input line. The two coordinates are the two on the diagonal.
                assert coordinates.length == 2;
                Coordinate[] polyCoordinates = new Coordinate[5];
                polyCoordinates[0] = coordinates[0];
                polyCoordinates[1] = new Coordinate(coordinates[0].x, coordinates[1].y);
                polyCoordinates[2] = coordinates[1];
                polyCoordinates[3] = new Coordinate(coordinates[1].x, coordinates[0].y);
                polyCoordinates[4] = polyCoordinates[0];
                geometry = factory.createPolygon(polyCoordinates);
                break;
            // Read string to point if no geometry type specified but Sedona should never reach here
            default:
                geometry = factory.createPoint(coordinates[0]);
        }
        return geometry;
    }
    
    private Geometry getGeometry(GenericRecord record) {
        Object geometryColumn = record.get(this.geometryColumn);
        switch (this.geometryType) {
            case CIRCLE:
                return getCircle((GenericRecord) geometryColumn);
            case POINT:
                return getPoint(getCoordinate((GenericContainer) geometryColumn));
            case POLYGON:
            case RECTANGLE: {
                if (geometryColumn instanceof GenericRecord) {
                    return getPolygon((GenericRecord) geometryColumn);
                }
            }
            default:
                return getGeometry(getCoordinates((GenericArray) geometryColumn), this.geometryType);
        }
    }
    
    private void setUserData(Geometry geometry, GenericRecord genericRecord) {
        geometry.setUserData(IntStream.range(0, userColumns.size())
                                      .mapToObj(i -> i)
                                      .collect(Collectors.toMap(userColumns::get,
                                                                i -> genericRecord.get(i + 1))));
    }
    
    @Override
    public Iterator<T> call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
        Iterable<GenericRecord> recordIterable = () -> genericRecordIterator;
        return StreamSupport.stream(recordIterable.spliterator(), false).map(record -> {
            T geometry = (T) getGeometry(record);
            setUserData(geometry, record);
            return geometry;
        }).iterator();
    }
}
