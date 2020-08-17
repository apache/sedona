package org.datasyslab.geospark.jts.geom;

import com.vividsolutions.jts.geom.*;

public class GeometryFactory extends com.vividsolutions.jts.geom.GeometryFactory {

    public GeometryFactory(com.vividsolutions.jts.geom.GeometryFactory original) {
        super(original.getPrecisionModel(), original.getSRID(), original.getCoordinateSequenceFactory());
    }

    public GeometryFactory(CoordinateSequenceFactory coordinateSequenceFactory) {
        super(coordinateSequenceFactory);
    }

    public GeometryFactory(PrecisionModel precisionModel) {
        super(precisionModel);
    }

    public GeometryFactory(PrecisionModel precisionModel, int SRID) {
        super(precisionModel, SRID);
    }

    public GeometryFactory() {
        super();
    }

    public Geometry fromJTS(Geometry original) {
        if (original instanceof Point || original instanceof LinearRing || original instanceof LineString ||
                original instanceof Polygon || original instanceof Circle || original instanceof MultiLineString ||
                original instanceof MultiPolygon || original instanceof MultiPoint || original instanceof GeometryCollection) {
            return original;
        }

        if (original instanceof com.vividsolutions.jts.geom.Point) {
            return new Point(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.LinearRing) {
            return new LinearRing(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.LineString) {
            return new LineString(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.Polygon) {
            return new Polygon(original);
        }

        if (original instanceof org.datasyslab.geospark.geometryObjects.Circle) {
            return new Circle(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.MultiLineString) {
            return new MultiLineString(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.MultiPolygon) {
            return new MultiPolygon(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.MultiPoint) {
            return new MultiPoint(original);
        }

        if (original instanceof com.vividsolutions.jts.geom.GeometryCollection) {
            return new GeometryCollection(original);
        }

        return original;
    }


    public Point createPoint(Coordinate coordinate) {
        return this.createPoint(coordinate != null ? this.getCoordinateSequenceFactory().create(new Coordinate[]{coordinate}) : null);
    }

    public Point createPoint(CoordinateSequence coordinates) {
        return new Point(coordinates, this);
    }

    public MultiLineString createMultiLineString(com.vividsolutions.jts.geom.LineString[] lineStrings) {
        return new MultiLineString(lineStrings, this);
    }

    public GeometryCollection createGeometryCollection(Geometry[] geometries) {
        return new GeometryCollection(geometries, this);
    }

    public MultiPolygon createMultiPolygon(com.vividsolutions.jts.geom.Polygon[] polygons) {
        return new MultiPolygon(polygons, this);
    }

    public LinearRing createLinearRing(Coordinate[] coordinates) {
        return this.createLinearRing(coordinates != null ? this.getCoordinateSequenceFactory().create(coordinates) : null);
    }

    public LinearRing createLinearRing(CoordinateSequence coordinates) {
        return new LinearRing(coordinates, this);
    }

    public MultiPoint createMultiPoint(com.vividsolutions.jts.geom.Point[] point) {
        return new MultiPoint(point, this);
    }

    public MultiPoint createMultiPoint(Coordinate[] coordinates) {
        return this.createMultiPoint(coordinates != null ? this.getCoordinateSequenceFactory().create(coordinates) : null);
    }

    public MultiPoint createMultiPoint(CoordinateSequence coordinates) {
        if (coordinates == null) {
            return this.createMultiPoint(new Point[0]);
        } else {
            Point[] points = new Point[coordinates.size()];

            for(int i = 0; i < coordinates.size(); ++i) {
                CoordinateSequence ptSeq = this.getCoordinateSequenceFactory().create(1, coordinates.getDimension());
                CoordinateSequences.copy(coordinates, i, ptSeq, 0, 1);
                points[i] = this.createPoint(ptSeq);
            }

            return this.createMultiPoint(points);
        }
    }

    public Polygon createPolygon(com.vividsolutions.jts.geom.LinearRing shell, com.vividsolutions.jts.geom.LinearRing[] holes) {
        return new Polygon(shell, holes, this);
    }

    public Polygon createPolygon(CoordinateSequence coordinates) {
        return this.createPolygon(this.createLinearRing(coordinates));
    }

    public Polygon createPolygon(Coordinate[] coordinates) {
        return this.createPolygon(this.createLinearRing(coordinates));
    }

    public Polygon createPolygon(com.vividsolutions.jts.geom.LinearRing shell) {
        return this.createPolygon(shell, (LinearRing[])null);
    }

    public LineString createLineString(Coordinate[] coordinates) {
        return this.createLineString(coordinates != null ? this.getCoordinateSequenceFactory().create(coordinates) : null);
    }

    public LineString createLineString(CoordinateSequence coordinates) {
        return new LineString(coordinates, this);
    }
}
