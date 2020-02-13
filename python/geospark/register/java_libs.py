from enum import Enum


class GeoSparkLib(Enum):
    JoinParams = "org.imbruced.geospark.JoinParams"
    Adapter = "org.datasyslab.geosparksql.utils.Adapter"
    GeoSparkWrapper = "org.imbruced.geospark.GeoSparkWrapper"
    JoinQuery = "org.datasyslab.geospark.spatialOperator.JoinQuery"
    KNNQuery = "org.datasyslab.geospark.spatialOperator.KNNQuery"
    CoordinateFactory = "org.imbruced.geospark.CoordinateFactory"
    RangeQuery = "org.datasyslab.geospark.spatialOperator.RangeQuery"
    GeomFactory = "org.imbruced.geospark.GeomFactory"
    Envelope = "com.vividsolutions.jts.geom.Envelope"
    GeoSerializerData = "org.imbruced.geospark.serializers.GeoSerializerData"
    PointRDD = "org.datasyslab.geospark.spatialRDD.PointRDD"
    PolygonRDD = "org.datasyslab.geospark.spatialRDD.PolygonRDD"
    CircleRDD = "org.datasyslab.geospark.spatialRDD.CircleRDD"
    LineStringRDD = "org.datasyslab.geospark.spatialRDD.LineStringRDD"
    RectangleRDD = "org.datasyslab.geospark.spatialRDD.RectangleRDD"
    SpatialRDD = "org.datasyslab.geospark.spatialRDD.SpatialRDD"
    FileDataSplitter = "org.datasyslab.geospark.enums.FileDataSplitter"
    GeoJsonReader = "org.datasyslab.geospark.formatMapper.GeoJsonReader"
    ShapeFileReader = "org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader"
    GeoSparkSQLRegistrator = "org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator"
    StorageLevel = "org.apache.spark.storage.StorageLevel"
    GridType = "org.datasyslab.geospark.enums.GridType"
    IndexType = "org.datasyslab.geospark.enums.IndexType"
    AdapterWrapper = "org.imbruced.geospark.AdapterWrapper"
    WktReader = "org.datasyslab.geospark.formatMapper.WktReader"
    GeometryAdapter = "org.imbruced.geospark.serializers.GeometryAdapter"
    RawJvmIndexRDDSetter = "org.imbruced.geospark.RawJvmIndexRDDSetter"
    ObjectSpatialRDDLoader = "org.imbruced.geospark.ObjectSpatialRDDLoader"
    WkbReader = "org.datasyslab.geospark.formatMapper.WkbReader"

    @classmethod
    def from_str(cls, geo_lib: str) -> 'GeoSparkLib':
        try:
            lib = getattr(cls, geo_lib.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {geo_lib} attribute")
        return lib