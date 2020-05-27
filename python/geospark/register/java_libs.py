from enum import Enum


class GeoSparkLib(Enum):
    JoinParams = "org.datasyslab.geospark.python.adapters.JoinParamsAdapter"
    Adapter = "org.datasyslab.geosparksql.utils.Adapter"
    JoinQuery = "org.datasyslab.geospark.spatialOperator.JoinQuery"
    KNNQuery = "org.datasyslab.geospark.spatialOperator.KNNQuery"
    RangeQuery = "org.datasyslab.geospark.spatialOperator.RangeQuery"
    Envelope = "com.vividsolutions.jts.geom.Envelope"
    GeoSerializerData = "org.datasyslab.geospark.python.adapters.GeoSparkPythonConverter"
    GeometryAdapter = "org.datasyslab.geospark.python.adapters.GeometryAdapter"
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
    AdapterWrapper = "org.datasyslab.geosparksql.utils.PythonAdapterWrapper"
    WktReader = "org.datasyslab.geospark.formatMapper.WktReader"
    RawJvmIndexRDDSetter = "org.datasyslab.geospark.python.adapters.RawJvmIndexRDDSetter"
    SpatialObjectLoaderAdapter = "org.datasyslab.geospark.python.adapters.SpatialObjectLoaderAdapter"
    WkbReader = "org.datasyslab.geospark.formatMapper.WkbReader"
    EnvelopeAdapter = "org.datasyslab.geospark.python.adapters.EnvelopeAdapter"
    GeoSparkPythonConverter = "org.datasyslab.geospark.python.adapters.GeoSparkPythonConverter"
    PythonRddToJavaRDDAdapter = "org.datasyslab.geospark.python.adapters.PythonRddToJavaRDDAdapter"

    @classmethod
    def from_str(cls, geo_lib: str) -> 'GeoSparkLib':
        try:
            lib = getattr(cls, geo_lib.upper())
        except AttributeError:
            raise AttributeError(f"{cls.__class__.__name__} has no {geo_lib} attribute")
        return lib