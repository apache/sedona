package org.datasyslab.geospark.geometryObjects;

import org.geotools.geometry.jts.JTS;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.io.WKTWriter;

public class ProjectTrans {
    private static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
    private static WKTReader reader = new WKTReader(geometryFactory);
    private static WKTWriter write = new WKTWriter();
    static ProjectTrans proj = new ProjectTrans();

    final String strWKTMercator = "PROJCS[\"world_Mercator\","
            + "GEOGCS[\"GCS_WGS_1984\","
            + "DATUM[\"WGS_1984\","
            + "SPHEROID[\"WGS_1984\",6378137,298.257223563]],"
            + "PRIMEM[\"Greenwich\",0],"
            + "UNIT[\"Degree\",0.017453292519943295]],"
            + "PROJECTION[\"Mercator_1SP\"],"
            + "PARAMETER[\"False_Easting\",0],"
            + "PARAMETER[\"False_Northing\",0],"
            + "PARAMETER[\"Central_Meridian\",0],"
            + "PARAMETER[\"latitude_of_origin\",0],"
            + "UNIT[\"Meter\",1]]";

    public Geometry lonlat2WebMactor(Geometry geom) {
        try {
            CoordinateReferenceSystem crsTarget = CRS.decode("EPSG:3857");

            MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84, crsTarget);
            return JTS.transform(geom, transform);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        String wktPoint = "POINT(100.02715479879 33.462715497945)";
        String wktLine = "LINESTRING(108.32803893589 41.306670233001,99.950999898452 25.84722546391)";
        String wktPolygon = "POLYGON((100.02715479879 32.168082192159,102.76873121104 37.194305614622,107.0334056301 34.909658604412,105.96723702534 30.949603786713,100.02715479879 32.168082192159))";

        try {
            long start = System.currentTimeMillis();
            Geometry geom1 = reader.read(wktPoint);
            Geometry geom1T = proj.lonlat2WebMactor(geom1);

            Geometry geom2 = reader.read(wktLine);
            Geometry geom2T = proj.lonlat2WebMactor(geom2);

            Geometry geom3 = reader.read(wktPolygon);
            Geometry geom3T = proj.lonlat2WebMactor(geom3);
            System.out.println(write.write(geom1T));
            System.out.println(write.write(geom2T));
            System.out.println(write.write(geom3T));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}
