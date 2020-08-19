package org.datasyslab.geospark.jts;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.geojson.GeoJsonConstants;
import com.vividsolutions.jts.io.geojson.GeoJsonWriter;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

public class GeoJsonFeatureWriter extends GeoJsonWriter {
    final static String NAME_GEOMETRY = "geometry";
    final static String NAME_FEATURE = "Feature";
    final static String NAME_USER_DATA = "UserData";

    private boolean feature = true;

    public GeoJsonFeatureWriter() {
        super();
        setEncodeCRS(false);
    }

    public GeoJsonFeatureWriter(boolean feature) {
        this();
        this.feature = feature;
    }

    private void writeFeature(Geometry geometry, Writer writer) throws IOException {
        // Get original geometry string, containing the geometry part of the GeoJSON Feature
        StringWriter geometryWriter = new StringWriter();
        super.write(geometry, geometryWriter);

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put(GeoJsonConstants.NAME_TYPE, NAME_FEATURE);
            put(NAME_GEOMETRY, new JSONAware() {
                public String toJSONString() {
                    return geometryWriter.toString();
                }
            });
            put(GeoJsonConstants.NAME_PROPERTIES, new LinkedHashMap<String, Object>() {{
                put(NAME_USER_DATA, geometry.getUserData());
            }});
        }};

        JSONObject.writeJSONString(map, writer);
    }

    @Override
    public void write(Geometry geometry, Writer writer) throws IOException {
        if (feature) {
            writeFeature(geometry, writer);
        } else {
            super.write(geometry, writer);
        }

        writer.flush();
    }
}
