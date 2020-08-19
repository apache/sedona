package org.datasyslab.geospark.jts;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.geojson.GeoJsonConstants;
import com.vividsolutions.jts.io.geojson.GeoJsonWriter;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;

public class GeoJsonFeatureWriter extends GeoJsonWriter {
    final static String NAME_GEOMETRY = "geometry";
    final static String NAME_FEATURE = "Feature";
    final static String NAME_USER_DATA = "UserData";

    private boolean parseUserData = true;

    public GeoJsonFeatureWriter() {
        super();
    }

    public GeoJsonFeatureWriter(boolean parseUserData) {
        this();
        this.parseUserData = parseUserData;
    }

    @Override
    public void write(Geometry geometry, Writer writer) throws IOException {
        Map<String, Object> properties = new LinkedHashMap<>();
        if(parseUserData) {
            properties.put(NAME_USER_DATA, geometry.getUserData());
        }

        Map<String, Object> map = new LinkedHashMap<String, Object>() {{
            put(GeoJsonConstants.NAME_TYPE, NAME_FEATURE);
            put(NAME_GEOMETRY, new JSONAware() {
                public String toJSONString() {
                    return write(geometry);
                }
            });
            put(GeoJsonConstants.NAME_PROPERTIES, properties);
        }};

        JSONObject.writeJSONString(map, writer);
        writer.flush();
    }
}
