package org.apache.sedona.common.S2Geography;

import com.google.common.geometry.*;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PolygonGeographyTest {
    @Test
    public void testEncodedPolygon() throws IOException {
        S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
        S2Point pt_mid = S2LatLng.fromDegrees(45, 0).toPoint();
        S2Point pt_end = S2LatLng.fromDegrees(0, 0).toPoint();

        // Prepare encoder output
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // Build a single polygon and wrap in geography
        List<S2Point> points = new ArrayList<>();
        points.add(pt);
        points.add(pt_mid);
        points.add(pt_end);
        points.add(pt);
        S2Loop polyline = new S2Loop(points);
        S2Polygon poly = new S2Polygon(polyline);
        System.out.println(poly.toString());
        PolygonGeography pg = new PolygonGeography(poly);

        // Encode the geography with tagging
        pg.encodeTagged(baos, new EncodeOptions());

        // Decode from the bytes
        byte[] encodedBytes = baos.toByteArray();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(encodedBytes));
        S2Geography roundtrip = S2Geography.decodeTagged(dis);

        // Verify kind
        assertEquals(S2Geography.GeographyKind.POLYGON, roundtrip.kind);
        System.out.println(roundtrip.toString());
        // Extract polygon and build WKT string
        // Extract decoded polygon
        assertEquals(1, pg.getPolygons().size());

        S2Polygon pl = pg.getPolygons().get(0);
        // Reconstruct WKT from first loop
        S2Loop loop = pl.loop(0);
        StringBuilder sb = new StringBuilder("POLYGON ((");
        for (int i = 0; i < loop.numVertices(); i++) {
            if (i > 0) sb.append(", ");
            S2LatLng ll = new S2LatLng(loop.vertex(i));
            sb.append(String.format("%.0f %.0f", ll.lng().degrees(), ll.lat().degrees()));
        }
        sb.append("))");
        // Build a simple triangle polygon: POLYGON ((-64 45, 0 45, 0 0, -64 45))
        String wkt = "POLYGON ((-64 45, 0 45, 0 0, -64 45))";
        assertEquals(wkt, sb.toString());
        assertTrue(roundtrip instanceof PolygonGeography);
        PolygonGeography rtTyped = (PolygonGeography) roundtrip;
        assertEquals(1, rtTyped.getPolygons().size());
        S2Polygon decodedPolygon = rtTyped.getPolygons().get(0);

        // Compare geometry equality
        assertTrue(decodedPolygon.equals(poly));

    }
}
