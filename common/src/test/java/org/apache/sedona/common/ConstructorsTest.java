package org.apache.sedona.common;

import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

import static org.junit.Assert.*;

public class ConstructorsTest {

    @Test
    public void geomFromWKT() throws ParseException {
        assertNull(Constructors.geomFromWKT(null, 0));

        Geometry geom = Constructors.geomFromWKT("POINT (1 1)", 0);
        assertEquals(0, geom.getSRID());
        assertEquals("POINT (1 1)", geom.toText());

        geom = Constructors.geomFromWKT("POINT (1 1)", 3006);
        assertEquals(3006, geom.getSRID());
        assertEquals("POINT (1 1)", geom.toText());

        ParseException invalid = assertThrows(ParseException.class, () -> Constructors.geomFromWKT("not valid", 0));
        assertEquals("Unknown geometry type: NOT (line 1)", invalid.getMessage());
    }
}