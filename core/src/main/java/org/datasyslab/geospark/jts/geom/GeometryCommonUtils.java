package org.datasyslab.geospark.jts.geom;

import java.util.Objects;
import org.locationtech.jts.geom.Geometry;

public class GeometryCommonUtils {
    public static boolean userDataEquals(Geometry a, Object b) {
        return Objects.equals(a.getUserData(), ((Geometry) b).getUserData());
    }

    public static String makeString(String geometry, Object userData) {
        String result = geometry;
        if (userData != null && !userData.equals(""))
            result += "\t" + userData;

        return result;
    }

    public static void initUserDataFrom(Geometry geometry, Geometry from) {
        Object userData = from.getUserData();
        if (userData == null) {
            userData = "";
        }
        geometry.setUserData(userData);
    }
}
