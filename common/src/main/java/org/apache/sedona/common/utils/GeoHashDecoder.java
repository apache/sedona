/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.utils;

import org.locationtech.jts.geom.Geometry;

public class GeoHashDecoder {
    private static final int[] bits = new int[] {16, 8, 4, 2, 1};
    private static final String base32 = "0123456789bcdefghjkmnpqrstuvwxyz";

    public static class InvalidGeoHashException extends Exception {
        public InvalidGeoHashException(String message) {
            super(message);
        }
    }

    public static Geometry decode(String geohash, Integer precision) throws InvalidGeoHashException {
        return decodeGeoHashBBox(geohash, precision).getBbox().toPolygon();
    }

    private static class LatLon {
        public Double[] lons;

        public Double[] lats;

        public LatLon(Double[] lons, Double[] lats) {
            this.lons = lons;
            this.lats = lats;
        }

        BBox getBbox() {
            return new BBox(
                    lons[0],
                    lons[1],
                    lats[0],
                    lats[1]
            );
        }
    }

    private static LatLon decodeGeoHashBBox(String geohash, Integer precision) throws InvalidGeoHashException {
        LatLon latLon = new LatLon(new Double[] {-180.0, 180.0}, new Double[] {-90.0, 90.0});
        String geoHashLowered = geohash.toLowerCase();
        int geoHashLength = geohash.length();
        int targetPrecision = geoHashLength;
        if (precision != null) {
            if (precision < 0) throw new InvalidGeoHashException("Precision can not be negative");
            else targetPrecision = Math.min(geoHashLength, precision);
        }
        boolean isEven = true;

        for (int i = 0; i < targetPrecision ; i++){
            char c = geoHashLowered.charAt(i);
            byte cd = (byte) base32.indexOf(c);
            if (cd == -1){
                throw new InvalidGeoHashException(String.format("Invalid character '%s' found at index %d", c, i));
            }
            for (int j = 0;j < 5; j++){
                byte mask = (byte) bits[j];
                int index = (mask & cd) == 0 ? 1 : 0;
                if (isEven){
                    latLon.lons[index] = (latLon.lons[0] + latLon.lons[1]) / 2;
                }
                else {
                    latLon.lats[index] = (latLon.lats[0] + latLon.lats[1]) / 2;
                }
                isEven = !isEven;
            }
        }
        return latLon;
    }

}
