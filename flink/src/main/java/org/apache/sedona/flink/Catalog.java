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
package org.apache.sedona.flink;

import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.sedona.flink.expressions.*;

public class Catalog {
    public static UserDefinedFunction[] getFuncs() {
        return new UserDefinedFunction[]{
                new Aggregators.ST_Envelope_Aggr(),
                new Aggregators.ST_Union_Aggr(),
                new Constructors.ST_Point(),
                new Constructors.ST_PointFromText(),
                new Constructors.ST_LineStringFromText(),
                new Constructors.ST_LineFromText(),
                new Constructors.ST_PolygonFromText(),
                new Constructors.ST_PolygonFromEnvelope(),
                new Constructors.ST_GeomFromWKT(),
                new Constructors.ST_GeomFromText(),
                new Constructors.ST_GeomFromWKB(),
                new Constructors.ST_GeomFromGeoJSON(),
                new Constructors.ST_GeomFromGeoHash(),
                new Constructors.ST_GeomFromGML(),
                new Constructors.ST_GeomFromKML(),
                new Constructors.ST_MPolyFromText(),
                new Constructors.ST_MLineFromText(),
                new Functions.ST_Area(),
                new Functions.ST_Azimuth(),
                new Functions.ST_Boundary(),
                new Functions.ST_Buffer(),
                new Functions.ST_ConcaveHull(),
                new Functions.ST_Envelope(),
                new Functions.ST_Distance(),
                new Functions.ST_3DDistance(),
                new Functions.ST_Length(),
                new Functions.ST_Transform(),
                new Functions.ST_FlipCoordinates(),
                new Functions.ST_GeoHash(),
                new Functions.ST_PointOnSurface(),
                new Functions.ST_Reverse(),
                new Functions.ST_GeometryN(),
                new Functions.ST_InteriorRingN(),
                new Functions.ST_PointN(),
                new Functions.ST_NPoints(),
                new Functions.ST_NumGeometries(),
                new Functions.ST_NumInteriorRings(),
                new Functions.ST_ExteriorRing(),
                new Functions.ST_AsEWKT(),
                new Functions.ST_AsEWKB(),
                new Functions.ST_AsText(),
                new Functions.ST_AsBinary(),
                new Functions.ST_AsGeoJSON(),
                new Functions.ST_AsGML(),
                new Functions.ST_AsKML(),
                new Functions.ST_Force_2D(),
                new Functions.ST_IsEmpty(),
                new Functions.ST_X(),
                new Functions.ST_Y(),
                new Functions.ST_Z(),
                new Functions.ST_YMax(),
                new Functions.ST_YMin(),
                new Functions.ST_XMax(),
                new Functions.ST_XMin(),
                new Functions.ST_ZMax(),
                new Functions.ST_ZMin(),
                new Functions.ST_NDims(),
                new Functions.ST_BuildArea(),
                new Functions.ST_SetSRID(),
                new Functions.ST_SRID(),
                new Functions.ST_IsClosed(),
                new Functions.ST_IsRing(),
                new Functions.ST_IsSimple(),
                new Functions.ST_IsValid(),
                new Functions.ST_Normalize(),
                new Functions.ST_AddPoint(),
                new Functions.ST_RemovePoint(),
                new Functions.ST_SetPoint(),
                new Functions.ST_LineFromMultiPoint(),
                new Functions.ST_Split(),
                new Functions.ST_S2CellIDs()
        };
    }

    public static UserDefinedFunction[] getPredicates() {
        return new UserDefinedFunction[]{
                new Predicates.ST_Intersects(),
                new Predicates.ST_Contains(),
                new Predicates.ST_Within(),
                new Predicates.ST_Covers(),
                new Predicates.ST_CoveredBy(),
                new Predicates.ST_Disjoint(),
                new Predicates.ST_OrderingEquals(),
        };
    }
}
