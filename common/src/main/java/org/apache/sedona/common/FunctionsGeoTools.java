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
package org.apache.sedona.common;

import org.apache.sedona.common.utils.GeomUtils;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;




public class FunctionsGeoTools {
    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS)
        throws FactoryException, TransformException {
        return transform(geometry, sourceCRS, targetCRS, false);
    }

    public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS, boolean lenient)
        throws FactoryException, TransformException {
        CoordinateReferenceSystem sourceCRSCode = parseCRSString(sourceCRS);
        CoordinateReferenceSystem targetCRScode = parseCRSString(targetCRS);
        return GeomUtils.transform(geometry, sourceCRSCode, targetCRScode, lenient);
    }

    private static CoordinateReferenceSystem parseCRSString(String CRSString)
            throws FactoryException
    {
        try {
            return CRS.decode(CRSString);
        }
        catch (NoSuchAuthorityCodeException e) {
            try {
                return CRS.parseWKT(CRSString);
            }
            catch (FactoryException ex) {
                throw new FactoryException("First failed to read as a well-known CRS code: \n" + e.getMessage() + "\nThen failed to read as a WKT CRS string: \n" + ex.getMessage());
            }
        }
    }
}
