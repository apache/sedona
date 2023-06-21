/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.common.raster;

import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

public class RasterEditors
{
    public static GridCoverage2D setSrid(GridCoverage2D raster, int srid) throws FactoryException
    {
        CoordinateReferenceSystem crs;
        if (srid == 0) {
            crs = DefaultEngineeringCRS.CARTESIAN_2D;
        } else {
            crs = CRS.decode("EPSG:" + srid);
        }
        ReferencedEnvelope referencedEnvelope = new ReferencedEnvelope(raster.getEnvelope2D(), crs);
        GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
        return gridCoverageFactory.create(raster.getName().toString(), raster.getRenderedImage(), referencedEnvelope);
    }
}
