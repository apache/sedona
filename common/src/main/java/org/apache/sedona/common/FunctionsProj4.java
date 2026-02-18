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
package org.apache.sedona.common;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.datasyslab.proj4sedona.core.Proj;
import org.datasyslab.proj4sedona.defs.CRSResult;
import org.datasyslab.proj4sedona.defs.Defs;
import org.datasyslab.proj4sedona.defs.UrlCRSProvider;
import org.datasyslab.proj4sedona.jts.JTSGeometryTransformer;
import org.datasyslab.proj4sedona.parser.CRSSerializer;
import org.locationtech.jts.geom.Geometry;

/**
 * CRS transformation functions using proj4sedona library.
 *
 * <p>This class provides coordinate reference system (CRS) transformation for vector geometries
 * using the proj4sedona library, which is a pure Java implementation without LGPL dependencies.
 *
 * <h2>Supported CRS Input Formats</h2>
 *
 * <ul>
 *   <li><b>EPSG code</b>: "EPSG:4326", "EPSG:3857"
 *   <li><b>WKT1</b>: OGC Well-Known Text format (PROJCS[...] or GEOGCS[...])
 *   <li><b>WKT2</b>: ISO 19162:2019 format (PROJCRS[...] or GEOGCRS[...])
 *   <li><b>PROJ string</b>: "+proj=longlat +datum=WGS84 +no_defs"
 *   <li><b>PROJJSON</b>: JSON representation of CRS ({"type": "GeographicCRS", ...})
 * </ul>
 *
 * <h2>NAD Grid Support</h2>
 *
 * <p>NAD grid files can be specified in PROJ strings using the +nadgrids parameter:
 *
 * <ul>
 *   <li><b>Local file</b>: "+nadgrids=/path/to/grid.gsb"
 *   <li><b>PROJ CDN</b>: "+nadgrids=@us_noaa_conus.tif" (@ prefix means optional)
 *   <li><b>HTTPS URL</b>: "+nadgrids=https://cdn.proj.org/us_noaa_conus.tif"
 * </ul>
 *
 * <p>Grid files with @ prefix are optional - if not found, the transformation continues without the
 * grid. Without @ prefix, the grid is mandatory and an error is thrown if not found.
 *
 * @since 1.9.0
 */
public class FunctionsProj4 {

  /** Pattern to match EPSG codes (e.g., "EPSG:4326", "epsg:2154") */
  private static final Pattern EPSG_PATTERN =
      Pattern.compile("^EPSG:(\\d+)$", Pattern.CASE_INSENSITIVE);

  /** Name used for the registered URL CRS provider. */
  private static final String URL_CRS_PROVIDER_NAME = "sedona-url-crs";

  /**
   * Tracks the currently registered URL CRS provider config (baseUrl + "|" + pathTemplate + "|" +
   * format). Null means no provider registered yet. Uses AtomicReference for thread-safe lazy
   * initialization on executors.
   */
  private static final AtomicReference<String> registeredUrlCrsConfig = new AtomicReference<>(null);

  /**
   * Reset the URL CRS provider state. Package-private for testing only. Removes the provider from
   * Defs and clears the cached config key.
   */
  static void resetUrlCrsProviderForTest() {
    Defs.removeProvider(URL_CRS_PROVIDER_NAME);
    registeredUrlCrsConfig.set(null);
  }

  /**
   * Register a URL-based CRS provider with proj4sedona's Defs registry. This provider will be
   * consulted before the built-in provider when resolving EPSG codes.
   *
   * <p>This method is safe to call concurrently from multiple threads — it uses double-checked
   * locking so the fast path (already registered with the same config) is lock-free, and the
   * synchronized slow path executes at most once per JVM (or once per config change).
   *
   * @param baseUrl The base URL of the CRS definition server
   * @param pathTemplate The URL path template (e.g., "/{authority}/{code}.json")
   * @param format The expected response format: "projjson", "proj", "wkt1", or "wkt2"
   */
  public static void registerUrlCrsProvider(String baseUrl, String pathTemplate, String format) {
    if (baseUrl == null || baseUrl.isEmpty()) {
      return;
    }

    // Canonicalize format to avoid unnecessary re-registration for equivalent configs
    String canonicalFormat = parseCrsFormat(format).name().toLowerCase(Locale.ROOT);
    String configKey = baseUrl + "|" + pathTemplate + "|" + canonicalFormat;

    // Fast path (lock-free): already registered with the same config.
    // This handles 99.999%+ of calls with just a volatile read + String.equals().
    if (configKey.equals(registeredUrlCrsConfig.get())) {
      return;
    }

    // Slow path: synchronize to make the remove-register-set sequence atomic.
    // Only the first thread per JVM (or per config change) enters this block.
    synchronized (registeredUrlCrsConfig) {
      // Re-check after acquiring lock — another thread may have registered already
      String current = registeredUrlCrsConfig.get();
      if (configKey.equals(current)) {
        return;
      }

      // Remove existing provider if config changed
      if (current != null) {
        Defs.removeProvider(URL_CRS_PROVIDER_NAME);
      }

      CRSResult.Format crsFormat = parseCrsFormat(format);

      UrlCRSProvider provider =
          UrlCRSProvider.builder(URL_CRS_PROVIDER_NAME)
              .baseUrl(baseUrl)
              .pathTemplate(pathTemplate)
              .format(crsFormat)
              .build();

      // Priority 50: before built-in (100) and spatialreference.org (101)
      Defs.registerProvider(provider, 50);
      registeredUrlCrsConfig.set(configKey);
    }
  }

  /**
   * Parse the CRS format string from config to the CRSResult.Format enum.
   *
   * @param format Format string: "projjson", "proj", "wkt1", or "wkt2"
   * @return The corresponding CRSResult.Format
   */
  private static CRSResult.Format parseCrsFormat(String format) {
    if (format == null || format.isEmpty()) {
      return CRSResult.Format.PROJJSON;
    }
    switch (format.toLowerCase(Locale.ROOT)) {
      case "proj":
        return CRSResult.Format.PROJ4;
      case "wkt1":
        return CRSResult.Format.WKT1;
      case "wkt2":
        return CRSResult.Format.WKT2;
      case "projjson":
      default:
        return CRSResult.Format.PROJJSON;
    }
  }

  /**
   * Transform a geometry from the source CRS specified by the geometry's SRID to the target CRS.
   *
   * @param geometry The geometry to transform
   * @param targetCRS Target CRS definition (EPSG code, WKT, PROJ string, or PROJJSON)
   * @return The transformed geometry with SRID set to the target CRS EPSG code (if identifiable)
   * @throws IllegalArgumentException if source CRS cannot be determined from geometry SRID
   */
  public static Geometry transform(Geometry geometry, String targetCRS) {
    return transform(geometry, null, targetCRS);
  }

  /**
   * Transform a geometry from one CRS to another with lenient parameter.
   *
   * <p><b>Note:</b> The {@code lenient} parameter is accepted for API compatibility with GeoTools
   * but is ignored. proj4sedona always performs strict transformation.
   *
   * @param geometry The geometry to transform
   * @param sourceCRS Source CRS definition (EPSG code, WKT, PROJ string, or PROJJSON), or null to
   *     use geometry's SRID
   * @param targetCRS Target CRS definition (EPSG code, WKT, PROJ string, or PROJJSON)
   * @param lenient Ignored parameter, kept for API compatibility
   * @return The transformed geometry with SRID set to the target CRS EPSG code (if identifiable)
   * @throws IllegalArgumentException if source CRS is null and geometry has no SRID
   */
  public static Geometry transform(
      Geometry geometry, String sourceCRS, String targetCRS, boolean lenient) {
    // lenient parameter is ignored - proj4sedona doesn't support it
    return transform(geometry, sourceCRS, targetCRS);
  }

  /**
   * Transform a geometry from one CRS to another.
   *
   * @param geometry The geometry to transform
   * @param sourceCRS Source CRS definition (EPSG code, WKT, PROJ string, or PROJJSON), or null to
   *     use geometry's SRID
   * @param targetCRS Target CRS definition (EPSG code, WKT, PROJ string, or PROJJSON)
   * @return The transformed geometry with SRID set to the target CRS EPSG code (if identifiable)
   * @throws IllegalArgumentException if source CRS is null and geometry has no SRID
   */
  public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS) {
    if (geometry == null) {
      return null;
    }

    // Determine source CRS
    String effectiveSourceCRS = sourceCRS;
    if (effectiveSourceCRS == null || effectiveSourceCRS.isEmpty()) {
      int srid = geometry.getSRID();
      if (srid != 0) {
        effectiveSourceCRS = "EPSG:" + srid;
      } else {
        throw new IllegalArgumentException(
            "Source CRS must be specified. No SRID found on geometry.");
      }
    }

    // Check if source and target are the same EPSG code
    Integer sourceSRID = extractEpsgCode(effectiveSourceCRS);
    Integer targetSRID = extractEpsgCode(targetCRS);

    if (sourceSRID != null && sourceSRID.equals(targetSRID)) {
      // Same CRS, just update SRID if needed
      if (geometry.getSRID() != targetSRID) {
        Geometry result = geometry.copy();
        result = Functions.setSRID(result, targetSRID);
        result.setUserData(geometry.getUserData());
        return result;
      }
      return geometry;
    }

    // Perform transformation using cached projections to avoid per-row overhead.
    // JTSGeometryTransformer.cached() uses Proj4.cachedConverter() internally,
    // which caches parsed Proj objects for repeated transformations.
    JTSGeometryTransformer transformer =
        JTSGeometryTransformer.cached(effectiveSourceCRS, targetCRS);
    Geometry transformed = transformer.transform(geometry);

    // Set SRID on result
    if (targetSRID == null) {
      try {
        Proj targetProj = new Proj(targetCRS);
        String epsgCode = CRSSerializer.toEpsgCode(targetProj);
        if (epsgCode != null) {
          Integer epsg = extractEpsgCode(epsgCode);
          if (epsg != null) {
            targetSRID = epsg;
          }
        }
      } catch (Exception e) {
        // Could not identify EPSG code, leave SRID as 0
      }
    }

    if (targetSRID == null) {
      targetSRID = 0;
    }
    transformed = Functions.setSRID(transformed, targetSRID);

    // Preserve user data
    transformed.setUserData(geometry.getUserData());

    return transformed;
  }

  /**
   * Extract EPSG code from a CRS string if it's in EPSG format.
   *
   * @param crs CRS string (may be EPSG code, WKT, PROJ string, etc.)
   * @return EPSG code as integer, or null if not an EPSG code format
   */
  private static Integer extractEpsgCode(String crs) {
    if (crs == null || crs.isEmpty()) {
      return null;
    }

    Matcher matcher = EPSG_PATTERN.matcher(crs.trim());
    if (matcher.matches()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }
}
