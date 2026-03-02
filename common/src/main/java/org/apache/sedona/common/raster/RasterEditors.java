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

import static org.apache.sedona.common.raster.MapAlgebra.addBandFromArray;
import static org.apache.sedona.common.raster.MapAlgebra.bandAsArray;

import java.awt.geom.Point2D;
import java.awt.image.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.media.jai.Interpolation;
import javax.media.jai.RasterFactory;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.utils.RasterInterpolate;
import org.apache.sedona.common.utils.RasterUtils;
import org.datasyslab.proj4sedona.core.Proj;
import org.geotools.api.coverage.grid.GridCoverage;
import org.geotools.api.coverage.grid.GridGeometry;
import org.geotools.api.metadata.spatial.PixelOrientation;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CRSFactory;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.referencing.datum.PixelInCell;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.MathTransform2D;
import org.geotools.api.referencing.operation.MathTransformFactory;
import org.geotools.api.referencing.operation.OperationMethod;
import org.geotools.api.referencing.operation.Projection;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.coverage.CoverageFactoryFinder;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridCoverageFactory;
import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.coverage.processing.Operations;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.DefaultMathTransformFactory;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.index.strtree.STRtree;

public class RasterEditors {

  /**
   * Changes the band pixel type of a specific band of a raster.
   *
   * @param raster The input raster.
   * @param dataType The desired data type of the pixel.
   * @return The modified raster with updated pixel type.
   */
  public static GridCoverage2D setPixelType(GridCoverage2D raster, String dataType) {
    int newDataType = RasterUtils.getDataTypeCode(dataType);

    // Extracting the original data
    RenderedImage originalImage = raster.getRenderedImage();
    Raster originalData = RasterUtils.getRaster(originalImage);

    int width = originalImage.getWidth();
    int height = originalImage.getHeight();
    int numBands = originalImage.getSampleModel().getNumBands();

    // Create a new writable raster with the specified data type
    WritableRaster modifiedRaster =
        RasterFactory.createBandedRaster(newDataType, width, height, numBands, null);

    // Populate modified raster and recreate sample dimensions
    GridSampleDimension[] sampleDimensions = raster.getSampleDimensions();
    for (int band = 0; band < numBands; band++) {
      double[] samples = originalData.getSamples(0, 0, width, height, band, (double[]) null);
      modifiedRaster.setSamples(0, 0, width, height, band, samples);
      if (!Double.isNaN(RasterUtils.getNoDataValue(sampleDimensions[band]))) {
        sampleDimensions[band] =
            RasterUtils.createSampleDimensionWithNoDataValue(
                sampleDimensions[band],
                castRasterDataType(
                    RasterUtils.getNoDataValue(sampleDimensions[band]), newDataType));
      }
    }

    // Clone the original GridCoverage2D with the modified raster
    return RasterUtils.clone(
        modifiedRaster, raster.getGridGeometry(), sampleDimensions, raster, null, true);
  }

  public static GridCoverage2D setSrid(GridCoverage2D raster, int srid) {
    CoordinateReferenceSystem crs;
    if (srid == 0) {
      crs = DefaultEngineeringCRS.GENERIC_2D;
    } else {
      crs = FunctionsGeoTools.sridToCRS(srid);
    }
    return replaceCrs(raster, crs);
  }

  /**
   * Sets the CRS of a raster using a CRS string. Accepts EPSG codes (e.g. "EPSG:4326"), WKT1, WKT2,
   * PROJ strings, and PROJJSON.
   *
   * @param raster The input raster.
   * @param crsString The CRS definition string.
   * @return The raster with the new CRS.
   */
  public static GridCoverage2D setCrs(GridCoverage2D raster, String crsString) {
    CoordinateReferenceSystem crs = parseCrsString(crsString);
    return replaceCrs(raster, crs);
  }

  /**
   * Parse a CRS string in any supported format into a GeoTools CoordinateReferenceSystem.
   *
   * <p>Parsing priority:
   *
   * <ol>
   *   <li>GeoTools CRS.decode — handles authority codes like EPSG:4326
   *   <li>GeoTools CRS.parseWKT — handles WKT1 strings
   *   <li>proj4sedona — handles WKT2, PROJ strings, PROJJSON. If an EPSG authority can be resolved,
   *       uses CRS.decode for a lossless result. Otherwise falls back to WKT1 conversion.
   * </ol>
   *
   * @param crsString The CRS definition string.
   * @return The parsed CoordinateReferenceSystem.
   * @throws IllegalArgumentException if the CRS string cannot be parsed.
   */
  static CoordinateReferenceSystem parseCrsString(String crsString) {
    // Step 1: Try GeoTools CRS.decode (handles EPSG:xxxx, AUTO:xxxx, etc.)
    try {
      return CRS.decode(crsString, true);
    } catch (FactoryException e) {
      // Not an authority code, continue
    }

    // Step 2: Try GeoTools WKT parsing with longitude-first axis order (handles WKT1)
    try {
      Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
      CRSFactory crsFactory = ReferencingFactoryFinder.getCRSFactory(hints);
      return crsFactory.createFromWKT(crsString);
    } catch (FactoryException e) {
      // Not WKT1, continue
    }

    // Step 3: Use proj4sedona (handles WKT2, PROJ, PROJJSON)
    Exception lastError = null;
    // Normalize PROJ string variants that proj4sedona outputs but cannot re-import.
    // Specifically, +proj=sterea (Oblique Stereographic Alternative) maps to +proj=stere
    // since proj4sedona's Stereographic class handles both via lat_0.
    String normalizedInput = crsString;
    if (crsString.contains("+proj=sterea")) {
      normalizedInput = crsString.replace("+proj=sterea", "+proj=stere");
    }
    try {
      Proj proj = new Proj(normalizedInput);

      // Try to resolve to an EPSG authority code for a lossless result
      String authority = proj.toEpsgCode();
      if (authority != null && !authority.isEmpty()) {
        try {
          return CRS.decode(authority, true);
        } catch (FactoryException ex) {
          // Authority code not recognized by GeoTools, fall through to WKT1
        }
      }

      // Fallback: convert to WKT1 via proj4sedona and parse with GeoTools.
      // proj4sedona may include parameters GeoTools doesn't expect (e.g. standard_parallel_1
      // for projections that don't use it). We handle this by trying several parse strategies:
      // 1. Raw WKT1 (proj4sedona's projection names may already be recognized by GeoTools)
      // 2. Normalized WKT1 (resolve projection names to canonical OGC names)
      // 3. Strip unexpected parameters iteratively
      String wkt1 = proj.toWkt1();
      if (wkt1 != null && !wkt1.isEmpty()) {
        Hints hints = new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, Boolean.TRUE);
        CRSFactory crsFactory = ReferencingFactoryFinder.getCRSFactory(hints);

        // Strategy 1: Try raw WKT1 directly
        try {
          return crsFactory.createFromWKT(wkt1);
        } catch (FactoryException ex) {
          // Raw WKT1 failed, continue with normalization
        }

        // Strategy 2: Try with normalized projection name
        String normalizedWkt = normalizeWkt1ProjectionName(wkt1);
        // Strategy 3: If parsing fails due to unexpected parameters, strip them iteratively.
        // proj4sedona sometimes includes parameters like standard_parallel_1 for projections
        // that don't use it. We parse the error message to identify and remove the offending
        // parameter, then retry.
        String currentWkt = normalizedWkt;
        for (int attempt = 0; attempt < 5; attempt++) {
          try {
            return crsFactory.createFromWKT(currentWkt);
          } catch (FactoryException ex) {
            String msg = ex.getMessage();
            if (msg != null) {
              Matcher paramMatcher = UNEXPECTED_PARAM_PATTERN.matcher(msg);
              if (paramMatcher.find()) {
                currentWkt = stripWktParameter(currentWkt, paramMatcher.group(1));
                continue;
              }
            }
            lastError = ex;
            break; // Different kind of error, give up
          }
        }
      }
    } catch (RuntimeException e) {
      lastError = e;
    }

    IllegalArgumentException error =
        new IllegalArgumentException(
            "Cannot parse CRS string. Supported formats: EPSG code (e.g. 'EPSG:4326'), "
                + "WKT1, WKT2, PROJ string, PROJJSON. Input: "
                + crsString);
    if (lastError != null) {
      error.addSuppressed(lastError);
    }
    throw error;
  }

  // Fallback map for proj4sedona projection names that have no equivalent in GeoTools'
  // alias database and cannot be resolved via normalized matching. These are proj4sedona-specific
  // long-form alias names. Verified via exhaustive testing of all 58 proj4sedona registered names.
  private static final Map<String, String> PROJECTION_NAME_FALLBACK;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("Lambert_Cylindrical_Equal_Area", "Cylindrical_Equal_Area");
    m.put("Extended_Transverse_Mercator", "Transverse_Mercator");
    m.put("Extended Transverse Mercator", "Transverse_Mercator");
    m.put("Lambert Tangential Conformal Conic Projection", "Lambert_Conformal_Conic");
    m.put("Mercator_Variant_A", "Mercator_1SP");
    m.put("Polar_Stereographic_variant_A", "Polar_Stereographic");
    m.put("Polar_Stereographic_variant_B", "Polar_Stereographic");
    m.put("Universal Transverse Mercator System", "Transverse_Mercator");
    m.put("Universal_Transverse_Mercator", "Transverse_Mercator");
    PROJECTION_NAME_FALLBACK = Collections.unmodifiableMap(m);
  }

  // Lazy-initialized caches built once from GeoTools' registered OperationMethod objects.
  // aliasCache: exact alias string -> canonical OGC name (ConcurrentHashMap for thread-safe
  // writes from Tier 2 normalized matching)
  // normalizedCache: normalized form (lowercase, no spaces/underscores) -> set of canonical names
  private static volatile Map<String, String> aliasCache;
  private static volatile Map<String, Set<String>> normalizedCache;

  private static final Pattern PROJECTION_PATTERN = Pattern.compile("PROJECTION\\[\"([^\"]+)\"\\]");
  private static final Pattern UNEXPECTED_PARAM_PATTERN =
      Pattern.compile("Parameter \"([^\"]+)\" was not expected");

  /**
   * Strip a named PARAMETER from a WKT1 string. Used to remove parameters that proj4sedona includes
   * but GeoTools does not expect (e.g. standard_parallel_1 for Transverse Mercator).
   *
   * @param wkt The WKT1 string.
   * @param paramName The parameter name to strip (e.g. "standard_parallel_1").
   * @return The WKT1 string with the parameter removed.
   */
  private static String stripWktParameter(String wkt, String paramName) {
    // Remove ,PARAMETER["paramName",value] or PARAMETER["paramName",value],
    String escaped = Pattern.quote(paramName);
    String result = wkt.replaceAll(",\\s*PARAMETER\\[\"" + escaped + "\",[^\\]]*\\]", "");
    if (result.equals(wkt)) {
      result = wkt.replaceAll("PARAMETER\\[\"" + escaped + "\",[^\\]]*\\]\\s*,?", "");
    }
    return result;
  }

  /**
   * Normalize a projection name for loose matching: lowercase, remove spaces and underscores.
   *
   * @param name The projection name to normalize.
   * @return The normalized form (e.g. "Lambert_Conformal_Conic_2SP" → "lambertconformalconic2sp").
   */
  private static String normalizeForMatch(String name) {
    return name.toLowerCase().replaceAll("[_ ]", "");
  }

  /**
   * Resolve a projection name to its canonical OGC WKT1 name. Uses a three-tier strategy:
   *
   * <ol>
   *   <li><b>Exact alias matching</b> — uses all aliases registered in GeoTools' {@link
   *       OperationMethod} objects from OGC, EPSG, GeoTIFF, ESRI, and PROJ authorities. This is a
   *       direct case-sensitive lookup into the alias cache.
   *   <li><b>Normalized matching</b> — strips spaces, underscores, and lowercases both the input
   *       and all known GeoTools projection names/aliases. If this yields exactly one canonical
   *       name, it is used. This handles formatting differences (e.g. spaces vs underscores) that
   *       arise when proj4sedona WKT1 output uses different conventions than GeoTools. Ambiguous
   *       normalized forms (mapping to multiple canonical names) are skipped to avoid incorrect
   *       resolution.
   *   <li><b>Hardcoded fallback</b> — for proj4sedona-specific projection names that have no
   *       equivalent in GeoTools' alias database (e.g. "Extended_Transverse_Mercator",
   *       "Lambert_Cylindrical_Equal_Area").
   * </ol>
   *
   * <p>Verified via exhaustive testing against all 58 proj4sedona registered projection names: 42
   * resolve via exact alias matching, 5 via normalized matching, and 9 via hardcoded fallback. The
   * remaining 2 (longlat, identity) are geographic CRS codes that produce no PROJECTION[] element
   * in WKT1.
   *
   * @param projName The projection name to resolve (e.g. "Lambert Conformal Conic").
   * @return The canonical OGC name (e.g. "Lambert_Conformal_Conic"), or the input unchanged.
   */
  private static String resolveProjectionName(String projName) {
    ensureCachesBuilt();

    // Tier 1: Exact alias match from GeoTools
    String resolved = aliasCache.get(projName);
    if (resolved != null) {
      return resolved;
    }

    // Tier 2: Normalized match (handles space/underscore differences automatically)
    String normalized = normalizeForMatch(projName);
    Set<String> candidates = normalizedCache.get(normalized);
    if (candidates != null && candidates.size() == 1) {
      String canonical = candidates.iterator().next();
      aliasCache.put(projName, canonical);
      return canonical;
    }

    // Tier 3: Hardcoded fallback for proj4sedona-specific names not in GeoTools.
    // Uses normalized matching (lowercase, no spaces/underscores) so both
    // "Lambert_Cylindrical_Equal_Area" and "Lambert Cylindrical Equal Area" match.
    String normalizedProj = normalizeForMatch(projName);
    for (Map.Entry<String, String> entry : PROJECTION_NAME_FALLBACK.entrySet()) {
      if (normalizeForMatch(entry.getKey()).equals(normalizedProj)) {
        String canonical = entry.getValue();
        aliasCache.put(projName, canonical);
        return canonical;
      }
    }
    return projName;
  }

  /**
   * Build caches mapping projection aliases and normalized names to canonical OGC names. Scans all
   * GeoTools {@link OperationMethod} objects registered for {@link Projection}. Thread-safe via
   * double-checked locking.
   */
  private static void ensureCachesBuilt() {
    if (aliasCache != null) {
      return;
    }
    synchronized (RasterEditors.class) {
      if (aliasCache != null) {
        return;
      }
      MathTransformFactory mtf = ReferencingFactoryFinder.getMathTransformFactory(null);
      DefaultMathTransformFactory factory;
      if (mtf instanceof DefaultMathTransformFactory) {
        factory = (DefaultMathTransformFactory) mtf;
      } else {
        factory = new DefaultMathTransformFactory();
      }
      Set<OperationMethod> methods = factory.getAvailableMethods(Projection.class);

      Map<String, String> aliases = new ConcurrentHashMap<>();
      Map<String, Set<String>> normalized = new HashMap<>();

      for (OperationMethod method : methods) {
        String canonical = method.getName().getCode();
        aliases.put(canonical, canonical);
        normalized
            .computeIfAbsent(normalizeForMatch(canonical), k -> new HashSet<>())
            .add(canonical);
        if (method.getAlias() != null) {
          for (Object alias : method.getAlias()) {
            String aliasName = alias.toString().replaceAll("^[^:]+:", "");
            aliases.put(aliasName, canonical);
            normalized
                .computeIfAbsent(normalizeForMatch(aliasName), k -> new HashSet<>())
                .add(canonical);
          }
        }
      }
      aliasCache = aliases;
      normalizedCache = normalized;
    }
  }

  /**
   * Normalize projection names in WKT1 strings generated by proj4sedona to be compatible with
   * GeoTools. Uses GeoTools' alias-aware resolution with a small hardcoded fallback.
   */
  private static String normalizeWkt1ProjectionName(String wkt1) {
    Matcher m = PROJECTION_PATTERN.matcher(wkt1);
    if (m.find()) {
      String projName = m.group(1);
      String resolved = resolveProjectionName(projName);
      if (!resolved.equals(projName)) {
        return wkt1.substring(0, m.start(1)) + resolved + wkt1.substring(m.end(1));
      }
    }
    return wkt1;
  }

  private static GridCoverage2D replaceCrs(GridCoverage2D raster, CoordinateReferenceSystem crs) {
    GridCoverageFactory gridCoverageFactory = CoverageFactoryFinder.getGridCoverageFactory(null);
    MathTransform2D transform = raster.getGridGeometry().getGridToCRS2D();
    Map<?, ?> properties = raster.getProperties();
    GridCoverage[] sources = raster.getSources().toArray(new GridCoverage[0]);
    return gridCoverageFactory.create(
        raster.getName().toString(),
        raster.getRenderedImage(),
        crs,
        transform,
        raster.getSampleDimensions(),
        sources,
        properties);
  }

  public static GridCoverage2D setGeoReference(
      GridCoverage2D raster, String geoRefCoords, String format) {
    String[] coords = geoRefCoords.split(" ");
    if (coords.length != 6) {
      return null;
    }

    double scaleX = Double.parseDouble(coords[0]);
    double skewY = Double.parseDouble(coords[1]);
    double skewX = Double.parseDouble(coords[2]);
    double scaleY = Double.parseDouble(coords[3]);
    double upperLeftX = Double.parseDouble(coords[4]);
    double upperLeftY = Double.parseDouble(coords[5]);
    AffineTransform2D affine;

    if (format.equalsIgnoreCase("GDAL")) {
      affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    } else if (format.equalsIgnoreCase("ESRI")) {
      upperLeftX = upperLeftX - (scaleX * 0.5);
      upperLeftY = upperLeftY - (scaleY * 0.5);
      affine = new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    } else {
      throw new IllegalArgumentException(
          "Please select between the following formats GDAL and ESRI");
    }
    int height = RasterAccessors.getHeight(raster), width = RasterAccessors.getWidth(raster);

    GridGeometry2D gridGeometry2D =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, width, height),
            PixelOrientation.UPPER_LEFT,
            affine,
            raster.getCoordinateReferenceSystem(),
            null);
    return RasterUtils.clone(
        raster.getRenderedImage(),
        gridGeometry2D,
        raster.getSampleDimensions(),
        raster,
        null,
        true);
  }

  public static GridCoverage2D setGeoReference(GridCoverage2D raster, String geoRefCoords) {
    return setGeoReference(raster, geoRefCoords, "GDAL");
  }

  public static GridCoverage2D setGeoReference(
      GridCoverage2D raster,
      double upperLeftX,
      double upperLeftY,
      double scaleX,
      double scaleY,
      double skewX,
      double skewY) {
    String geoRedCoord =
        String.format("%f %f %f %f %f %f", scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
    return setGeoReference(raster, geoRedCoord, "GDAL");
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster,
      double widthOrScale,
      double heightOrScale,
      double gridX,
      double gridY,
      boolean useScale,
      String algorithm)
      throws TransformException {
    /*
     * Old Parameters
     */
    AffineTransform2D affine = RasterUtils.getGDALAffineTransform(raster);
    int originalWidth = RasterAccessors.getWidth(raster),
        originalHeight = RasterAccessors.getHeight(raster);
    double upperLeftX = affine.getTranslateX(), upperLeftY = affine.getTranslateY();
    double originalSkewX = affine.getShearX(), originalSkewY = affine.getShearY();
    double originalScaleX = affine.getScaleX(), originalScaleY = affine.getScaleY();
    CoordinateReferenceSystem crs = raster.getCoordinateReferenceSystem2D();

    /*
     * New Parameters
     */
    int newWidth = useScale ? originalWidth : (int) Math.floor(widthOrScale);
    int newHeight = useScale ? originalHeight : (int) Math.floor(heightOrScale);
    double newScaleX = useScale ? widthOrScale : originalScaleX;
    double newScaleY = useScale ? heightOrScale : originalScaleY;
    double newUpperLeftX = upperLeftX, newUpperLeftY = upperLeftY;

    if (noConfigChange(
        originalWidth,
        originalHeight,
        upperLeftX,
        upperLeftY,
        originalScaleX,
        originalScaleY,
        newWidth,
        newHeight,
        gridX,
        gridY,
        newScaleX,
        newScaleY,
        useScale)) {
      // no reconfiguration parameters provided
      return raster;
    }

    ReferencedEnvelope envelope2D = raster.getEnvelope2D();
    // process scale changes due to changes in widthOrScale and heightOrScale
    if (!useScale) {
      newScaleX = (Math.abs(envelope2D.getMaxX() - envelope2D.getMinX())) / newWidth;
      newScaleY =
          Math.signum(originalScaleY)
              * Math.abs(envelope2D.getMaxY() - envelope2D.getMinY())
              / newHeight;
    } else {
      // height and width cannot have floating point, ceil them to next greatest integer in that
      // case.
      newWidth =
          (int)
              Math.ceil(
                  Math.abs(envelope2D.getMaxX() - envelope2D.getMinX()) / Math.abs(newScaleX));
      newHeight =
          (int)
              Math.ceil(
                  Math.abs(envelope2D.getMaxY() - envelope2D.getMinY()) / Math.abs(newScaleY));
    }

    if (!approximateEquals(upperLeftX, gridX) || !approximateEquals(upperLeftY, gridY)) {
      // change upperLefts to gridX/Y to check if any warping is needed
      GridCoverage2D tempRaster =
          setGeoReference(raster, gridX, gridY, newScaleX, newScaleY, originalSkewX, originalSkewY);

      // check expected grid coordinates for old upperLefts
      int[] expectedCellCoordinates =
          RasterUtils.getGridCoordinatesFromWorld(tempRaster, upperLeftX, upperLeftY);

      // get expected world coordinates at the expected grid coordinates
      Point2D expectedGeoPoint =
          RasterUtils.getWorldCornerCoordinates(
              tempRaster, expectedCellCoordinates[0] + 1, expectedCellCoordinates[1] + 1);

      // check for shift
      if (!approximateEquals(expectedGeoPoint.getX(), upperLeftX)) {
        if (!useScale) {
          newScaleX = Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX()) / newWidth;
        } else {
          // width cannot have floating point, ceil it to next greatest integer in that case.
          newWidth =
              (int)
                  Math.ceil(
                      Math.abs(envelope2D.getMaxX() - expectedGeoPoint.getX())
                          / Math.abs(newScaleX));
        }
        newUpperLeftX = expectedGeoPoint.getX();
      }

      if (!approximateEquals(expectedGeoPoint.getY(), upperLeftY)) {
        if (!useScale) {
          newScaleY =
              Math.signum(newScaleY)
                  * Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY())
                  / newHeight;
        } else {
          // height cannot have floating point, ceil it to next greatest integer in that case.
          newHeight =
              (int)
                  Math.ceil(
                      Math.abs(envelope2D.getMinY() - expectedGeoPoint.getY())
                          / Math.abs(newScaleY));
        }
        newUpperLeftY = expectedGeoPoint.getY();
      }
    }

    MathTransform transform =
        new AffineTransform2D(
            newScaleX, originalSkewY, originalSkewX, newScaleY, newUpperLeftX, newUpperLeftY);
    GridGeometry2D gridGeometry =
        new GridGeometry2D(
            new GridEnvelope2D(0, 0, newWidth, newHeight),
            PixelInCell.CELL_CORNER,
            transform,
            crs,
            null);
    Interpolation resamplingAlgorithm = createInterpolationAlgorithm(algorithm);
    GridCoverage2D newRaster;
    GridCoverage2D noDataValueMask;
    GridCoverage2D resampledNoDataValueMask;

    if ((!Objects.isNull(algorithm) && !algorithm.isEmpty())
        && (algorithm.equalsIgnoreCase("Bilinear") || algorithm.equalsIgnoreCase("Bicubic"))) {
      // Create and resample noDataValue mask
      noDataValueMask = RasterUtils.extractNoDataValueMask(raster);
      resampledNoDataValueMask =
          resample(
              noDataValueMask,
              widthOrScale,
              heightOrScale,
              gridX,
              -gridY,
              useScale,
              "NearestNeighbor");

      // Replace noDataValues with mean of neighbors and resample
      raster = RasterUtils.replaceNoDataValues(raster);
      newRaster =
          (GridCoverage2D)
              Operations.DEFAULT.resample(raster, null, gridGeometry, resamplingAlgorithm);

      // Apply resampled noDataValue mask to resampled raster
      newRaster = RasterUtils.applyRasterMask(newRaster, resampledNoDataValueMask);
    } else {
      newRaster =
          (GridCoverage2D)
              Operations.DEFAULT.resample(raster, null, gridGeometry, resamplingAlgorithm);
    }
    return newRaster;
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster,
      double widthOrScale,
      double heightOrScale,
      boolean useScale,
      String algorithm)
      throws TransformException {
    return resample(
        raster,
        widthOrScale,
        heightOrScale,
        RasterAccessors.getUpperLeftX(raster),
        RasterAccessors.getUpperLeftY(raster),
        useScale,
        algorithm);
  }

  public static GridCoverage2D resample(
      GridCoverage2D raster, GridCoverage2D referenceRaster, boolean useScale, String algorithm)
      throws FactoryException, TransformException {
    int srcSRID = RasterAccessors.srid(raster);
    int destSRID = RasterAccessors.srid(referenceRaster);
    if (srcSRID != destSRID) {
      throw new IllegalArgumentException(
          "Provided input raster and reference raster have different SRIDs");
    }
    double[] refRasterMetadata = RasterAccessors.metadata(referenceRaster);
    int newWidth = (int) refRasterMetadata[2];
    int newHeight = (int) refRasterMetadata[3];
    double gridX = refRasterMetadata[0];
    double gridY = refRasterMetadata[1];
    double newScaleX = refRasterMetadata[4];
    double newScaleY = refRasterMetadata[5];
    if (useScale) {
      return resample(raster, newScaleX, newScaleY, gridX, gridY, useScale, algorithm);
    }
    return resample(raster, newWidth, newHeight, gridX, gridY, useScale, algorithm);
  }

  private static boolean approximateEquals(double a, double b) {
    double tolerance = 1E-6;
    return Math.abs(a - b) <= tolerance;
  }

  private static boolean noConfigChange(
      int oldWidth,
      int oldHeight,
      double oldUpperX,
      double oldUpperY,
      double originalScaleX,
      double originalScaleY,
      int newWidth,
      int newHeight,
      double newUpperX,
      double newUpperY,
      double newScaleX,
      double newScaleY,
      boolean useScale) {
    if (!useScale)
      return ((oldWidth == newWidth)
          && (oldHeight == newHeight)
          && (approximateEquals(oldUpperX, newUpperX))
          && (approximateEquals(oldUpperY, newUpperY)));
    return ((approximateEquals(originalScaleX, newScaleX))
        && (approximateEquals(originalScaleY, newScaleY))
        && (approximateEquals(oldUpperX, newUpperX))
        && (approximateEquals(oldUpperY, newUpperY)));
  }

  public static GridCoverage2D normalizeAll(GridCoverage2D rasterGeom) {
    return normalizeAll(rasterGeom, 0d, 255d, true, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom, double minLim, double maxLim) {
    return normalizeAll(rasterGeom, minLim, maxLim, true, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom, double minLim, double maxLim, boolean normalizeAcrossBands) {
    return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, null, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      boolean normalizeAcrossBands,
      Double noDataValue) {
    return normalizeAll(rasterGeom, minLim, maxLim, normalizeAcrossBands, noDataValue, null, null);
  }

  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      Double noDataValue,
      Double minValue,
      Double maxValue) {
    return normalizeAll(rasterGeom, minLim, maxLim, true, noDataValue, minValue, maxValue);
  }

  /**
   * @param rasterGeom Raster to be normalized
   * @param minLim Lower limit of normalization range
   * @param maxLim Upper limit of normalization range
   * @param normalizeAcrossBands flag to determine the normalization method
   * @param noDataValue NoDataValue used in raster
   * @param minValue Minimum value in raster
   * @param maxValue Maximum value in raster
   * @return a raster with all values in all bands normalized between minLim and maxLim
   */
  public static GridCoverage2D normalizeAll(
      GridCoverage2D rasterGeom,
      double minLim,
      double maxLim,
      boolean normalizeAcrossBands,
      Double noDataValue,
      Double minValue,
      Double maxValue) {
    if (minLim > maxLim) {
      throw new IllegalArgumentException("minLim cannot be greater than maxLim");
    }

    int numBands = rasterGeom.getNumSampleDimensions();
    RenderedImage renderedImage = rasterGeom.getRenderedImage();
    int rasterDataType = renderedImage.getSampleModel().getDataType();

    double globalMin = minValue != null ? minValue : Double.MAX_VALUE;
    double globalMax = maxValue != null ? maxValue : -Double.MAX_VALUE;

    // Initialize arrays to store band-wise min and max values
    double[] minValues = new double[numBands];
    double[] maxValues = new double[numBands];
    Arrays.fill(minValues, Double.MAX_VALUE);
    Arrays.fill(maxValues, -Double.MAX_VALUE);

    // Trigger safe mode if noDataValue is null - noDataValue is set to maxLim and data values are
    // normalized to range [minLim, maxLim-1].
    // This is done to prevent setting valid data as noDataValue.
    double safetyTrigger = (noDataValue == null) ? 1 : 0;

    // Compute global min and max values across all bands if necessary and not provided
    if (minValue == null || maxValue == null) {
      for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
        double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
        double bandNoDataValue =
            RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));

        if (noDataValue == null) {
          noDataValue = maxLim;
        }

        for (double val : bandValues) {
          if (val != bandNoDataValue) {
            if (normalizeAcrossBands) {
              globalMin = Math.min(globalMin, val);
              globalMax = Math.max(globalMax, val);
            } else {
              minValues[bandIndex] = Math.min(minValues[bandIndex], val);
              maxValues[bandIndex] = Math.max(maxValues[bandIndex], val);
            }
          }
        }
      }
    } else {
      globalMin = minValue;
      globalMax = maxValue;
    }

    // Normalize each band
    for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
      double[] bandValues = bandAsArray(rasterGeom, bandIndex + 1);
      double bandNoDataValue = RasterUtils.getNoDataValue(rasterGeom.getSampleDimension(bandIndex));
      double currentMin =
          normalizeAcrossBands ? globalMin : (minValue != null ? minValue : minValues[bandIndex]);
      double currentMax =
          normalizeAcrossBands ? globalMax : (maxValue != null ? maxValue : maxValues[bandIndex]);

      if (Double.compare(currentMax, currentMin) == 0) {
        Arrays.fill(bandValues, minLim);
      } else {
        for (int i = 0; i < bandValues.length; i++) {
          if (bandValues[i] != bandNoDataValue) {
            double normalizedValue =
                minLim
                    + ((bandValues[i] - currentMin) * (maxLim - safetyTrigger - minLim))
                        / (currentMax - currentMin);
            bandValues[i] = castRasterDataType(normalizedValue, rasterDataType);
          } else {
            bandValues[i] = noDataValue;
          }
        }
      }

      // Update the raster with the normalized band and noDataValue
      rasterGeom = addBandFromArray(rasterGeom, bandValues, bandIndex + 1);
      rasterGeom = RasterBandEditors.setBandNoDataValue(rasterGeom, bandIndex + 1, noDataValue);
    }

    return rasterGeom;
  }

  public static GridCoverage2D reprojectMatch(
      GridCoverage2D source, GridCoverage2D target, String interpolationAlgorithm) {
    Interpolation interp = createInterpolationAlgorithm(interpolationAlgorithm);
    CoordinateReferenceSystem crs = target.getCoordinateReferenceSystem();
    GridGeometry gridGeometry = target.getGridGeometry();
    GridCoverage2D result =
        (GridCoverage2D) Operations.DEFAULT.resample(source, crs, gridGeometry, interp);
    return RasterUtils.shiftRasterToZeroOrigin(result, null);
  }

  private static Interpolation createInterpolationAlgorithm(String algorithm) {
    Interpolation interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
    if (!Objects.isNull(algorithm) && !algorithm.isEmpty()) {
      if (algorithm.equalsIgnoreCase("nearestneighbor")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_NEAREST);
      } else if (algorithm.equalsIgnoreCase("bilinear")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
      } else if (algorithm.equalsIgnoreCase("bicubic")) {
        interp = Interpolation.getInstance(Interpolation.INTERP_BICUBIC);
      }
    }
    return interp;
  }

  private static double castRasterDataType(double value, int dataType) {
    switch (dataType) {
      case DataBuffer.TYPE_BYTE:
        // Cast to unsigned byte (0-255)
        double remainder = value % 256;
        double v = (remainder < 0) ? remainder + 256 : remainder;
        return (int) v;
      case DataBuffer.TYPE_SHORT:
        return (short) value;
      case DataBuffer.TYPE_INT:
        return (int) value;
      case DataBuffer.TYPE_USHORT:
        return (char) value;
      case DataBuffer.TYPE_FLOAT:
        return (float) value;
      case DataBuffer.TYPE_DOUBLE:
      default:
        return value;
    }
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster)
      throws IllegalArgumentException {
    return interpolate(inputRaster, 2.0, "fixed", null, null, null);
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster, Double power)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, "fixed", null, null, null);
  }

  public static GridCoverage2D interpolate(GridCoverage2D inputRaster, Double power, String mode)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, null, null, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster, Double power, String mode, Double numPointsOrRadius)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, numPointsOrRadius, null, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster,
      Double power,
      String mode,
      Double numPointsOrRadius,
      Double maxRadiusOrMinPoints)
      throws IllegalArgumentException {
    return interpolate(inputRaster, power, mode, numPointsOrRadius, maxRadiusOrMinPoints, null);
  }

  public static GridCoverage2D interpolate(
      GridCoverage2D inputRaster,
      Double power,
      String mode,
      Double numPointsOrRadius,
      Double maxRadiusOrMinPoints,
      Integer band)
      throws IllegalArgumentException {
    if (!mode.equalsIgnoreCase("variable") && !mode.equalsIgnoreCase("fixed")) {
      throw new IllegalArgumentException(
          "Invalid 'mode': '" + mode + "'. Expected one of: 'Variable', 'Fixed'.");
    }

    Raster rasterData = inputRaster.getRenderedImage().getData();
    WritableRaster raster =
        rasterData.createCompatibleWritableRaster(
            RasterAccessors.getWidth(inputRaster), RasterAccessors.getHeight(inputRaster));
    int width = raster.getWidth();
    int height = raster.getHeight();
    int numBands = raster.getNumBands();
    GridSampleDimension[] gridSampleDimensions = inputRaster.getSampleDimensions();

    if (band != null && (band < 1 || band > numBands)) {
      throw new IllegalArgumentException("Band index out of range.");
    }

    // Interpolation for each band
    for (int bandIndex = 0; bandIndex < numBands; bandIndex++) {
      if (band == null || bandIndex == band - 1) {
        // Generate STRtree
        STRtree strtree = RasterInterpolate.generateSTRtree(inputRaster, bandIndex);
        Double noDataValue = RasterUtils.getNoDataValue(inputRaster.getSampleDimension(bandIndex));
        int countNoDataValues = 0;

        // Skip band if STRtree is empty or has all valid data pixels
        if (strtree.isEmpty() || strtree.size() == width * height) {
          continue;
        }

        if (mode.equalsIgnoreCase("variable") && strtree.size() < numPointsOrRadius) {
          throw new IllegalArgumentException(
              "Parameter 'numPoints' is larger than no. of valid pixels in band "
                  + bandIndex
                  + ". Please choose an appropriate value");
        }

        // Perform interpolation
        for (int y = 0; y < height; y++) {
          for (int x = 0; x < width; x++) {
            double value = rasterData.getSampleDouble(x, y, bandIndex);
            if (Double.isNaN(value) || value == noDataValue) {
              countNoDataValues++;
              double interpolatedValue =
                  RasterInterpolate.interpolateIDW(
                      x,
                      y,
                      strtree,
                      width,
                      height,
                      power,
                      mode,
                      numPointsOrRadius,
                      maxRadiusOrMinPoints);
              interpolatedValue =
                  (Double.isNaN(interpolatedValue)) ? noDataValue : interpolatedValue;
              if (interpolatedValue != noDataValue) {
                countNoDataValues--;
              }
              raster.setSample(x, y, bandIndex, interpolatedValue);
            } else {
              raster.setSample(x, y, bandIndex, value);
            }
          }
        }

        // If all noDataValues are interpolated, update band metadata (remove nodatavalue)
        if (countNoDataValues == 0) {
          gridSampleDimensions[bandIndex] =
              RasterUtils.removeNoDataValue(inputRaster.getSampleDimension(bandIndex));
        }
      } else {
        raster.setSamples(
            0,
            0,
            raster.getWidth(),
            raster.getHeight(),
            band,
            rasterData.getSamples(
                0, 0, raster.getWidth(), raster.getHeight(), band, (double[]) null));
      }
    }

    return RasterUtils.clone(
        raster, inputRaster.getGridGeometry(), gridSampleDimensions, inputRaster, null, true);
  }
}
