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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.geotools.api.referencing.operation.MathTransformFactory;
import org.geotools.api.referencing.operation.OperationMethod;
import org.geotools.api.referencing.operation.Projection;
import org.geotools.referencing.ReferencingFactoryFinder;
import org.geotools.referencing.operation.DefaultMathTransformFactory;

/**
 * Centralized CRS name normalization for bridging GeoTools ↔ proj4sedona naming differences.
 *
 * <p>GeoTools and proj4sedona use different canonical names for some map projections (e.g. GeoTools
 * uses "Mercator_2SP" while proj4sedona uses "Mercator"). This class normalizes names once at the
 * import/export boundaries so downstream code operates on compatible names.
 *
 * <p>All lookups use pre-computed maps; normalization is O(1) after class initialization.
 */
final class CrsNormalization {

  private CrsNormalization() {}

  // Shared regex for extracting projection names from WKT1 strings
  static final Pattern PROJECTION_PATTERN = Pattern.compile("PROJECTION\\[\"([^\"]+)\"\\]");

  // =====================================================================
  // Import direction: proj4sedona → GeoTools
  // =====================================================================

  // Hardcoded fallback for proj4sedona names with no GeoTools alias.
  // Keys are pre-normalized (lowercase, no spaces/underscores) for O(1) lookup.
  // Verified via exhaustive testing of all 58 proj4sedona registered projection names.
  private static final Map<String, String> PROJ4SEDONA_TO_GEOTOOLS;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("lambertcylindricalequalarea", "Cylindrical_Equal_Area");
    m.put("extendedtransversemercator", "Transverse_Mercator");
    m.put("lamberttangentialconformalconicprojection", "Lambert_Conformal_Conic");
    m.put("mercatorvarianta", "Mercator_1SP");
    m.put("polarstereographicvarianta", "Polar_Stereographic");
    m.put("polarstereographicvariantb", "Polar_Stereographic");
    m.put("universaltransversemercatorsystem", "Transverse_Mercator");
    m.put("universaltransversemercator", "Transverse_Mercator");
    PROJ4SEDONA_TO_GEOTOOLS = m;
  }

  // =====================================================================
  // Export direction: GeoTools → proj4sedona
  // =====================================================================

  // GeoTools canonical names that proj4sedona does not recognize.
  private static final Map<String, String> GEOTOOLS_TO_PROJ4SEDONA;

  static {
    Map<String, String> m = new HashMap<>();
    m.put("Mercator_2SP", "Mercator");
    GEOTOOLS_TO_PROJ4SEDONA = m;
  }

  // =====================================================================
  // GeoTools alias caches (lazy-initialized, thread-safe)
  // =====================================================================

  // aliasCache: exact alias string → canonical OGC name
  // normalizedCache: normalized form → set of canonical names (for disambiguation)
  private static volatile Map<String, String> aliasCache;
  private static volatile Map<String, Set<String>> normalizedCache;

  // =====================================================================
  // Public API
  // =====================================================================

  /**
   * Normalize WKT1 projection names from proj4sedona output for GeoTools consumption. Uses a
   * three-tier resolution strategy:
   *
   * <ol>
   *   <li><b>Exact alias matching</b> — direct lookup against all GeoTools registered aliases (OGC,
   *       EPSG, GeoTIFF, ESRI, PROJ authorities).
   *   <li><b>Normalized matching</b> — case-insensitive, ignoring spaces/underscores. Only used
   *       when the normalized form maps unambiguously to a single canonical name.
   *   <li><b>Hardcoded fallback</b> — pre-normalized lookup for proj4sedona-specific names that
   *       have no equivalent in GeoTools' alias database.
   * </ol>
   *
   * @param wkt1 The WKT1 string from proj4sedona.
   * @return The WKT1 string with the projection name normalized for GeoTools.
   */
  static String normalizeWkt1ForGeoTools(String wkt1) {
    return replaceProjectionName(wkt1, CrsNormalization::resolveForGeoTools);
  }

  /**
   * Normalize WKT1 projection names from GeoTools output for proj4sedona consumption.
   *
   * @param wkt1 The WKT1 string from GeoTools.
   * @return The WKT1 string with the projection name normalized for proj4sedona.
   */
  static String normalizeWkt1ForProj4sedona(String wkt1) {
    return replaceProjectionName(wkt1, GEOTOOLS_TO_PROJ4SEDONA::get);
  }

  // =====================================================================
  // Internal implementation
  // =====================================================================

  /** Functional interface for projection name lookup. */
  @FunctionalInterface
  private interface NameResolver {
    /** Return the replacement name, or null if no mapping exists. */
    String resolve(String projName);
  }

  /**
   * Replace the projection name in a WKT1 string using the given resolver. Shared logic for both
   * import and export directions.
   */
  private static String replaceProjectionName(String wkt1, NameResolver resolver) {
    Matcher m = PROJECTION_PATTERN.matcher(wkt1);
    if (m.find()) {
      String projName = m.group(1);
      String resolved = resolver.resolve(projName);
      if (resolved != null && !resolved.equals(projName)) {
        return wkt1.substring(0, m.start(1)) + resolved + wkt1.substring(m.end(1));
      }
    }
    return wkt1;
  }

  /**
   * Three-tier resolution: proj4sedona projection name → canonical GeoTools name.
   *
   * @return The resolved name, or null if the name is already compatible.
   */
  private static String resolveForGeoTools(String projName) {
    ensureCachesBuilt();

    // Tier 1: Exact alias match from GeoTools
    String resolved = aliasCache.get(projName);
    if (resolved != null) {
      return resolved;
    }

    // Tier 2: Normalized match (handles space/underscore/case differences)
    String normalized = normalizeForMatch(projName);
    Set<String> candidates = normalizedCache.get(normalized);
    if (candidates != null && candidates.size() == 1) {
      String canonical = candidates.iterator().next();
      aliasCache.put(projName, canonical); // cache for next time
      return canonical;
    }

    // Tier 3: Pre-normalized hardcoded fallback (O(1) lookup)
    String fallback = PROJ4SEDONA_TO_GEOTOOLS.get(normalized);
    if (fallback != null) {
      aliasCache.put(projName, fallback); // cache for next time
      return fallback;
    }

    return null; // name is already compatible or unknown
  }

  /**
   * Normalize a string for loose matching: lowercase, remove spaces and underscores.
   *
   * @param name The name to normalize.
   * @return Normalized form (e.g. "Lambert_Conformal_Conic_2SP" → "lambertconformalconic2sp").
   */
  static String normalizeForMatch(String name) {
    return name.toLowerCase(Locale.ROOT).replaceAll("[_ ]", "");
  }

  /**
   * Build GeoTools alias caches from all registered {@link OperationMethod} objects. Thread-safe
   * via double-checked locking. Called at most once.
   */
  private static void ensureCachesBuilt() {
    if (aliasCache != null) {
      return;
    }
    synchronized (CrsNormalization.class) {
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
}
