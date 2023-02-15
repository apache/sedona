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

#ifndef GEOS_C_DYN
#define GEOS_C_DYN

/* We don't need to depend on geos_c.h in libgeos directly. We can add forward
 * type declarations for them since the libgeos C API only deals with pointer
 * types of them, so they are essentially opaque to us. */
struct GEOSGeom_t;
struct GEOSPrepGeom_t;
struct GEOSCoordSeq_t;
struct GEOSContextHandle;

typedef struct GEOSGeom_t GEOSGeometry;
typedef struct GEOSPrepGeom_t GEOSPreparedGeometry;
typedef struct GEOSContextHandle *GEOSContextHandle_t;
typedef struct GEOSCoordSeq_t GEOSCoordSequence;

typedef void (*GEOSMessageHandler)(const char *fmt, ...);

/**
 * Check if GEOS C was loaded

 * @return 1 if GEOS C was loaded, otherwise return 0.
 */
int is_geos_c_loaded();

/**
 * Load GEOS C functions from libgeos_c library on specified path.
 *
 * We don't link to libgeos_c directly because shapely already brought its own
 * copy of libgeos_c, it is better to use the same copy of libgeos_c in our
 * extension since our extension is an augmentation of shapely. Mixing various
 * versions of the same library together is likely to cause nasty bugs.
 *
 * @param path path to the libgeos_c library
 * @param err_msg buffer for receiving error message in case of errors
 * @param len length of the error message buffer
 * @return 0 when GEOS functions were loaded correctly, otherwise returns a
 * non-zero value
 */
int load_geos_c_library(const char *path, char *err_msg, int len);

/**
 * Load GEOS C functions from specified (platform-specific) library handle
 *
 * This function is similar to `load_geos_c_library`. The only exception is
 * that it does not load the libgeos_c library from file.
 *
 * @param handle platform-specific handle to load functions from
 * @param err_msg buffer for receiving error message in case of errors
 * @param len length of the error message buffer
 * @return 0 when GEOS functions were loaded correctly, otherwise returns a
 * non-zero value
 */
int load_geos_c_from_handle(void *handle, char *err_msg, int len);

#define GEOS_FP_QUALIFIER extern
#include "geos_c_dyn_funcs.h"
#undef GEOS_FP_QUALIFIER

/* Supported geometry types */
enum GEOSGeomTypes {
  GEOS_POINT,
  GEOS_LINESTRING,
  GEOS_LINEARRING,
  GEOS_POLYGON,
  GEOS_MULTIPOINT,
  GEOS_MULTILINESTRING,
  GEOS_MULTIPOLYGON,
  GEOS_GEOMETRYCOLLECTION
};

#endif /* GEOS_C_DYN */
