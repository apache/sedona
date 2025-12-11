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

/* The following are function pointers to GEOS C APIs provided by
 * libgeos_c. These functions must be called after a successful invocation of
 * `load_geos_c_library` or `load_geos_c_from_handle` */

GEOS_FP_QUALIFIER GEOSContextHandle_t (*dyn_GEOS_init_r)();

GEOS_FP_QUALIFIER void (*dyn_GEOS_finish_r)(GEOSContextHandle_t handle);

GEOS_FP_QUALIFIER GEOSMessageHandler (*dyn_GEOSContext_setErrorHandler_r)(
    GEOSContextHandle_t extHandle, GEOSMessageHandler ef);

GEOS_FP_QUALIFIER int (*dyn_GEOSGeomTypeId_r)(GEOSContextHandle_t handle,
                                              const GEOSGeometry *g);

GEOS_FP_QUALIFIER char (*dyn_GEOSHasZ_r)(GEOSContextHandle_t handle,
                                         const GEOSGeometry *g);

GEOS_FP_QUALIFIER int (*dyn_GEOSGetSRID_r)(GEOSContextHandle_t handle,
                                           const GEOSGeometry *g);

GEOS_FP_QUALIFIER void (*dyn_GEOSSetSRID_r)(GEOSContextHandle_t handle,
                                            GEOSGeometry *g, int SRID);

GEOS_FP_QUALIFIER const GEOSCoordSequence *(*dyn_GEOSGeom_getCoordSeq_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_getDimensions_r)(
    GEOSContextHandle_t handle, const GEOSCoordSequence *s, unsigned int *dims);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_getSize_r)(GEOSContextHandle_t handle,
                                                    const GEOSCoordSequence *s,
                                                    unsigned int *size);

GEOS_FP_QUALIFIER GEOSCoordSequence *(*dyn_GEOSCoordSeq_copyFromBuffer_r)(
    GEOSContextHandle_t handle, const double *buf, unsigned int size, int hasZ,
    int hasM);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_copyToBuffer_r)(
    GEOSContextHandle_t handle, const GEOSCoordSequence *s, double *buf,
    int hasZ, int hasM);

GEOS_FP_QUALIFIER GEOSCoordSequence *(*dyn_GEOSCoordSeq_create_r)(
    GEOSContextHandle_t handle, unsigned int size, unsigned int dims);

GEOS_FP_QUALIFIER void (*dyn_GEOSCoordSeq_destroy_r)(GEOSContextHandle_t handle,
                                                     GEOSCoordSequence *s);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_getXY_r)(GEOSContextHandle_t handle,
                                                  const GEOSCoordSequence *s,
                                                  unsigned int idx, double *x,
                                                  double *y);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_getXYZ_r)(GEOSContextHandle_t handle,
                                                   const GEOSCoordSequence *s,
                                                   unsigned int idx, double *x,
                                                   double *y, double *z);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_setXY_r)(GEOSContextHandle_t handle,
                                                  GEOSCoordSequence *s,
                                                  unsigned int idx, double x,
                                                  double y);

GEOS_FP_QUALIFIER int (*dyn_GEOSCoordSeq_setXYZ_r)(GEOSContextHandle_t handle,
                                                   GEOSCoordSequence *s,
                                                   unsigned int idx, double x,
                                                   double y, double z);

GEOS_FP_QUALIFIER const GEOSGeometry *(*dyn_GEOSGetExteriorRing_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

GEOS_FP_QUALIFIER int (*dyn_GEOSGetNumInteriorRings_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

GEOS_FP_QUALIFIER int (*dyn_GEOSGetNumCoordinates_r)(GEOSContextHandle_t handle,
                                                     const GEOSGeometry *g);

GEOS_FP_QUALIFIER int (*dyn_GEOSGeom_getCoordinateDimension_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g);

GEOS_FP_QUALIFIER const GEOSGeometry *(*dyn_GEOSGetInteriorRingN_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g, int n);

GEOS_FP_QUALIFIER int (*dyn_GEOSGetNumGeometries_r)(GEOSContextHandle_t handle,
                                                    const GEOSGeometry *g);

GEOS_FP_QUALIFIER const GEOSGeometry *(*dyn_GEOSGetGeometryN_r)(
    GEOSContextHandle_t handle, const GEOSGeometry *g, int n);

GEOS_FP_QUALIFIER char (*dyn_GEOSisEmpty_r)(GEOSContextHandle_t handle,
                                            const GEOSGeometry *g);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createEmptyPoint_r)(
    GEOSContextHandle_t handle);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createPoint_r)(
    GEOSContextHandle_t handle, GEOSCoordSequence *s);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createPointFromXY_r)(
    GEOSContextHandle_t handle, double x, double y);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createEmptyLineString_r)(
    GEOSContextHandle_t handle);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createLineString_r)(
    GEOSContextHandle_t handle, GEOSCoordSequence *s);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createEmptyPolygon_r)(
    GEOSContextHandle_t handle);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createPolygon_r)(
    GEOSContextHandle_t handle, GEOSGeometry *shell, GEOSGeometry **holes,
    unsigned int nholes);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createLinearRing_r)(
    GEOSContextHandle_t handle, GEOSCoordSequence *s);

GEOS_FP_QUALIFIER void (*dyn_GEOSGeom_destroy_r)(GEOSContextHandle_t handle,
                                                 GEOSGeometry *g);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createCollection_r)(
    GEOSContextHandle_t handle, int type, GEOSGeometry **geoms,
    unsigned int ngeoms);

GEOS_FP_QUALIFIER GEOSGeometry *(*dyn_GEOSGeom_createEmptyCollection_r)(
    GEOSContextHandle_t handle, int type);
