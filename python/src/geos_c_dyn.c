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

#define _GNU_SOURCE
#include "geos_c_dyn.h"

#if defined(_WIN32) || defined(_WIN64)
#define TARGETING_WINDOWS
#include <tchar.h>
#include <windows.h>
#else
#include <dlfcn.h>
#include <errno.h>
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define GEOS_FP_QUALIFIER
#include "geos_c_dyn_funcs.h"
#undef GEOS_FP_QUALIFIER

#define LOAD_GEOS_FUNCTION(func)                                               \
  if (load_geos_c_symbol(handle, #func, (void **)&dyn_##func, err_msg, len) != \
      0) {                                                                     \
    return -1;                                                                 \
  }

#ifdef TARGETING_WINDOWS
static void win32_get_last_error(char *err_msg, int len) {
  wchar_t info[256];
  unsigned int error_code = GetLastError();
  int info_length = FormatMessageW(
      FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, /* flags */
      NULL,                                      /* message source*/
      error_code,                                /* the message (error) ID */
      MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), /* default language */
      info,                                      /* the buffer */
      sizeof(info) / sizeof(wchar_t),            /* size in wchars */
      NULL);
  int num_bytes = WideCharToMultiByte(CP_UTF8, 0, info, info_length, err_msg,
                                      len, NULL, NULL);
  num_bytes = min(num_bytes, len - 1);
  err_msg[num_bytes] = '\0';
}
#endif

static void *try_load_geos_c_symbol(void *handle, const char *func_name) {
#ifndef TARGETING_WINDOWS
  return dlsym(handle, func_name);
#else
  return GetProcAddress((HMODULE)handle, func_name);
#endif
}

static int load_geos_c_symbol(void *handle, const char *func_name,
                              void **p_func, char *err_msg, int len) {
  void *func = try_load_geos_c_symbol(handle, func_name);
  if (func == NULL) {
#ifndef TARGETING_WINDOWS
    snprintf(err_msg, len, "%s", dlerror());
#else
    win32_get_last_error(err_msg, len);
#endif
    return -1;
  }
  *p_func = func;
  return 0;
}

int load_geos_c_library(const char *path, char *err_msg, int len) {
#ifndef TARGETING_WINDOWS
  void *handle = dlopen(path, RTLD_LOCAL | RTLD_NOW);
  if (handle == NULL) {
    snprintf(err_msg, len, "%s", dlerror());
    return -1;
  }
#else
  int num_chars = MultiByteToWideChar(CP_UTF8, 0, path, -1, NULL, 0);
  wchar_t *wpath = calloc(num_chars, sizeof(wchar_t));
  if (wpath == NULL) {
    snprintf(err_msg, len, "%s", "Cannot allocate memory for wpath");
    return -1;
  }
  MultiByteToWideChar(CP_UTF8, 0, path, -1, wpath, num_chars);
  HMODULE module = LoadLibraryW(wpath);
  free(wpath);
  if (module == NULL) {
    win32_get_last_error(err_msg, len);
    return -1;
  }
  void *handle = module;
#endif
  return load_geos_c_from_handle(handle, err_msg, len);
}

int is_geos_c_loaded() { return (dyn_GEOS_init_r != NULL) ? 1 : 0; }

int load_geos_c_from_handle(void *handle, char *err_msg, int len) {
  LOAD_GEOS_FUNCTION(GEOS_finish_r);
  LOAD_GEOS_FUNCTION(GEOSContext_setErrorHandler_r);
  LOAD_GEOS_FUNCTION(GEOSGeomTypeId_r);
  LOAD_GEOS_FUNCTION(GEOSHasZ_r);
  LOAD_GEOS_FUNCTION(GEOSGetSRID_r);
  LOAD_GEOS_FUNCTION(GEOSSetSRID_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_getCoordSeq_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getDimensions_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getSize_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_create_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_destroy_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getXY_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_getXYZ_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_setXY_r);
  LOAD_GEOS_FUNCTION(GEOSCoordSeq_setXYZ_r);
  LOAD_GEOS_FUNCTION(GEOSGetExteriorRing_r);
  LOAD_GEOS_FUNCTION(GEOSGetNumInteriorRings_r);
  LOAD_GEOS_FUNCTION(GEOSGetInteriorRingN_r);
  LOAD_GEOS_FUNCTION(GEOSGetNumCoordinates_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_getCoordinateDimension_r);
  LOAD_GEOS_FUNCTION(GEOSGetNumGeometries_r);
  LOAD_GEOS_FUNCTION(GEOSGetGeometryN_r);
  LOAD_GEOS_FUNCTION(GEOSisEmpty_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createEmptyPoint_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createPoint_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createPointFromXY_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createEmptyLineString_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createLineString_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createEmptyPolygon_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createPolygon_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createLinearRing_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createCollection_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_createEmptyCollection_r);
  LOAD_GEOS_FUNCTION(GEOSGeom_destroy_r);

  /* These functions are not mandantory, only libgeos (>=3.10.0) bundled with
   * shapely>=1.8.0 has these functions. */
  dyn_GEOSCoordSeq_copyFromBuffer_r =
      try_load_geos_c_symbol(handle, "GEOSCoordSeq_copyFromBuffer_r");
  dyn_GEOSCoordSeq_copyToBuffer_r =
      try_load_geos_c_symbol(handle, "GEOSCoordSeq_copyToBuffer_r");

  /* Deliberately load GEOS_init_r after all other functions, so that we can
   * check if all functions were loaded by checking if GEOS_init_r was
   * loaded. */
  LOAD_GEOS_FUNCTION(GEOS_init_r);
  return 0;
}
