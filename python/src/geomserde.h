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

#ifndef GEOM_SERDE
#define GEOM_SERDE

#include "geos_c_dyn.h"

/**
 * Error code for internal APIs of the sedona python extension module.
 */
typedef enum SedonaErrorCode {
  SEDONA_SUCCESS,
  SEDONA_UNKNOWN_GEOM_TYPE,
  SEDONA_UNKNOWN_COORD_TYPE,
  SEDONA_UNSUPPORTED_GEOM_TYPE,
  SEDONA_INCOMPLETE_BUFFER,
  SEDONA_BAD_GEOM_BUFFER,
  SEDONA_GEOS_ERROR,
  SEDONA_ALLOC_ERROR,
  SEDONA_INTERNAL_ERROR,
} SedonaErrorCode;

/**
 * Converts error code to human readable error message
 *
 * @param err error code
 * @return error message
 */
extern const char *sedona_get_error_message(int err);

/**
 * Serializes a GEOS geometry object as a binary buffer
 *
 * @param handle the GEOS context handle
 * @param geom The GEOS geometry object to serialize
 * @param p_buf OUTPUT parameter for receiving the pointer to buffer
 * @param p_buf_size OUTPUT parameter for receiving size of the buffer
 * @return error code
 */
SedonaErrorCode sedona_serialize_geom(GEOSContextHandle_t handle,
                                      const GEOSGeometry *geom, char **p_buf,
                                      int *p_buf_size);

/**
 * Deserializes a serialized geometry to a GEOS geometry object
 *
 * @param handle GEOS context handle
 * @param buf buffer containing serialized geometry
 * @param buf_size size of the buffer
 * @param p_geom OUTPUT parameter for receiving deserialized GEOS geometry
 * @param p_bytes_read OUTPUT parameter for receiving number of bytes read
 * @return error code
 */
SedonaErrorCode sedona_deserialize_geom(GEOSContextHandle_t handle,
                                        const char *buf, int buf_size,
                                        GEOSGeometry **p_geom,
                                        int *p_bytes_read);

#endif /* GEOM_SERDE */
