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

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdio.h>

#include "geomserde.h"
#include "geos_c_dyn.h"
#include "pygeos/c_api.h"

PyDoc_STRVAR(module_doc, "Geometry serialization/deserialization module.");

#define ERR_MSG_BUF_SIZE 1024

#ifdef __GNUC__
#define thread_local __thread
#elif __STDC_VERSION__ >= 201112L
#define thread_local _Thread_local
#elif defined(_MSC_VER)
#define thread_local __declspec(thread)
#else
#error Cannot define thread_local
#endif

static PyObject *load_libgeos_c(PyObject *self, PyObject *args) {
  PyObject *obj;
  char err_msg[ERR_MSG_BUF_SIZE];
  if (!PyArg_ParseTuple(args, "O", &obj)) {
    return NULL;
  }

  if (PyLong_Check(obj)) {
    unsigned long long handle = PyLong_AsUnsignedLongLong(obj);
    if (load_geos_c_from_handle((void *)handle, err_msg, sizeof(err_msg)) !=
        0) {
      PyErr_Format(PyExc_RuntimeError, "Failed to find libgeos_c functions: %s",
                   err_msg);
      return NULL;
    }
  } else if (PyUnicode_Check(obj)) {
    const char *libname = PyUnicode_AsUTF8(obj);
    if (load_geos_c_library(libname, err_msg, sizeof(err_msg)) != 0) {
      PyErr_Format(PyExc_RuntimeError, "Failed to find libgeos_c functions: %s",
                   err_msg);
      return NULL;
    }
  } else {
    PyErr_SetString(PyExc_TypeError,
                    "load_libgeos_c expects a string or long argument");
    return NULL;
  }

  Py_INCREF(Py_None);
  return Py_None;
}

static thread_local GEOSContextHandle_t handle;
static thread_local char *geos_err_msg;

static void geos_msg_handler(const char *fmt, ...) {
  if (geos_err_msg == NULL) return;
  va_list ap;
  va_start(ap, fmt);
  vprintf(fmt, ap);
  vsnprintf(geos_err_msg, ERR_MSG_BUF_SIZE, fmt, ap);
  va_end(ap);
}

static GEOSContextHandle_t get_geos_context_handle() {
  if (handle == NULL) {
    if (!is_geos_c_loaded()) {
      PyErr_SetString(
          PyExc_RuntimeError,
          "libgeos_c was not loaded, please call load_libgeos_c first");
      return NULL;
    }

    handle = dyn_GEOS_init_r();
    if (handle == NULL) {
      goto oom_failure;
    }
    geos_err_msg = malloc(ERR_MSG_BUF_SIZE);
    if (geos_err_msg == NULL) {
      goto oom_failure;
    }
    dyn_GEOSContext_setErrorHandler_r(handle, geos_msg_handler);
  }

  geos_err_msg[0] = '\0';
  return handle;

oom_failure:
  PyErr_NoMemory();
  if (handle != NULL) {
    dyn_GEOS_finish_r(handle);
    handle = NULL;
  }
  if (geos_err_msg != NULL) {
    free(geos_err_msg);
    geos_err_msg = NULL;
  }
  return NULL;
}

static void handle_geomserde_error(SedonaErrorCode err) {
  const char *errmsg = sedona_get_error_message(err);
  if (err == SEDONA_ALLOC_ERROR) {
    PyErr_NoMemory();
  } else if (err == SEDONA_INTERNAL_ERROR) {
    PyErr_Format(PyExc_RuntimeError, "%s", errmsg);
  } else if (err == SEDONA_GEOS_ERROR) {
    const char *errmsg = sedona_get_error_message(err);
    PyErr_Format(PyExc_RuntimeError, "%s: %s", errmsg, geos_err_msg);
  } else {
    const char *errmsg = sedona_get_error_message(err);
    PyErr_Format(PyExc_ValueError, "%s", errmsg);
  }
}

static PyObject *do_serialize(GEOSGeometry *geos_geom) {
  if (geos_geom == NULL) {
    Py_INCREF(Py_None);
    return Py_None;
  }

  GEOSContextHandle_t handle = get_geos_context_handle();
  if (handle == NULL) {
    return NULL;
  }

  char *buf = NULL;
  int buf_size = 0;
  SedonaErrorCode err =
      sedona_serialize_geom(handle, geos_geom, &buf, &buf_size);
  if (err != SEDONA_SUCCESS) {
    handle_geomserde_error(err);
    return NULL;
  }

  PyObject *bytearray = PyByteArray_FromStringAndSize(buf, buf_size);
  free(buf);
  return bytearray;
}

static GEOSGeometry *do_deserialize(PyObject *args,
                                    GEOSContextHandle_t *out_handle,
                                    int *p_bytes_read) {
  Py_buffer view;
  if (!PyArg_ParseTuple(args, "y*", &view)) {
    return NULL;
  }

  GEOSContextHandle_t handle = get_geos_context_handle();
  if (handle == NULL) {
    return NULL;
  }

  /* The Py_buffer filled by PyArg_ParseTuple is guaranteed to be C-contiguous,
   * so we can simply proceed with view.buf and view.len */
  const char *buf = view.buf;
  int buf_size = view.len;
  GEOSGeometry *geom = NULL;
  SedonaErrorCode err =
      sedona_deserialize_geom(handle, buf, buf_size, &geom, p_bytes_read);
  PyBuffer_Release(&view);
  if (err != SEDONA_SUCCESS) {
    handle_geomserde_error(err);
    return NULL;
  }

  *out_handle = handle;
  return geom;
}

/* serialize/deserialize functions for Shapely 2.x */

static PyObject *serialize(PyObject *self, PyObject *args) {
  PyObject *pygeos_geom = NULL;
  if (!PyArg_ParseTuple(args, "O", &pygeos_geom)) {
    return NULL;
  }

  GEOSGeometry *geos_geom = NULL;
  char success = PyGEOS_GetGEOSGeometry(pygeos_geom, &geos_geom);
  if (success == 0) {
    PyErr_SetString(
        PyExc_TypeError,
        "Argument is of incorrect type. Please provide only Geometry objects.");
    return NULL;
  }

  return do_serialize(geos_geom);
}

static PyObject *deserialize(PyObject *self, PyObject *args) {
  GEOSContextHandle_t handle = NULL;
  int length = 0;
  GEOSGeometry *geom = do_deserialize(args, &handle, &length);
  if (geom == NULL) {
    return NULL;
  }
  PyObject *pygeom = PyGEOS_CreateGeometry(geom, handle);
  return Py_BuildValue("(Ni)", pygeom, length);
}

/* serialize/deserialize functions for Shapely 1.x */

static PyObject *serialize_1(PyObject *self, PyObject *args) {
  GEOSGeometry *geos_geom = NULL;
  if (!PyArg_ParseTuple(args, "K", &geos_geom)) {
    return NULL;
  }
  return do_serialize(geos_geom);
}

static PyObject *deserialize_1(PyObject *self, PyObject *args) {
  GEOSContextHandle_t handle = NULL;
  int length = 0;
  GEOSGeometry *geom = do_deserialize(args, &handle, &length);
  if (geom == NULL) {
    return NULL;
  }

  /* These functions would be called by Shapely using ctypes when constructing
   * a Shapely BaseGeometry object from GEOSGeometry pointer. We call them here
   * to get rid of the extra overhead introduced by ctypes. */
  int geom_type_id = dyn_GEOSGeomTypeId_r(handle, geom);
  char has_z = dyn_GEOSHasZ_r(handle, geom);
  return Py_BuildValue("(Kibi)", geom, geom_type_id, has_z, length);
}

/* Module definition for Shapely 2.x */

static PyMethodDef geomserde_methods_shapely_2[] = {
    {"load_libgeos_c", load_libgeos_c, METH_VARARGS, "Load libgeos_c."},
    {"serialize", serialize, METH_VARARGS,
     "Serialize geometry object as bytearray."},
    {"deserialize", deserialize, METH_VARARGS,
     "Deserialize bytes-like object to geometry object."},
    {NULL, NULL, 0, NULL}, /* Sentinel */
};

static struct PyModuleDef geomserde_module_shapely_2 = {
    PyModuleDef_HEAD_INIT, "geomserde_speedup", module_doc, 0,
    geomserde_methods_shapely_2};

/* Module definition for Shapely 1.x */

static PyMethodDef geomserde_methods_shapely_1[] = {
    {"load_libgeos_c", load_libgeos_c, METH_VARARGS, "Load libgeos_c."},
    {"serialize_1", serialize_1, METH_VARARGS,
     "Serialize geometry object as bytearray."},
    {"deserialize_1", deserialize_1, METH_VARARGS,
     "Deserialize bytes-like object to geometry object."},
    {NULL, NULL, 0, NULL}, /* Sentinel */
};

static struct PyModuleDef geomserde_module_shapely_1 = {
    PyModuleDef_HEAD_INIT, "geomserde_speedup", module_doc, 0,
    geomserde_methods_shapely_1};

PyMODINIT_FUNC PyInit_geomserde_speedup(void) {
  if (import_shapely_c_api() != 0) {
    /* As long as the capsule provided by Shapely 2.0 cannot be loaded, we
     * assume that we're working with Shapely 1.0 */
    PyErr_Clear();
    return PyModuleDef_Init(&geomserde_module_shapely_1);
  }

  return PyModuleDef_Init(&geomserde_module_shapely_2);
}
