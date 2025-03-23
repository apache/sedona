#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from abc import ABC, abstractmethod
from typing import List, Optional
import json
from xml.etree.ElementTree import Element, SubElement, tostring

import numpy as np
import rasterio  # type: ignore
import rasterio.env  # type: ignore
from rasterio.io import DatasetReader  # type: ignore
from rasterio.io import MemoryFile  # type: ignore
from rasterio.transform import Affine  # type: ignore

try:
    # for rasterio >= 1.3.0
    from rasterio._path import _parse_path as parse_path  # type: ignore
except:
    # for rasterio >= 1.2.0
    from rasterio.path import parse_path  # type: ignore

from .awt_raster import AWTRaster
from .meta import AffineTransform, SampleDimension


GDAL_VERSION = rasterio.env.GDALVersion.runtime()


def _has_env_with_gdal_mem_enabled():
    if rasterio.env.hasenv():
        if GDAL_VERSION.at_least(rasterio.env.GDALVersion(3, 10)):
            # For GDAL >= 3.10, GDAL_MEM_ENABLE_OPEN must be enabled to load
            # MEM:: dataset. Please refer to
            # https://gdal.org/en/latest/drivers/raster/mem.html for details.
            options = rasterio.env.getenv()
            return options.get("GDAL_MEM_ENABLE_OPEN") == "YES"
        else:
            return True
    else:
        return False


def _rasterio_open(fp, driver=None):
    """A variant of rasterio.open. This function skip setting up a new GDAL env
    when there is already an environment. This saves us lots of overhead
    introduced by GDAL env initialization.

    """
    if _has_env_with_gdal_mem_enabled():
        # There is already an env, so we can get rid of the overhead of
        # GDAL env initialization in rasterio.open().
        return DatasetReader(parse_path(fp), driver=driver)
    else:
        with rasterio.env.Env(GDAL_MEM_ENABLE_OPEN="YES"):
            return rasterio.open(fp, mode="r", driver=driver)


def _rasterio_open_memfile(memfile: MemoryFile, driver=None):
    if not _has_env_with_gdal_mem_enabled():
        with rasterio.env.Env(GDAL_MEM_ENABLE_OPEN="YES"):
            return memfile.open(driver=driver)
    return memfile.open(driver=driver)


def _generate_vrt_xml(
    src_path,
    data_type,
    width,
    height,
    geo_transform,
    crs_wkt,
    off_x,
    off_y,
    band_indices,
) -> bytes:
    # Create root element
    root = Element("VRTDataset")
    root.set("rasterXSize", str(width))
    root.set("rasterYSize", str(height))

    # Add CRS
    if crs_wkt is not None and crs_wkt != "":
        srs = SubElement(root, "SRS")
        srs.text = crs_wkt

    # Add GeoTransform
    gt = SubElement(root, "GeoTransform")
    gt.text = geo_transform

    # Add bands
    for i, band_index in enumerate(band_indices, start=1):
        band = SubElement(root, "VRTRasterBand")
        band.set("dataType", data_type)
        band.set("band", str(i))

        # Add source
        source = SubElement(band, "SimpleSource")
        src_prop = SubElement(source, "SourceFilename")
        src_prop.text = src_path

        # Set source properties
        SubElement(source, "SourceBand").text = str(band_index + 1)
        SubElement(
            source,
            "SrcRect",
            {
                "xOff": str(off_x),
                "yOff": str(off_y),
                "xSize": str(width),
                "ySize": str(height),
            },
        )
        SubElement(
            source,
            "DstRect",
            {"xOff": "0", "yOff": "0", "xSize": str(width), "ySize": str(height)},
        )

    # Generate pretty XML
    xml_bytes = tostring(root, encoding="utf-8")
    return xml_bytes


class SedonaRaster(ABC):
    _width: int
    _height: int
    _bands_meta: List[SampleDimension]
    _affine_trans: AffineTransform
    _crs_wkt: str

    def __init__(
        self,
        width: int,
        height: int,
        bands_meta: List[SampleDimension],
        affine_trans: AffineTransform,
        crs_wkt: str,
    ):
        self._width = width
        self._height = height
        self._bands_meta = bands_meta
        self._affine_trans = affine_trans
        self._crs_wkt = crs_wkt

    @property
    def width(self) -> int:
        """Width of the raster in pixel"""
        return self._width

    @property
    def height(self) -> int:
        """Height of the raster in pixel"""
        return self._height

    @property
    def crs_wkt(self) -> str:
        """CRS of the raster as a WKT string"""
        return self._crs_wkt

    @property
    def bands_meta(self) -> List[SampleDimension]:
        """Metadata of bands, including nodata value for each band"""
        return self._bands_meta

    @property
    def affine_trans(self) -> AffineTransform:
        """Geo transform of the raster"""
        return self._affine_trans

    @abstractmethod
    def as_numpy(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout"""
        raise NotImplementedError()

    def as_numpy_masked(self) -> np.ndarray:
        """Get the bands data as an numpy array in CHW layout, with nodata
        values masked as nan.

        """
        arr = self.as_numpy()
        nodata_values = np.array([bm.nodata for bm in self._bands_meta])
        nodata_values_reshaped = nodata_values[:, None, None]
        mask = arr == nodata_values_reshaped
        masked_arr = np.where(mask, np.nan, arr)
        return masked_arr

    @abstractmethod
    def as_rasterio(self) -> DatasetReader:
        """Retrieve the raster as an rasterio DatasetReader"""
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Release all resources allocated for this sedona raster. The rasterio
        DatasetReader returned by as_rasterio() will also be closed.

        """
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


class InDbSedonaRaster(SedonaRaster):
    awt_raster: AWTRaster
    rasterio_memfile: Optional[MemoryFile]
    rasterio_dataset_reader: Optional[DatasetReader]

    def __init__(
        self,
        width: int,
        height: int,
        bands_meta: List[SampleDimension],
        affine_trans: AffineTransform,
        crs_wkt: str,
        awt_raster: AWTRaster,
    ):
        super().__init__(width, height, bands_meta, affine_trans, crs_wkt)
        self.awt_raster = awt_raster
        self.rasterio_memfile = None
        self.rasterio_dataset_reader = None

    def as_numpy(self) -> np.ndarray:
        sm = self.awt_raster.sample_model
        return sm.as_numpy(self.awt_raster.data_buffer)

    def as_rasterio(self) -> DatasetReader:
        if self.rasterio_dataset_reader is not None:
            return self.rasterio_dataset_reader

        affine = Affine.from_gdal(
            self._affine_trans.ip_x,
            self._affine_trans.scale_x,
            self._affine_trans.skew_x,
            self._affine_trans.ip_y,
            self._affine_trans.skew_y,
            self._affine_trans.scale_y,
        )
        num_bands = len(self._bands_meta)

        data_array = np.ascontiguousarray(self.as_numpy())

        dtype = data_array.dtype
        if dtype == np.uint8:
            data_type = "Byte"
        elif dtype == np.int8:
            data_type = "Int8"
        elif dtype == np.uint16:
            data_type = "Uint16"
        elif dtype == np.int16:
            data_type = "Int16"
        elif dtype == np.uint32:
            data_type = "UInt32"
        elif dtype == np.int32:
            data_type = "Int32"
        elif dtype == np.float32:
            data_type = "Float32"
        elif dtype == np.float64:
            data_type = "Float64"
        elif dtype == np.int64:
            data_type = "Int64"
        elif dtype == np.uint64:
            data_type = "Uint64"
        else:
            raise RuntimeError("unknown dtype: " + str(dtype))

        arr_if = data_array.__array_interface__
        data_pointer = arr_if["data"][0]
        geotransform = (
            f"{self._affine_trans.ip_x}/{self._affine_trans.scale_x}/{self._affine_trans.skew_x}/"
            + f"{self._affine_trans.ip_y}/{self._affine_trans.skew_y}/{self._affine_trans.scale_y}"
        )
        desc = (
            f"MEM:::DATAPOINTER={data_pointer},PIXELS={self._width},LINES={self._height},BANDS={num_bands},"
            + f"DATATYPE={data_type},GEOTRANSFORM={geotransform}"
        )

        # If we are using GDAL >= 3.7, we can use the SPATIALREFERENCE
        # parameter; otherwise we have to wrap the MEM dataset with an VRT to
        # set up the SRS.
        if GDAL_VERSION.at_least(rasterio.env.GDALVersion(3, 7)):
            escaped_srs = json.dumps(self._crs_wkt.replace("\n", ""))
            desc += f",SPATIALREFERENCE={escaped_srs}"
            dataset = _rasterio_open(desc, driver="MEM")
        else:
            # construct a VRT to wrap this MEM dataset, with SRS set up properly
            vrt_xml = _generate_vrt_xml(
                desc,
                data_type,
                self._width,
                self._height,
                geotransform.replace("/", ","),
                self._crs_wkt,
                0,
                0,
                list(range(num_bands)),
            )
            self.rasterio_memfile = MemoryFile(vrt_xml, ext=".vrt")
            dataset = _rasterio_open_memfile(self.rasterio_memfile, driver="VRT")

        # XXX: dataset does not copy the data held by data_array, so we set
        # data_array as a property of dataset to make sure that the lifetime of
        # data_array is as long as dataset, otherwise we may see band data
        # corruption.
        dataset.mem_data_array = data_array
        return dataset

    def close(self):
        if self.rasterio_dataset_reader is not None:
            self.rasterio_dataset_reader.close()
            self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None
