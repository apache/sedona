# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC, abstractmethod
from typing import List, Optional
import json
from xml.etree.ElementTree import Element, SubElement, tostring  # nosec B405

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
from .data_buffer import DataBuffer
from .meta import AffineTransform, SampleDimension
from .sample_model import ComponentSampleModel, SampleModel

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


def _numpy_dtype_to_data_buffer_type(dtype: np.dtype) -> int:
    """Map a numpy dtype to the corresponding DataBuffer type constant.

    Parameters
    ----------
    dtype : np.dtype
        The numpy dtype to map.

    Returns
    -------
    int
        One of DataBuffer.TYPE_* constants.

    Raises
    ------
    ValueError
        If the dtype has no corresponding DataBuffer type.
    """
    dtype = np.dtype(dtype)  # normalize
    mapping = {
        np.dtype(np.uint8): DataBuffer.TYPE_BYTE,
        np.dtype(np.int8): DataBuffer.TYPE_BYTE,
        np.dtype(np.int16): DataBuffer.TYPE_SHORT,
        np.dtype(np.uint16): DataBuffer.TYPE_USHORT,
        np.dtype(np.int32): DataBuffer.TYPE_INT,
        np.dtype(np.uint32): DataBuffer.TYPE_INT,
        np.dtype(np.float32): DataBuffer.TYPE_FLOAT,
        np.dtype(np.float64): DataBuffer.TYPE_DOUBLE,
    }
    if dtype not in mapping:
        raise ValueError(
            f"Unsupported numpy dtype {dtype} for raster serialization. "
            f"Supported: uint8, int8, int16, uint16, int32, uint32, float32, float64. "
            f"Note: uint32 maps to signed TYPE_INT — values above 2^31-1 will "
            f"overflow silently. int8 maps to TYPE_BYTE (unsigned on JVM) — "
            f"negative values will be reinterpreted."
        )
    return mapping[dtype]


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

    def with_bands(self, new_data: np.ndarray) -> "SedonaRaster":
        """Replace pixel data, preserving spatial metadata.

        Only supported on InDbSedonaRaster.
        """
        raise TypeError(f"with_bands() is not supported on {type(self).__name__}.")

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

        # Cached opaque blobs for round-trip serialization.
        # These are set by raster_serde._deserialize() or with_bands().
        # If None, the raster cannot be serialized.
        self._name: str = ""
        self._category_blobs: Optional[List[bytes]] = None
        self._properties_blob: Optional[bytes] = None
        self._color_model_blob: Optional[bytes] = None

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

    def with_bands(self, new_data: np.ndarray) -> "InDbSedonaRaster":
        """Create a new InDbSedonaRaster with replaced pixel data but same spatial metadata.

        The spatial metadata (CRS, affine transform, name) and cached opaque blobs
        (colorModel, properties) are preserved from the source raster. The category
        blobs and band metadata are adjusted to match the new band count.

        The colorModel blob is replayed unchanged even if band count or dtype changes.
        The JVM tolerates this: all analytical operations use the SampleModel (which
        is rebuilt correctly), and self-healing mechanisms in RasterEditors, MapAlgebra,
        and RasterUtils rebuild the colorModel when needed for rendering operations.

        The returned raster uses BandedSampleModel (BSQ layout) regardless of the
        source raster's SampleModel type. The JVM accepts any valid SampleModel
        during deserialization.

        Parameters
        ----------
        new_data : np.ndarray
            New pixel data. Accepted shapes:
              - (height, width) — interpreted as single-band CHW with C=1
              - (bands, height, width) — CHW layout
            Height and width must match the source raster.
            Band count and dtype may differ from source.

        Returns
        -------
        InDbSedonaRaster
            A new raster with the given pixel data and adjusted metadata.

        Raises
        ------
        ValueError
            If spatial dimensions don't match.
        RuntimeError
            If the source raster has no cached blobs (cannot be serialized).
        """
        if (
            self._category_blobs is None
            or self._properties_blob is None
            or self._color_model_blob is None
        ):
            raise RuntimeError(
                "Cannot call with_bands() on a raster without cached blob fields. "
                "Only rasters created by raster_serde.deserialize() support "
                "with_bands()."
            )

        if new_data.ndim == 2:
            new_data = new_data[np.newaxis, :, :]  # HW → CHW

        if new_data.ndim != 3:
            raise ValueError(
                f"new_data must be 2D (H, W) or 3D (C, H, W), got {new_data.ndim}D"
            )

        n_bands, h, w = new_data.shape
        if h != self._height or w != self._width:
            raise ValueError(
                f"Spatial dimensions ({h}, {w}) don't match raster "
                f"({self._height}, {self._width})"
            )

        # Map numpy dtype → DataBuffer type
        data_type = _numpy_dtype_to_data_buffer_type(new_data.dtype)

        # Adjust category blobs for new band count
        source_n_bands = len(self._bands_meta)
        if n_bands <= source_n_bands:
            category_blobs = list(self._category_blobs[:n_bands])
        else:
            category_blobs = list(self._category_blobs)
            # Replicate last source category blob for new bands
            last_blob = self._category_blobs[-1]
            for _ in range(n_bands - source_n_bands):
                category_blobs.append(last_blob)

        # Adjust band metadata for new band count
        if n_bands <= source_n_bands:
            bands_meta = list(self._bands_meta[:n_bands])
        else:
            bands_meta = list(self._bands_meta)
            for _ in range(n_bands - source_n_bands):
                bands_meta.append(
                    SampleDimension(
                        description="",
                        offset=0.0,
                        scale=1.0,
                        nodata=float("nan"),
                    )
                )

        # Build BandedSampleModel (TYPE_BANDED = 1)
        # ComponentSampleModel.__init__() sets TYPE_COMPONENT, so we must
        # override to TYPE_BANDED after construction.
        new_sample_model = ComponentSampleModel(
            data_type,
            w,
            h,
            1,  # pixel_stride
            w,  # scanline_stride
            list(range(n_bands)),  # bank_indices: [0, 1, ..., n-1]
            [0] * n_bands,  # band_offsets: [0, 0, ..., 0]
        )
        new_sample_model.sample_model_type = SampleModel.TYPE_BANDED

        # Build DataBuffer: one bank per band, each flattened row-major
        banks = [np.ascontiguousarray(new_data[i].flatten()) for i in range(n_bands)]
        new_data_buffer = DataBuffer(data_type, banks, w * h, [0] * n_bands)

        new_awt_raster = AWTRaster(0, 0, w, h, new_sample_model, new_data_buffer)

        result = InDbSedonaRaster(
            self._width,
            self._height,
            bands_meta,
            self._affine_trans,
            self._crs_wkt,
            new_awt_raster,
        )
        result._name = self._name
        result._category_blobs = category_blobs
        result._properties_blob = self._properties_blob
        result._color_model_blob = self._color_model_blob  # replay unchanged
        return result

    def close(self):
        if self.rasterio_dataset_reader is not None:
            self.rasterio_dataset_reader.close()
            self.rasterio_dataset_reader = None
        if self.rasterio_memfile is not None:
            self.rasterio_memfile.close()
            self.rasterio_memfile = None
