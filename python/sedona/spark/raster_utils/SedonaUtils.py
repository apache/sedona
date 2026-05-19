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

from sedona.spark.maps.SedonaMapUtils import SedonaMapUtils


class SedonaUtils:
    @classmethod
    def display_image(cls, df):
        """Display raster images in a Jupyter notebook.

        Accepts DataFrames with either:
        - A raster column (GridCoverage2D) — auto-applies RS_AsImage
        - An HTML image column from RS_AsImage() — renders directly

        Falls back to the SedonaMapUtils HTML table path for other DataFrames.
        """
        from IPython.display import HTML, display

        schema = df.schema

        # Detect raster UDT columns and auto-apply RS_AsImage.
        # Without this, passing a raw raster DataFrame to the fallback path
        # causes __convert_to_gdf_or_pdf__ to Arrow-serialize the full raster
        # grid, which hangs for large rasters (e.g., 1400x800).
        raster_cols = [
            f.name
            for f in schema.fields
            if hasattr(f.dataType, "typeName") and f.dataType.typeName() == "rastertype"
        ]
        if raster_cols:
            # Replace each raster column with its RS_AsImage() HTML representation,
            # preserving all other columns in the DataFrame.
            select_exprs = []
            for f in schema.fields:
                col_name_escaped = f.name.replace("`", "``")
                if f.name in raster_cols:
                    select_exprs.append(
                        f"RS_AsImage(`{col_name_escaped}`) as `{col_name_escaped}`"
                    )
                else:
                    select_exprs.append(f"`{col_name_escaped}`")
            df = df.selectExpr(*select_exprs)

        pdf = SedonaMapUtils.__convert_to_gdf_or_pdf__(df, rename=False)
        display(HTML(pdf.to_html(escape=False)))
