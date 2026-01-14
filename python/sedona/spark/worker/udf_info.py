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

from dataclasses import dataclass

from sedona.spark import GeometryType


@dataclass
class UDFInfo:
    arg_offsets: list
    geom_offsets: dict
    function: object
    return_type: object
    name: str

    def get_function_call_sql(self, table_name: str, cast_to_wkb: bool = False) -> str:
        arg_offset_str = ", ".join([f"_{el}" for el in self.arg_offsets])
        function_expr = f"{self.name}({arg_offset_str})"
        if isinstance(self.return_type, GeometryType) and cast_to_wkb:
            return f"SELECT ST_GeomToSedonaSpark({function_expr}) AS _0 FROM {table_name}"  # nosec

        return f"SELECT {function_expr} AS _0 FROM {table_name}"  # nosec

    def sedona_db_transformation_expr(
        self, table_name: str, cast_to_wkb: bool = False
    ) -> str:
        fields = []
        for arg in self.arg_offsets:
            if arg in self.geom_offsets and cast_to_wkb:
                crs = self.geom_offsets[arg]
                fields.append(
                    f"ST_GeomFromSedonaSpark(_{arg}, 'EPSG:{crs}') AS _{arg}"
                )  # nosec
                continue

            fields.append(f"_{arg}")

        fields_expr = ", ".join(fields)
        return f"SELECT {fields_expr} FROM {table_name}"  # nosec
