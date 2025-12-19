from dataclasses import dataclass

from sedona.spark import GeometryType


@dataclass
class UDFInfo:
    arg_offsets: list
    geom_offsets: dict
    function: object
    return_type: object
    name: str

    def get_function_call_sql(self, table_name: str) -> str:
        arg_offset_str = ", ".join([f"_{el}" for el in self.arg_offsets])
        function_expr = f"{self.name}({arg_offset_str})"
        if isinstance(self.return_type, GeometryType):
            return f"SELECT ST_GeomToSedonaSpark({function_expr}) AS _0 FROM {table_name}"

        return f"SELECT {function_expr} AS _0 FROM {table_name}"

    def sedona_db_transformation_expr(self, table_name: str) -> str:
        fields = []
        for arg in self.arg_offsets:
            if arg in self.geom_offsets:
                crs = self.geom_offsets[arg]
                fields.append(f"ST_GeomFromSedonaSpark(_{arg}, 'EPSG:{crs}') AS _{arg}")
                continue

            fields.append(f"_{arg}")


        fields_expr = ", ".join(fields)
        return f"SELECT {fields_expr} FROM {table_name}"
