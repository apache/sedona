from shapely.wkb import loads
import pytest


@pytest.mark.flink
def test_register(table_env):
    result = (
        table_env.sql_query("SELECT ST_ASBinary(ST_Point(1.0, 2.0))")
        .execute()
        .collect()
    )

    assert 1 == len([el for el in result])


@pytest.mark.flink
def test_register_udf(table_env):
    from pyflink.table.udf import ScalarFunction, udf

    class Buffer(ScalarFunction):
        def eval(self, s):
            geom = loads(s)
            return geom.buffer(1).wkb

    table_env.create_temporary_function(
        "ST_BufferPython", udf(Buffer(), result_type="Binary")
    )

    buffer_table = table_env.sql_query(
        "SELECT ST_BufferPython(ST_ASBinary(ST_Point(1.0, 2.0))) AS buffer"
    )

    table_env.create_temporary_view("buffer_table", buffer_table)

    result = (
        table_env.sql_query("SELECT ST_Area(ST_GeomFromWKB(buffer)) FROM buffer_table")
        .execute()
        .collect()
    )

    items = [el for el in result]
    area = items[0][0]

    assert 3.12 < area < 3.14
    assert 1 == len(items)
