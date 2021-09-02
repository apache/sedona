from sedona.core.serde.spark_config import spark_conf_getter
from tests.tools import serializer_path


def pytest_configure(config):
    with open(serializer_path) as file:
        lines = file.readlines()
    serializer = "".join(lines).strip()

    spark_conf_getter.serialization = serializer
