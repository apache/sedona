import os

from sedona.core.jvm.config import SparkJars, SedonaMeta
from tests.test_base import TestBase


class TestCoreJVMConfig(TestBase):
    def test_yarn_jars(self):

        used_jar_files = ",".join(
            os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars"))
        )

        # Test's don't run on YARN but can set the property manually to confirm
        # the python checks work
        self.spark.conf.set("spark.yarn.dist.jars", used_jar_files)

        assert "sedona" in SparkJars().jars

        self.spark.conf.unset("spark.yarn.dist.jars")

    def test_sedona_version(self):
        used_jar_files = ",".join(
            os.listdir(os.path.join(os.environ["SPARK_HOME"], "jars"))
        )

        # Test's don't run on YARN but can set the property manually to confirm
        # the python checks work
        self.spark.conf.set("spark.yarn.dist.jars", used_jar_files)

        assert SedonaMeta().version is not None

        self.spark.conf.unset("spark.yarn.dist.jars")
