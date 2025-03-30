import os.path
import zipfile


def download_jar(sedona_version, scala_version) -> str:
    pass


def list_classes_in_jar(jar_file_path):
    with zipfile.ZipFile(jar_file_path, 'r') as jar:
        jar_contents = jar.namelist()

        class_files = [
            file for file in jar_contents
            if file.endswith('.class') and "org/apache/sedona/flink/confluent/constructors" in file and "ST_" in file
        ]

        return class_files


def list_functions(sedona_version, scala_version, path: str | None = None):
    if path is None:
        path = download_jar(sedona_version, scala_version)

    path = os.path.join(path, f"sedona-flink-shaded_{scala_version}-{sedona_version}-SNAPSHOT.jar")

    return list_classes_in_jar(path)
