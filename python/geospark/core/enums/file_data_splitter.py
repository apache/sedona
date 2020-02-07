from enum import Enum

import attr

from geospark.core.jvm.abstract import JvmObject
from geospark.utils.decorators import require


class FileDataSplitter(Enum):

    CSV = "CSV"
    TSV = "TSV"
    GEOJSON = "GEOJSON"
    WKT = "WKT"
    WKB = "WKB"
    COMA = "COMA"
    TAB = "TAB"
    QUESTIONMARK = "QUESTIONMARK"
    SINGLEQUOTE = "SINGLEQUOTE"
    QUOTE = "QUOTE"
    UNDERSCORE = "UNDERSCORE"
    DASH = "DASH"
    PERCENT = "PERCENT"
    TILDE = "TILDE"
    PIPE = "PIPE"
    SEMICOLON = "SEMICOLON"


@attr.s
class FileSplitterJvm(JvmObject):

    splitter = attr.ib(type=FileDataSplitter)

    def _create_jvm_instance(self):
        return self.jvm_splitter(self.splitter.value) if self.splitter.value is not None else None

    @property
    @require(["FileDataSplitter"])
    def jvm_splitter(self):
        return self.jvm.FileDataSplitter.getFileDataSplitter
