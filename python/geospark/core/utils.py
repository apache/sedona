class ImportedJvmLib:
    _imported_libs = []

    @classmethod
    def has_library(cls, library: str) -> bool:
        return library in cls._imported_libs

    @classmethod
    def import_lib(cls, library: str) -> bool:
        if library not in cls._imported_libs:
            cls._imported_libs.append(library)
        else:
            return False
        return True

