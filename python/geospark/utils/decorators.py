from typing import List, Iterable, Callable, TypeVar

T = TypeVar('T')


class classproperty(object):

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)

    def __set__(self, instance, value):
        return self.f()


def get_first_meet_criteria_element_from_iterable(iterable: Iterable[T], criteria: Callable[[T], int]) -> int:
    for index, element in enumerate(iterable):
        if criteria(element):
            return index
    return -1


def require(library_names: List[str]):
    def wrapper(func):
        def run_function(*args, **kwargs):
            from geospark.core.utils import ImportedJvmLib
            has_all_libs = [lib for lib in library_names]
            first_not_fulfill_value = get_first_meet_criteria_element_from_iterable(
                has_all_libs, lambda x: not ImportedJvmLib.has_library(x)
            )

            if first_not_fulfill_value == -1:
                return func(*args, **kwargs)
            else:
                raise ModuleNotFoundError(f"Did not found {has_all_libs[first_not_fulfill_value]}, make sure that was correctly imported via py4j"
                                          f"Did you use GeoSparkRegistrator.registerAll, Your jars were properly copied to $SPARK_HOME/jars ? ")
        return run_function
    return wrapper

