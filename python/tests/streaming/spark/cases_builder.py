from typing import Dict, Any, List


class SuiteContainer:

    def __init__(self, container: Dict[str, Any]):
        self.container = container

    @classmethod
    def empty(cls):
        return cls(container=dict(function_name=None, arguments=None, expected_result=None, transform=None))

    def with_function_name(self, function_name: str):
        self.container["function_name"] = function_name
        return self.__class__(
            container=self.container
        )

    def with_expected_result(self, expected_result: Any):
        self.container["expected_result"] = expected_result
        return self.__class__(
            container=self.container
        )

    def with_arguments(self, arguments: List[str]):
        self.container["arguments"] = arguments
        return self.__class__(
            container=self.container
        )

    def with_transform(self, transform: str):
        self.container["transform"] = transform
        return self.__class__(
            container=self.container
        )

    def __iter__(self):
        return self.container.__iter__()

    def __getitem__(self, name):
        return self.container.__getitem__(name)
