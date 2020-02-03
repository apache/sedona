from geo_pyspark.utils.meta import MultipleMeta


class TestMultipleMeta:

    def test_class_methods(self):
        class A(metaclass=MultipleMeta):
            @classmethod
            def get(cls, a: int, b: int) -> int:
                return a + b + A.help_function()

            @classmethod
            def get(cls, a: str) -> str:
                return a * A.help_function()

            @classmethod
            def help_function(cls) -> int:
                return 5

            @classmethod
            def wget(cls, a: int, b: str) -> str:
                return a * b

            @classmethod
            def wget(cls, a: int) -> int:
                return a

        assert A.help_function() == 5
        assert A.get("s") == "s" * 5
        assert A.get(1, 2) == 8
        assert A.wget(4, "s") == 4*"s"
        assert A.wget(4) == 4

    def test_static_methods(self):
        class A(metaclass=MultipleMeta):
            @staticmethod
            def get(a: int, b: int) -> int:
                return a + b

            @staticmethod
            def get(a: str) -> str:
                return a

            @classmethod
            def help_function(cls) -> int:
                return A.get(1, 2) * A.get("s")
        assert A.help_function() == "sss"

    def test_basic_methods(self):

        class A(metaclass=MultipleMeta):
            @staticmethod
            def get(a: int, b: int) -> int:
                return a + b

            @staticmethod
            def get(a: int) -> int:
                return a

            @classmethod
            def wget(cls, a: int):
                return a

            @classmethod
            def wget(cls, c: int, a: int):
                return c + a

            def multiply_get(self) -> int:
                return A.get(1, 2) * A.get(3)

            def multiply_get(self, a: int):
                return A.get(a, 2) * A.get(a)

            def multiply_get(self, c: str):
                return A.wget(1, 2) * A.wget(4) * c
        assert A().multiply_get() == 9
        assert A().multiply_get(10) == 120
        assert A().multiply_get("c") == 12 * "c"
