class GeometryUnavailableException(Exception):

    def __init__(self, message):
        super().__init__(message)


class InvalidGeometryException(Exception):

    def __init__(self, message):
        super().__init__(message)
