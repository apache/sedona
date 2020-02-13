

class InvalidParametersException(Exception):
    """
    Exception added to handle invalid constructor parameters
    """

    def __init__(self, msg: str):
        super().__init__(msg)
