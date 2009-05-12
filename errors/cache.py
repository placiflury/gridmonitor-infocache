
class CacheError(Exception):
    """ 
    Exception raised for Cache errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class CREATE_ERROR(CacheError):
    """Exception raised if Cache could not be created.
    """
    pass

class ACCESS_ERROR(CacheError):
    """Exception raised if Cache could not be accessed.
    """
    pass

