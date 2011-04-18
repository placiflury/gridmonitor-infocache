class GRIS_ERROR(Exception):
    """ 
    Exception raised for GRIS errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class CONNECT_ERROR(GRIS_ERROR):
    """Exception raised if GRIS not reacheable.
    """
    pass


