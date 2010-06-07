
class VOMSException(Exception):
    """ 
    Exception raised for VOMS errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class VOMS_CONNECT_ERROR(VOMSException):
    """Exception raised if connection to VOMS could not be set up.
    """
    pass

class VOMS_ENV_ERROR(VOMSException):
    """Exception raised if environment is not set up correctly.
    """
    pass

