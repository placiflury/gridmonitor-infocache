# last modified 10.3.2009 
class GIISError(Exception):
    """ 
    Exception raised for GIIS errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
    
    def desc(self):
        return self.message


class GIISConError(GIISError):
    """Exception raised if GIIS not reacheable.
    """
    pass
