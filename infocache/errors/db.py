class DbError(Exception):
    """ 
    Exception raised for Database  errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class Input_Error(DbError):
    """Exception raised for invalid input.
    """
    pass

