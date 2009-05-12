# last modified 10.3.2009 
class StatsError(Exception):
    """ 
    Exception raised for errors resulting from collection 
    of statistical information about the grid.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
    
    def desc(self):
        return self.message


class TYPE_ERROR(StatsError):
    """Exception raised if type of statistical container
       is not known..
    """
    pass
