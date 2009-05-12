"""
Nordugrid Information system modeling class 
"""
# Todo Check validity of ldap records (expiration time etc.)

__author__="Placi Flury placi.flury@switch.ch"
__date__="11.4.2009"
__version__="0.1.1"

import logging

class NGUser:
    """ 
    USER_ATTRS specifies the names of the ldap queue attributes we
    are interested in. We only store those enlisted.

    The class internally maps the USER_ATTRS into class variables. For the 
    sake of notational simplicity the PREFIX part is omitted (if possible).
    """
    
    USER_ATTRS = ["nordugrid-authuser-sn","Mds-validto",\
                "nordugrid-authuser-queuelength" ] # number of queueing jobs user


    PREFIX = "nordugrid-authuser-"  

    def __init__(self,rec):
        """ 
        Generates class variables for every USER_ATTRS entry. If possible the
        PREFIX is omitted from the variable name. 
        
        params: rec - LDAPSearchResult object
        """       
        self.class_vars = []  # names of class variables we dynamically generate 
        self.log = logging.getLogger(__name__)

        for attr in NGUser.USER_ATTRS:
            var_name = attr.replace(NGUser.PREFIX,'') # getting rid of prefix
            var_name = var_name.replace('-','_')  
            self.class_vars.append(var_name)
            if rec.has_attribute(attr):
                assignment = "self.%s = rec.get_attr_values(attr)" % var_name
                self.log.debug("Assigning: %s=%s" % (var_name,rec.get_attr_values(attr)))
            else:
                assignment = "self.%s=None" % var_name
                self.log.debug("Assigning: %s=[]" % (var_name))
            exec(assignment) 

    def pickle_init(self):
        del self.log

    def get_user_dn(self):
        user_dn = self.get_attribute_values("sn")
        if user_dn:
            return user_dn[0]
        else:
            return None

    def get_attributes(self):
        """ Getting all the attribute names defined for this user. """
        return self.class_vars   
 
    def get_attribute_values(self, name):
        """
        Returns values of specified variable. Only attributes 
        of that are part of 'queue' ldap entry (and that are 
        specified in the USER_ATTRS can be queried).
        
        params: name - name of variable (without PREFIX)
        """
        name = name.replace('-','_')
        if name in self.class_vars:
            return eval("self."+name)
        return None

