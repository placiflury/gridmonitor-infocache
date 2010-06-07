"""
Nordugrid Information system modeling classes.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="12.12.2008"
__version__="0.1.0"

from ldap.cidict import cidict


class LDAPSearchResult:
    """
     A class to model LDAP results.

     Taken from example in http://www.packtpub.com/article/python-ldap-applications-ldap-opearations
     (10.12.2008)
     and slightly adapted to our needs
    """

    def __init__(self, entry_tuple):
        (dn, attrs) = entry_tuple
        if dn:
            self.dn = dn
        else:
            return
        self.attrs = cidict(attrs)

    def has_attribute(self, attr_name):
        """Returns true if there is an attribute by this name in the
        record.

        has_attribute(string attr_name)->boolean
        """
        return self.attrs.has_key( attr_name )

    def get_attr_values(self, key):
        """Get a list of attribute values.
        get_attr_values(string key)->['value1','value2']
        """
        return self.attrs[key]

    def get_attr_names(self):
        """Get a list of attribute names.
        get_attr_names()->['name1','name2',...]
        """
        return self.attrs.keys()

    def get_dn(self):
        """Get the DN string for the record.
        get_dn()->string dn
        """
        return self.dn


class LDAPCommon:
    """
    Holds common functionality that is shared among 
    all Nordugrid Information system classes. It should be 
    viewed as an 'abstract' class that never gets instatiated.
    """

    def __is_record_valid(self):
        """
        Verified validity time of ldap entry
        """
        pass

    def format_res(self,results):
        """ 
        taken from example in http://www.packtpub.com/article/python-ldap-applications-ldap-opearations
        (10.12.2008)
        - returns a list of LDAPSearchResult items 
           [(dn, { attr-name : [value_1, value_2, ...]}),...]
        """
        res =[]
        if type(results) == tuple and len (results) == 2:
            (code, arr) = results
        elif type(results) == list:
            arr = results
        if len(results) == 0:
            return res

        for item in arr:
            res.append(LDAPSearchResult(item))

        return res








