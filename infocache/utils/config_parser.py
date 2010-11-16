#!/usr/bin/env python
"""
Configuration reader for site-specific SMSCG settings. The reader processes
ini-style configuration files.

Basic error handling has been implemented, ithowever does no semantic checking, 
e.g. if the option 'server:' has been set an invalid hostname it will not complain.
"""
__author__="Placi Flury grid@switch.ch"
__date__="8.11.2010"
__version__="0.2.0"

import ConfigParser
import sys
import os.path
import logging

config = None

class GIIS2DBError(Exception):
    """ 
    Exception raised for GIIS2DB errors.
    Attributes:
        expression -- input expression in which error occurred
        message -- explanation of error 
    """
    def __init__(self, expression, message):
        self.expression = expression
        self.message = message

class GIIS2DBConfigError(GIIS2DBError):
    """Exception raised on configuration error(s)."""
    pass


class ConfigReader:

    def __init__(self,config_file):
        """ 
            config_file -- file with 'ini' style config settings
        """     
        self.log = logging.getLogger(__name__)
        self.parser = ConfigParser.ConfigParser()
        if os.path.exists(config_file) and os.path.isfile(config_file):
            self.parser.read(config_file)
        else:
            raise GIIS2DBConfigError("Config file missing", "File '%s' doesn't exist." % (config_file))            


    def __get_list(self,list_str):
        """ Transforms  ',' delimited string list in a python list """
        items = list_str.split(',')
        
        l = []
        for i in items:
            it = i.strip()
            if it: 
                l.append(it)
        return l 

        

    def get(self, option=None):
        """
        Reads options from the [general] section of the config file.

        If no 'option' argument has been passed it will return 
        all options (and values) of the [general] section. 
        If an options has been specified its value, or None if the 
        value does not exist weill be returned.  
        """

        general = self.parser.options('general')
        
        gen = {}
        if not general:
            if option:
                return None
            return gen
        
        for item  in general:
            value = self.parser.get('general',item).strip()
            if value:
                gen[item] = value
        
        if option:
            if gen.has_key(option):
                return gen[option]
            return None
        return gen    


if __name__ == "__main__":
    try:        
        c = ConfigReader(sys.argv[1])
        #print c.get_default_mappings()
        #print c.get_pool_accounts()
        g= c.get()
    except GIIS2DBError, e:
        print "Error: ", e.message

