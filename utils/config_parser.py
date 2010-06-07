#!/usr/bin/env python
"""
Configuration reader for site-specific SMSCG settings. The reader processes
ini-style configuration files.

Basic error handling has been implemented, ithowever does no semantic checking, 
e.g. if the option 'server:' has been set an invalid hostname it will not complain.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="5.1.2010"
__version__="0.1.0"

import ConfigParser
import sys
import os.path

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

    def get_voms_servers(self):
        """ 
        Returns a dictionary with the VO and the corresponding voms server. 
        The information about the supported VO and voms servers is read from the 
        configuration referred by the 'gridmomnitor_configfile'.

        returns --  {<vo_name> : <vo_server>}
        """ 
        
        gm_cfg = self.get('gridmonitor_configfile')
        parser2 = ConfigParser.ConfigParser()
        if os.path.exists(gm_cfg) and os.path.isfile(gm_cfg):
            parser2.read(gm_cfg)
        else:
            raise GIIS2DBConfigError("GridMonitor Config file missing", "File '%s' doesn't exist." % (gm_cfg))         

        if not parser2.has_section('app:main'):
            raise GIIS2DBConfigError("Secion app:main missing", "File '%s' has no app:main section." % (gm_cfg))         
        
        voms_raw = parser2.get('app:main','voms')
        voms_servers = dict()
        if voms_raw:
            for voms in  voms_raw.split(','):
                server = voms.split(':')[0]
                tail = voms.split(':')[1]
                for vo in tail.split('|'):
                    voms_servers[vo] = server.strip()
        
        return voms_servers
        

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
        print c.get_voms_servers()
        #print c.get_default_mappings()
        #print c.get_pool_accounts()
        g= c.get()
    except GIIS2DBError, e:
        print "Error: ", e.message

