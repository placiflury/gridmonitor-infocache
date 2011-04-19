"""
Container for statistical information about the grid compoenents.
"""
__author__="Placi Flury grid@switch.ch"
__date__="09.11.2010"
__version__="0.2.0"

# last change: added totalcpus to cluster attributes (CSTATS_ATTRS)

import logging
from gridmonitor.model.api.stats_api import StatsApi
from infocache.errors.stats import * 

class NGStats(StatsApi):

    QSTATS_ATTRS=['grid_running','grid_queued','local_queued',\
                'prelrms_queued','running'] # queue attributes
    CSTATS_ATTRS=['total_jobs','used_cpus','total_cpus']  # cluster attributes, appended 
                                                       # to cluster stats

    STATS_ATTRS = QSTATS_ATTRS + CSTATS_ATTRS 

    def __init__(self,name, type):
        self.log = logging.getLogger(__name__)
        self.name = name        
        self.children = [] # list of subcomponents
        
 
        if type not in StatsApi.VALID_TYPES:
            self.log.error("Invalid type '%s' for statistical container." % type)
            raise TYPE_ERROR("Type error", "Type '%s' not valid" % type)            
    
        for attr_name in NGStats.STATS_ATTRS: 
            assignment = "self.%s=0" % attr_name
            exec(assignment)
    
    def set_attribute(self,attr_name,attr_value):

        if attr_name in NGStats.STATS_ATTRS:
            if type(attr_value) == int:
                assignment = ("self.%s=%d" % (attr_name,attr_value))
            if type(attr_value) == float:
                assignment = ("self.%s=%f" % (attr_name,attr_value))
            else:
                assignment = ("self.%s=%s" % (attr_name,attr_value))
            exec(assignment)
        else: # just ignore assigment
            self.log.warn("Assigment to unknown object attribute ('%s')" %(attr_name))
            
    def add_child(self,child):
        if isinstance(child,NGStats):
            self.children.append(child)
        else: 
            self.log.warn("Tried to add child of wrong type.")
            
    def get_type(self):
        return self.type

    def get_name(self):
        return self.name

    def get_children(self):
        """ returns list of sub-containers. The items of the list are
            as well NGStats objects.
        """
        return self.children

    def get_attribute_names(self):
        """ Getting all the attribute names defined for this container. """
        return NGStats.STATS_ATTRS 

    def get_attribute(self, name):
        """
        Returns value of specified variable. 
        """
        if name in NGStats.STATS_ATTRS:    
            return eval("self."+name)
        return None 

    def pickle_init(self):
        del(self.log)

