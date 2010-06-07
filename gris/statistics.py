"""
Container for statistical information about the grid compoenents.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="13.10.2009"
__version__="0.1.2"

# last change: added totalcpus to cluster attributes (CSTATS_ATTRS)

import logging
from gridmonitor.model.api.stats_api import StatsApi
from errors.stats import * 

class NGStats(StatsApi):

    QSTATS_ATTRS=['cpus','gridrunning','gridqueued','localqueued',\
                'prelrmsqueued','running'] # queue attributes
    CSTATS_ATTRS=['totaljobs','usedcpus','totalcpus']  # cluster attributes. Notice queue stats will be summed
                                                       # up to cluster stats
    SPECIAL_ATTRS=['vo_usage']   

    STATS_ATTRS = QSTATS_ATTRS + CSTATS_ATTRS + SPECIAL_ATTRS

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
        # XXX adding a check if child has correct type 
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

