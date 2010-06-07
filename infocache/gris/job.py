"""
Nordugrid Information system modeling class 
"""
# Todo Check validity of ldap records (expiration time etc.)

__author__="Placi Flury placi.flury@switch.ch"
__date__="11.4.2009"
__version__="0.1.1"

import logging
from gridmonitor.model.api.job_api import JobApi

class NGJob(JobApi):
    """ 
    JOB_ATTRS specifies the names of the ldap queue attributes we
    are interested in. We only store those enlisted.

    The class internally maps the JOB_ATTRS into object variables. For the 
    sake of notational simplicity the PREFIX part is omitted (if possible).
    """
    
    JOB_ATTRS = ["nordugrid-job-jobname","nordugrid-job-globalid", \
            "nordugrid-job-globalowner","nordugrid-job-status",\
            "nordugrid-job-clientsoftware", "nordugrid-job-cpucount",\
            "nordugrid-job-jobname", "nordugrid-job-executionnodes",\
            "nordugrid-job-submissiontime","nordugrid-job-completiontime",\
            "nordugrid-job-exitcode","nordugrid-job-proxyexpirationtime",\
            "nordugrid-job-reqcputime","nordugrid-job-reqwalltime",\
            "nordugrid-job-sessiondirerasetime", "nordugrid-job-stderr",\
            "nordugrid-job-stdout", "nordugrid-job-usedcputime", \
            "nordugrid-job-execcluster", "nordugrid-job-execqueue", \
            "nordugrid-job-usedwalltime", "nordugrid-job-submissionui"]


    PREFIX = "nordugrid-job-"  

    def __init__(self,rec):
        """ 
        Generates object variables for every JOB_ATTRS entry. If possible the
        PREFIX is omitted from the variable name. 
        
        params: rec - LDAPSearchResult object
        """       
        self.obj_vars = []  # names of object variables we dynamically generate 
        self.log = logging.getLogger(__name__)

        for attr in NGJob.JOB_ATTRS:
            var_name = attr.replace(NGJob.PREFIX,'') # getting rid of prefix
            var_name = var_name.replace('-','_')  
            self.obj_vars.append(var_name)
            if rec.has_attribute(attr):
                assignment = "self.%s = rec.get_attr_values(attr)" % var_name
                self.log.debug("Assigning: %s=%s" % (var_name,rec.get_attr_values(attr)))
            else:
                assignment = "self.%s=[]" % var_name
                self.log.debug("Assigning: %s=[]" % (var_name))
            exec(assignment) 

    def pickle_init(self):
        del self.log

    def get_jobname(self):
        return self.get_attribute_first_value("jobname")

    def get_globalid(self):
        return self.get_attribute_first_value("globalid")

    def get_globalowner(self):
       return self.get_attribute_first_value("globalowner")

    def get_status(self):
       return self.get_attribute_first_value("status")

    def get_exitcode(self):
        return self.get_attribute_first_value("exitcode")

    def get_cluster_name(self):
        return self.get_attribute_first_value("execcluster")

    def get_queue_name(self):
        return self.get_attribute_first_value("execqueue")

    def get_usedwalltime(self):
        t= self.get_attribute_first_value("usedwalltime")
        if t:
            return int(t)
        else:  # XXX empiric... don't know why that happens 
            return 0

    def get_attribute_first_value(self,name):
        items = self.get_attribute_values(name)
        if items:
            return items[0]
        return None

    def get_attribute_names(self):
        """ Getting all the attribute names defined for this user. """
        return self.obj_vars   
 
    def get_attribute_values(self, name):
        """
        Returns values of specified variable. Only attributes 
        of that are part of 'queue' ldap entry (and that are 
        specified in the JOB_ATTRS can be queried).
        
        params: name - name of variable (without PREFIX)
        """
        name = name.replace('-','_')
        if name in self.obj_vars:
            return eval("self."+name)
        return [] 

