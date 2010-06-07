"""
Nordugrid Information system modeling class 
"""

__author__="Placi Flury placi.flury@switch.ch"
__date__="11.4.2009"
__version__="0.2.0"

import ldap
import logging

from user import NGUser 
from job import NGJob
from infocache.utils.common import LDAPSearchResult, LDAPCommon
from infocache.utils.utils import str_cannonize
from gridmonitor.model.api.queue_api import QueueApi

class NGQueue(LDAPCommon, QueueApi):
    """ 
    QUEUE_ATTRS specifies the names of the ldap queue attributes we
    are interested in. We only store those enlisted.

    The class internally maps the QUEUE_ATTRS into object variables. For the 
    sake of notational simplicity the PREFIX part is omitted (if possible).
    """

    QUEUE_ATTRS = ["Mds-validto","nordugrid-queue-name", \
                    "nordugrid-queue-status", \
                    "nordugrid-queue-running", \
                    "nordugrid-queue-gridrunning", \
                    "nordugrid-queue-prelrmsqueued",\
                    "nordugrid-queue-localqueued", \
                    "nordugrid-queue-gridqueued", \
                    "nordugrid-queue-totalcpus",\
                    "nordugrid-queue-nodememory", \
                    "nordugrid-queue-nodecpu", \
                     "nordugrid-queue-maxcputime", \
                    "nordugrid-queue-maxwalltime", \
                    "nordugrid-queue-benchmark", \
                    "nordugrid-homogeneity" ]
    PREFIX = "nordugrid-queue-"  

    def __init__(self,ldap_con,cluster_name,rec):
        """ 
        Generates object variables for every QUEUE_ATTRS entry. If possible the
        PREFIX is omitted from the variable name. 
        
        params: rec - LDAPSearchResult object
        """       
        self.obj_vars = []  # names of object variables we dynamically generate 
        self.allowed_users = []  # dns of users that are allowed on this queue
        self.jobs =[]         # jobs (grid and non-grid) of queue
        self.cluster_name = cluster_name
        self.ldap_con = ldap_con        
        self.log = logging.getLogger(__name__)

        for attr in NGQueue.QUEUE_ATTRS:
            var_name = attr.replace(NGQueue.PREFIX,'') # getting rid of prefix
            var_name = var_name.replace('-','_')  
            self.obj_vars.append(var_name)
            if rec.has_attribute(attr):  
                assignment = "self.%s = rec.get_attr_values(attr)" % var_name
                self.log.debug("Assigning: %s=%s" % (var_name,rec.get_attr_values(attr)))
            else:
                assignment = "self.%s=[]" % var_name
                self.log.debug("Assigning: %s=[]" % (var_name))
            exec(assignment) 
        
        self.cname = str_cannonize(self.get_name())        
        self.refresh_users()
    
    def pickle_init(self):
        if self.ldap_con:
            self.ldap_con = None # unbinding shall be done where binding took place
        del self.log        
        self.jobs = []
        self.allowed_users =[]
        

    def get_jobs(self,filter):
        """ LDAP gets querried every time, hence 'refresh_jobs'
            operation is intrinsic to this call.
        """
        base = "nordugrid-info-group-name=jobs,\
            nordugrid-queue-name=%s,\
            nordugrid-cluster-name=%s,\
            Mds-Vo-name=local,o=grid"\
            % (self.get_name(), self.cluster_name)
        scope = ldap.SCOPE_ONELEVEL

        try:
            res = self.ldap_con.search_s(base,scope,filter,NGJob.JOB_ATTRS)
        except ldap.NO_SUCH_OBJECT:
            self.log.error( "GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." 
                        % (self.cluster_name,base,scope,filter,NGJob.JOB_ATTRS))

        records = LDAPCommon.format_res(self,res)
        jobs = []

        for rec in records:
            jobs.append(NGJob(rec))

        return jobs

    def get_job(self,job_id):
            """ 
            Query LDAP for job specified by job_id.
            """
            base = "nordugrid-info-group-name=jobs,\
                nordugrid-queue-name=%s,\
                nordugrid-cluster-name=%s,\
                Mds-Vo-name=local,o=grid"\
                % (self.get_name(), self.cluster_name)

            filter = "(nordugrid-job-globalid=%s)" % job_id
            scope = ldap.SCOPE_ONELEVEL
            try:
                res = self.ldap_con.search_s(base,scope,filter,NGJob.JOB_ATTRS)
            except ldap.NO_SUCH_OBJECT:
                self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'."
                     % (self.cluster_name,base,scope,filter,NGJob.JOB_ATTRS))
            records = LDAPCommon.format_res(self,res)

            if records:
                return NGJob(records[0])
            return None

        
    def refresh_users(self):
        # fetch users that are allowed for this queue
        base = "nordugrid-info-group-name=users,\
            nordugrid-queue-name=%s,\
            nordugrid-cluster-name=%s,\
            Mds-Vo-name=local,o=grid" \
            % (self.get_name(), self.cluster_name)
        filter = "(objectClass=nordugrid-authuser)"
        scope = ldap.SCOPE_ONELEVEL
        
        try:
            res = self.ldap_con.search_s(base,scope,filter,NGUser.USER_ATTRS)
        except ldap.NO_SUCH_OBJECT:
            self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %
            (self.cluster_name,base,scope,filter,NGUser.USER_ATTRS))

            raise GRISError("GRIS No such object",
                "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.cluster_name,base,scope,filter,NGUser.USER_ATTRS))
        
        records = LDAPCommon.format_res(self,res)

        self.allowed_users = []

        for rec in records:
            self.allowed_users.append(NGUser(rec).get_user_dn())

    def get_allowed_users(self):
        """ Returns 'DN' list of users that are allowed to submit 
            for this particular queue.
        """
        return self.allowed_users


    def get_name(self):
        return self.get_attribute_first_value("name")
    
    def get_cname(self):
        return self.cname

    def get_name(self):
        return self.get_attribute_first_value("name")

    def get_cname(self):
        return self.cname

    def get_cpus(self):
        val = self.get_attribute_first_value("totalcpus")
        if val:
            return int(val)
        return 0

    def get_gridrunning(self):
        val = self.get_attribute_first_value("gridrunning")
        if val:
            return int(val)
        return 0

    def get_gridqueued(self):
        val= self.get_attribute_first_value("gridqueued")
        if val:
            return int(val)
        return 0

    def get_localqueued(self):
        val =  self.get_attribute_first_value("localqueued")
        if val:
            return int(val)
        return 0

    def get_running(self):
        val = self.get_attribute_first_value("running")
        if val:
            return int(val)
        return 0

    def get_prelrmsqueued(self):
        val = self.get_attribute_first_value("prelrmsqueued")
        if val:
            return int(val)
        return 0


    def get_attribute_first_value(self,name):
        items = self.get_attribute_values(name)
        if items:
            return items[0]
        return None


    def get_attribute_names(self):
        """ Getting all the attribute names defined for this queue. """
        return self.obj_vars   
 
    def get_attribute_values(self, name):
        """
        Returns values of specified variable. Only attributes 
        of that are part of 'queue' ldap entry (and that are 
        specified in the QUEUE_ATTRS can be queried).
        
        params: name - name of variable (without PREFIX)
        """
        name = name.replace('-','_')
        if name in self.obj_vars:
            return eval("self."+name)
        return [] 
    
    def set_maxsubtime_status_filter(self,**kwargs):
        """ 
        Specify a filter for getting jobs on specific cluster and queue.
        
        Args:
        status  - job status (optional)
        subtime - only take jobs submitted since subtime (optional). Time/date must
                  be specified in ldap format i.e. %Y%m%d%H%M%SZ    
        negation_of_status - if set to True, it negates the 'status' (requires status to be set)
        """


        if 'negation_of_status' in kwargs.keys() and 'status' in kwargs.keys() \
            and kwargs['negation_of_status']:
            prefix='(&(objectClass=nordugrid-job) \
                    (nordugrid-job-submissiontime>=%s) \
                 (!(nordugrid-job-status=%s)))'
        else:
            prefix='(&(objectClass=nordugrid-job) \
                    (nordugrid-job-submissiontime>=%s) \
                    (nordugrid-job-status=%s))'

        if not 'subtime' in kwargs.keys():
            prefix='(&(objectClass=nordugrid-job) (nordugrid-job-submissiontime=%s) (nordugrid-job-status=%s))'
            subtime='*'
        else:
            subtime=kwargs['subtime']
        if not 'status' in kwargs.keys():
            status = '*'
        else:
            status=kwargs['status']
        return  prefix % (subtime,status)





