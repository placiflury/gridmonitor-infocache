#!/usr/bin/env python
"""
Class for querying the Nordugrid Information System GRIS. 
"""
# Todo Check validity of ldap records (expiration time etc.)

__author__="Placi Flury placi.flury@switch.ch"
__date__="11.04.2009"
__version__="0.2.1"

import ldap as LDAP
import logging
from common import *
from queue import NGQueue
from gridmonitor.model.api.cluster_api import ClusterApi
from errors.gris import * 

class NGCluster(LDAPCommon,ClusterApi):
    """
    A class to query the GRIS ldap of Nordugrid
    """
    CLUSTER_ATTRS = ["nordugrid-cluster-name","nordugrid-cluster-aliasname", 
                    "nordugrid-cluster-location", "nordugrid-cluster-support", 
                    "nordugrid-cluster-issuerca", "nordugrid-cluster-middleware",
                    "nordugrid-cluster-runtimeenvironment","nordugrid-cluster-totaljobs",
                    "nordugrid-cluster-benchmark", "nordugrid-cluster-homogeneity",
                    "nordugrid-cluster-nodecpu", "nordugrid-cluster-opsys", 
                    "nordugrid-cluster-usedcpus"]

    PREFIX = "nordugrid-cluster-"

    def __init__(self,host, port=2135):
        """
        Queries the GRIS server specified by 'host' on 'port' (default = 2135)
        for information about that NorduGrid cluster.
        
        For every variable in CLUSTER_ATTRS an object variable will be created. For the
        sake of notational simplicity the variable name omits the 'PREFIX' part (if applicable).
        """ 
        self.log = logging.getLogger(__name__)

        host = host.strip() 
        if not host.startswith("ldap://"):
            host = "ldap://" + host
        port_suffix = ":" + str(port)
        if not host.endswith(port_suffix):
            host += port_suffix
        self.gris_server = host        
        self.queues = []    

        try:
            self.ldap = LDAP.initialize(host)    
            self.ldap.simple_bind_s()
            self.log.debug("Connected to GRIS %s:" % (host))
        except LDAP.SERVER_DOWN:
            self.log.warn("GRIS server %s is down." % host)
            raise CONNECT_ERROR("Connection error", "Could not connect to server %s" % host)
        except LDAP.LDAPError, e:
            self.log.error("GRIS ldap error: %s." % e.desc())
            raise GRIS_ERROR("GRIS Error", e.desc()) 

        self.obj_vars = [] # holds names of object variables we dynamically generate
        self.__generate_cluster_vars() 
        self.refresh_queues()
        
    def __del__(self):
        if self.ldap:
            self.ldap.unbind()
        if self.queues:
            for q in self.queues:
                del q

    def pickle_init(self):
        """ Cuts back class/object so that it can be pickled. Beware,
            that after calling this method, the object can only be used
            to retrieve static information (since the ldap connection
            gets cut.)"""
        if self.ldap:
            self.ldap.unbind()
            self.ldap = None
        del self.log
        self.queues = None  
    
    def __generate_cluster_vars(self):
        """ Generates and populates dynamically object variables from CLUSTER_ATTRS list. If
            possible the PREFIX is omitted.
        """
        
        base ="Mds-Vo-name=local,o=grid"
        filter ="(objectClass=nordugrid-cluster)"
        scope = LDAP.SCOPE_ONELEVEL
    
        try:        
            res = self.ldap.search_s(base,scope,filter, NGCluster.CLUSTER_ATTRS)
        except ldap.NO_SUCH_OBJECT:
            self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGCluser.CLUSTER_ATTRS))
            
            raise GRISError("GRIS No such object",
                "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGCluster.CLUSTER_ATTRS))

        records = LDAPCommon.format_res(self,res)
       
        for attr in NGCluster.CLUSTER_ATTRS:   # dynamic generation  of object variables
            var_name = attr.replace(NGCluster.PREFIX,'') 
            var_name = var_name.replace('-','_')
            self.obj_vars.append(var_name)
            for rec in records:
                if rec.has_attribute(attr):
                    assignment = "self.%s = rec.get_attr_values(attr)" % var_name
                    self.log.debug("Assigning: %s=%s" % (var_name,rec.get_attr_values(attr)))
                else:
                    assignment = "self.%s = []" % var_name
                    self.log.debug("Assigning: %s=[]" % (var_name))
                exec(assignment)


    def refresh_queues(self):
        """ Populates list of queue objects."""        

        base = "nordugrid-cluster-name=%s,Mds-Vo-name=local,o=grid" % self.get_name()
        filter ="(objectClass=nordugrid-queue)"
        scope = LDAP.SCOPE_ONELEVEL
        try:
            res = self.ldap.search_s(base,scope,filter,NGQueue.QUEUE_ATTRS)
        except ldap.NO_SUCH_OBJECT:
            self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGQueue.QUEUE_ATTRS))
            
            raise GRISError("GRIS No such object",
                "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGQueue.QUEUE_ATTRS))
        records = LDAPCommon.format_res(self,res)
        
        if self.queues:
            del self.queues 
            self.queues = []

        for rec in records:
            self.queues.append(NGQueue(self.ldap,self.get_name(),rec))

    def refresh_queue(self, name):
        base = "nordugrid-queue-name=%s, nordugrid-cluster-name=%s,\
                Mds-Vo-name=local,o=grid" % (name, self.get_name())
        filter ="(objectClass=nordugrid-queue)"
        scope = LDAP.SCOPE_ONELEVEL
        
        try: 
            res = self.ldap.search_s(base,scope,filter,NGQueue.QUEUE_ATTRS)
        except LDAP.NO_SUCH_OBJECT:
            self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGQueue.QUEUE_ATTRS))

            raise GRISError("GRIS No such object",
                "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.gris_server,base,scope,filter,NGQueue.QUEUE_ATTRS))

        records = LDAPCommon.format_res(self,res)
 
        for i,q in enumerate(self.queues):
            qname = q.get_name()
            if qname == name:
                if len(records) > 0:
                    self.queues[i] = NGQueue(self.ldap, self.get_name(),records[0])
                else:
                    self.queues.remove(q)
                break

    def get_queues(self):
        """ returns list of queue objects """
        if not self.queues:
            self.refresh_queues()
        return self.queues

    def get_name(self):
        """ returns hostname of cluster (frontend) """
        return self.get_attribute_first_value('name')
    
    def get_alias(self):
        """ returns alias name  of cluster (frontend) """
        return self.get_attribute_first_value('aliasname')

    def get_attribute_names(self):
        """ Getting all the attribute names defined for this cluster. """
        return self.obj_vars

    def get_attribute_values(self, name):
        """
        Returns values (list) of specified variable. Only attributes 
        that are part of 'cluster' ldap entry (and that are 
        specified in the CLUSTER_ATTRs can be queried).
        
        params: name - name of variable (without PREFIX)
        """
        name = name.replace('-','_')
        if name in self.obj_vars:
            return eval("self."+ name)
        return [] 

    def get_attribute_first_value(self,name):
        items = self.get_attribute_values(name)
        if items:
            return items[0]
        return None

if __name__ == "__main__":
    import logging.config
    logging.config.fileConfig("logging.conf")
    
    try: 
        ng = NGCluster("smscg-ce.projects.cscs.ch", 2135)
    except CONNECT_ERROR:
        print "could not connect...adios"

    print ng.get_name()
    for q in ng.get_queues():
        for attr in q.get_attribute_names():
            print attr , " -> " , q.get_attribute_values(attr)
    ng.refresh_queue('swisspit')
    for q in ng.get_queues():
        for attr in q.get_attribute_names():
            print attr , " -> " , q.get_attribute_values(attr)
    ng.refresh_queue('swisspit')
    del ng
        
