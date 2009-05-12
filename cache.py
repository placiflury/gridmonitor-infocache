#!/usr/bin/env python
"""
The Cache class is used to cache the  
information from the grid information system. The class
provides methods to populate and access the cache.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="14.4.2009"
__version__="0.1.0"

import shelve
import logging

class Cache:
    
    def __init__(self,cache_file, infosys=None):
        """ infosys == None is used for read access 
            to the cache. 
        """
        pass

    def create(self):
        """ Creates a new and empty cache. Which 
            is locked for read until the close() 
            method is called."""
        pass

    def populate(self):
        """ Populates the new and empty cache. The following 
            cache 'layout' must be followed:
            db_key  - value (syntax)
            
            'clusters'  - dict('cluster_name'=<cluster_obj>,...) 
            'cluster_queues' - dict('cluster_name'= dict(queue_name: <<queue_obj>,..), ...)
            'cluster_jobs' - dict('cluster_name'=[<job_obj>,...],...)
            'cluster_allowed_users' - dict('cluster_name'=[user_dn,..])

            'user_cluster_queues' - dict(user_dn= dict(cluster_name=[<queue_obj>,],...),...)
            'user_jobs' = dict(user_dn:dict=(job_status=[<job_obj>,], ..., orphans=[<job_obj>,]),...) 

            Notice: <objects> must implement their respective 'API', 
            e.g. the <cluster_obj> must be accessible via methods of 
            api.cluster_api.ClusterApi
        """
        pass

    def get_handle(self):
        """ Returns a read handle to cache. Data
            of cache pointed to by handle are read only. 
            (It's a copy of the cache). If there is no cache
            it returns None.
        """ 
        pass
    def close_handle(self, handle):
        """ Closes handle to cache. Should be called after handle
            returned by get_handle method isn't used anymore.
         """
        pass

    def close(self):
        """ Closes connection to cache and removes read lock."""
