#!/usr/bin/env python
"""
The Cache class is used to cache the  
information from the grid information system. The class
provides methods to populate and access the cache.

XXX: collect statistics and store them in cache

"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="14.4.2009"
__version__="0.1.0"

import shelve,logging, os.path
from os import rename
from errors.cache import *
from errors.stats import *
from cache import Cache
from gris import *
from statistics import *

class GrisCache(Cache):

    def __init__(self,dbfile,infosys):
        self.log = logging.getLogger(__name__)
        self.gris_list = infosys
        self.dbfile = dbfile 
    
    def create(self):
        """ Creates a new and empty shelve database that is used
            as cache file. 
        """
        dbfile_tmp = self.dbfile + ".lock"

        try:
            self.dbase = shelve.open(dbfile_tmp, 'n') # creates a new and empty shelve
            self.log.info("Created new cache at: %s" % dbfile_tmp)
        except Exception, e:
            self.log.error("Creation of '%s' cache failed with %r" % (dbfile_tmp, e))
            raise CREATE_ERROR("Creation Error", "Could not create cache at '%s'." % dbfile_tmp)
            
    def populate(self):
        """ Populates cache with information that gets directly
            queries from the information system. 
        """
        try:
            self._populate_clusterinfo()
            self._populate_userinfo()
            self._populate_statistics()
        except Exception, e:
            self.log.error("While populating cache got '%r'" % e)
            raise CREATE_ERROR("Populating cache failed", "Could not populate cache, got error %r" % e)

    def _populate_clusterinfo(self):
        clusters = dict()
        cluster_queues = dict()
        cluster_jobs = dict()
        cluster_allowed_users = dict() 
        
        # some init values
        self.dbase['cluster_queues'] =cluster_queues
        self.dbase['cluster_jobs'] = cluster_jobs        
        self.dbase['cluster_allowed_users'] = cluster_allowed_users        
       
        for gris, port in self.gris_list:
            try:
                ng = NGCluster(gris,port)
            except:
                continue
            
            jobs_list = list()
            allowed_user_list = list()
            duplicates_list = list()
            queues_dict = dict()
            
            cluster_name = ng.get_name()
            queues = ng.get_queues()

            for q in queues:
                queues_dict[q.get_name()] = q
                for j in q.get_jobs():
                    j.pickle_init()
                    jobs_list.append(j)
                for u in q.get_allowed_users():
                    if u not in duplicates_list: 
                        duplicates_list.append(u)
                        allowed_user_list.append(u)
                q.pickle_init()

            cluster_queues[cluster_name] = queues_dict
            cluster_jobs[cluster_name] = jobs_list
            cluster_allowed_users[cluster_name] = allowed_user_list
 
            ng.pickle_init()
            clusters[cluster_name]=ng

            self.dbase['cluster_queues'] =cluster_queues
            self.dbase['cluster_jobs'] = cluster_jobs        
            self.dbase['cluster_allowed_users'] = cluster_allowed_users        

        self.dbase['clusters'] = clusters
      
   
    def _populate_userinfo(self):
        user_jobs = dict()
        user_cluster_queues = dict()

        for cluster_name,cluster in self.dbase['clusters'].iteritems():
            queues_dict = self.dbase['cluster_queues'][cluster_name]
            cluster_jobs = self.dbase['cluster_jobs'][cluster_name]

            for j in cluster_jobs:
                owner = j.get_globalowner()
                job_qname = j.get_queue_name()
                status = j.get_status()
                try:
                    job_q = queues_dict[job_qname]
                except:
                    self.log.info("The job %s has run on a queue (%s) that doesn't exist anymore on %s." % \
                        (j.get_globalid(), job_qname, cluster_name))
                    continue
         
                if not user_jobs.has_key(owner):
                    user_jobs[owner] = dict()
                
                if owner not in job_q.get_allowed_users(): # orphaned jobs
                    if not user_jobs[owner].has_key('orphans'):
                        user_jobs[owner]['orphans'] = list()
                    self.log.info("Found orphaned job of user '%s' on cluster/queue: '%s/%s'" % \
                            (owner,j.get_cluster_name(),job_qname))
                    user_jobs[owner]['orphans'].append(j)    
                    self.log.error("%s not in %r" % (owner, job_q.get_allowed_users()))
                else:
                    if not user_jobs[owner].has_key(status):
                        user_jobs[owner][status] = list()
                    user_jobs[owner][status].append(j)
                            
                    if not user_cluster_queues.has_key(owner):
                        user_cluster_queues[owner] = dict(cluster_name=list())
                    if not user_cluster_queues[owner].has_key(cluster_name):
                        user_cluster_queues[owner][cluster_name] = list()
                    user_cluster_queues[owner][cluster_name].append(job_q) 
                
        self.dbase['user_jobs'] = user_jobs
        self.dbase['user_cluster_queues'] = user_cluster_queues

    
    def _populate_statistics(self):
        """ collects statistics about user, jobs and clusters. """
        # XXX user statistics skipped for the moment as it's not 
        #     yet clear what we want to evaluate there.
        # XXX keep track of orphans
        
        gstats = NGStats('smscg','grid')  
        
        for cluster_name in self.dbase['cluster_queues'].keys():
            cstats = NGStats(cluster_name,'cluster')
            for qname in self.dbase['cluster_queues'][cluster_name].keys():
                q = self.dbase['cluster_queues'][cluster_name][qname]
                qstats = NGStats(q.get_name(),'queue') 
                for attr_name in NGStats.STATS_ATTRS:
                    fct_sig = "q.get_%s()" % attr_name
                    qstats.set_attribute(attr_name,eval(fct_sig))
                    cstats.set_attribute(attr_name,eval(fct_sig) + cstats.get_attribute(attr_name))   
                    gstats.set_attribute(attr_name,eval(fct_sig) + gstats.get_attribute(attr_name))   
                qstats.pickle_init()
                cstats.add_child(qstats)
            cstats.pickle_init()
            gstats.add_child(cstats)
        gstats.pickle_init() 
        self.dbase['grid_stats'] = gstats             
        
    def get_handle(self):
        if os.path.exists(self.dbfile) and os.path.isfile(self.dbfile):
            try:
                dbase = shelve.open(self.dbfile, 'r') # read only
                self.log.info("Accessing cache/shelve '%s'" % self.dbfile)
            except Exception, e:
                self.log.error("Access of '%s' cache failed with %r" % (self.dbfile, e))
                raise ACCESS_ERROR("ACCESS Error", "Could not access cache at '%s'." % self.dbfile)
            return dbase     
        return None

    def close_handle(self,handle):
        try:
            handle.close()
        except:
            pass

    def close(self):
        self.dbase.close()
        self.log.debug("Removing lock of new cache...")
        rename(self.dbfile+".lock", self.dbfile)    
    

if __name__ == "__main__":
    import logging.config
    import time
    from gridmonitor.model.api.job_api import *
    from gridmonitor.model.api.queue_api import *
    from gridmonitor.model.api.cluster_api import *
    
    logging.config.fileConfig("logging.conf")
    
    def _print(d):
        if type(d) == str:
            print d
        elif type(d) == dict:
            for k in d.keys():
                print k, ' - ', _print(d[k])
        elif type(d) == list:
            for item in d:
                _print(item)
        elif isinstance(d,JobApi):
            names = d.get_attribute_names()
            for name in names:
                print '\t', name, d.get_attribute_values(name)
        elif isinstance(d,QueueApi):
            names = d.get_attribute_names()
            for name in names:
                print '\t', name, d.get_attribute_values(name)
        elif isinstance(d,ClusterApi):
            names = d.get_attribute_names()
            for name in names:
                print '\t', name, d.get_attribute_values(name)
        elif isinstance(d,StatsApi):
            names = d.get_attribute_names()
            for name in names:
                print '\t', name, d.get_attribute(name)
        else:
            
            print "type %s not handled" % (type(d))

    # write to cache
    #db= GrisCache("/home/flury/testcache", [('smscg.unibe.ch','2135')])
    #db= GrisCache("/home/flury/testcache",[('globus.vital-it.ch','2135')],)
    #db= GrisCache("/home/flury/testcache",[('smscg-ce.projects.cscs.ch','2135')]")
    #db= GrisCache("/home/flury/testcache",[('nordugrid.unibe.ch','2135')]")
    #db= GrisCache("/home/flury/testcache",[('lheppc50.unibe.ch','2135')]")
    #db= GrisCache("/home/flury/testcache",[('smscg.epfl.ch','2135')]")
    #db= GrisCache("/home/flury/testcache",[('grid03.unige.ch','2135')]")
    #db= GrisCache("/home/flury/testcache",[('smscg-ce.cscs.ch','2135')]")
    db= GrisCache("/home/flury/testcache",[('ocikbpra.unizh.ch','2135'),('disir.switch.ch','2135'), \
                   ('smscg.unibe.ch','2135'),('globus.vital-it.ch','2135'), \
                    ('lheppc50.unibe.ch','2135'),('smscg.epfl.ch','2135'),\
                    ('grid03.unige.ch','2135'),('smscg-ce.cscs.ch','2135')])
    db.create()
    db.populate()
    db.close()

    # read from cache
    db_read = GrisCache([],"/home/flury/testcache")
    hd = db_read.get_handle() 
    for k in hd.keys():
        print "XXX" * 10, '--', k, '--','XXX' * 10
        _print(hd[k])
    
    db_read.close_handle(hd)
