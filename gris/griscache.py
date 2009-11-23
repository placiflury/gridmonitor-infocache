#!/usr/bin/env python
"""
The Cache class is used to cache the  
information from the grid information system. The class
provides methods to populate and access the cache.


"""
from __future__ import with_statement

__author__="Placi Flury placi.flury@switch.ch"
__date__="13.11.2009"
__version__="0.1.4"
# last change: implementation of threading for ldap requests

import shelve,logging, os.path
from os import rename
from errors.cache import *
from errors.stats import *
from gridmonitor.model.cache.api.cache import Cache
from gris import *
from statistics import *

import time, threading, Queue


class GrisCache(Cache):
    
    THREAD_LIMIT= 10   # number of GRIS'es that will be queried in parallel

    def __init__(self,dbfile,infosys):
        self.log = logging.getLogger(__name__)
        self.gris_q = Queue.Queue(0) # no limit to queue
        for gris in infosys:
            self.gris_q.put(gris)
        self.dbfile = dbfile 
        self.t_lock = threading.Lock()
        # gris-specific stuff 
        self.blacklist = None
        self.whitelist = None
        self.giis_proctime = -1
    
    def set_blacklist(self,blacklist):
        self.blacklist = blacklist

    def set_giis_proctime(self,proctime):
        self.giis_proctime = proctime
    
    def set_whitelist(self,whitelist):
        self.whitelist= whitelist
    
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
            queried from the information system. 
        """
        try:
            self._populate_clusterinfo()
            self._populate_userinfo()
            self._populate_statistics()
            self._populate_grisspecific()
        except Exception, e:
            self.log.error("While populating cache got '%r'" % e)
            raise CREATE_ERROR("Populating cache failed", "Could not populate cache, got error %r" % e)


    def __fetch_clusterinfo(self):
        while True:
            try:
                gris, port = self.gris_q.get(False) # non-blocking
            except Queue.Empty:
                break 
            try:
                timestamp = time.time()
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
            ng.pickle_init()

            with self.t_lock:    
                proc_time = time.time() - timestamp
                self._cluster_queues[cluster_name] = queues_dict  
                self._cluster_jobs[cluster_name] = jobs_list
                self._cluster_allowed_users[cluster_name] = allowed_user_list
                self._clusters[cluster_name]=ng 
                if self.whitelist and self.whitelist.has_key(ng.gris_server):
                    self.whitelist[ng.gris_server].append(proc_time)
 
    def _populate_clusterinfo(self):

        # shared among threads...
        self._clusters = dict()
        self._cluster_queues = dict()
        self._cluster_jobs = dict()
        self._cluster_allowed_users = dict() 
        
        # some init values
        self.dbase['cluster_queues'] =self._cluster_queues
        self.dbase['cluster_jobs'] = self._cluster_jobs        
        self.dbase['cluster_allowed_users'] = self._cluster_allowed_users        
     
        for n in xrange(GrisCache.THREAD_LIMIT):
            t = threading.Thread(target=self.__fetch_clusterinfo)
            t.start()
            while threading.activeCount() > 1:
                time.sleep(2)

        self.dbase['cluster_queues'] =self._cluster_queues
        self.dbase['cluster_jobs'] = self._cluster_jobs        
        self.dbase['cluster_allowed_users'] = self._cluster_allowed_users        
        self.dbase['clusters'] = self._clusters 
   
   
    def _populate_userinfo(self):
        user_jobs = dict()
        user_cluster_queues = dict()

        for cluster_name,cluster in self.dbase['clusters'].iteritems():
            queues_dict = self.dbase['cluster_queues'][cluster_name]
            cluster_jobs = self.dbase['cluster_jobs'][cluster_name]

            for j in cluster_jobs:
                owner = j.get_globalowner()
                # make sure 'owner' is a real user ( user info might be disrupted)
                if owner == None or owner == 'None':
                    owner ="<user_info_disrupted>"                    

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
                    self.log.debug("%s not in %r" % (owner, job_q.get_allowed_users()))
                else:
                    if not user_jobs[owner].has_key(status):
                        user_jobs[owner][status] = list()
                    user_jobs[owner][status].append(j)
             
            # poplulate user_cluster_qeueus 'user_cluster_queues' - dict(user_dn= dict(cluster_name=[<queue_obj>,],...),...) 
            for q in queues_dict.values():
                for user in q.get_allowed_users():
                    if not user_cluster_queues.has_key(user):
                        user_cluster_queues[user] = dict({cluster_name:list()})
                    if not user_cluster_queues[user].has_key(cluster_name):
                        user_cluster_queues[user][cluster_name] = list()
                    user_cluster_queues[user][cluster_name] = q 
                    
        self.dbase['user_cluster_queues'] = user_cluster_queues
        self.dbase['user_jobs'] = user_jobs

    
    def _populate_statistics(self):
        """ collects statistics about user, jobs and clusters. """
        # XXX user statistics skipped for the moment as it's not 
        #     yet clear what we want to evaluate there.
        # XXX keep track of orphans
        
        gstats = NGStats('smscg','grid')  
        
        for cluster_name in self.dbase['cluster_queues'].keys():
            cstats = NGStats(cluster_name,'cluster')
            # cluster statistics
            cl = self.dbase['clusters'][cluster_name]
            for attr_name in NGStats.CSTATS_ATTRS:
                fct_sig ="cl.get_%s()" % attr_name
                cstats.set_attribute(attr_name,eval(fct_sig))
                gstats.set_attribute(attr_name,eval(fct_sig) + gstats.get_attribute(attr_name))
            # collect queue statistics
            for qname in self.dbase['cluster_queues'][cluster_name].keys():
                q = self.dbase['cluster_queues'][cluster_name][qname]
                qstats = NGStats(q.get_name(),'queue') 
                for attr_name in NGStats.QSTATS_ATTRS:
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
    
    def _populate_grisspecific(self):
        self.dbase['giis_proctime'] = self.giis_proctime
        self.dbase['gris_blacklist'] = self.blacklist
        self.dbase['gris_whitelist'] = self.whitelist
        
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
        self.dbase['timestamp'] = time.ctime()
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
