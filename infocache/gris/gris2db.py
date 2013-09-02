"""
The Gris2db class polls list of GRIS'es and populates
a database with results from poll. The DB is used by 
the GridMonitor to display the current state of the Grid.
"""
from __future__ import with_statement

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "10.04.2011"
__version__ = "0.3.0"

import logging
import time
import cPickle 
import Queue
from  threading import Lock, Thread
from datetime import datetime
from sqlalchemy import and_ as AND

from arclib import GetClusterInfo
from arclib import GetClusterJobs


from infocache.db import meta, schema
from infocache.db.cluster import ClusterMeta
from infocache.errors.db import Input_Error
from infocache.gris.statistics import NGStats
from infocache.gris.access import ClusterAccess

class Gris2db(object):
    
    THREAD_LIMIT = 18       # number of GRIS'es that will be queried in parallel
    BLACK_COUNTER = 5      # number of query cylces a cluster is blacklisted
    USER_UPDATE_PERIOD = 7200 # periodicity for updating user access lists in DB in seconds

    JOB_FIN_STATES = ['LOST',
                'FIN_DELETED',
                'FLD_DELETED',
                'KIL_DELETED',
                'FIN_FETCHED',
                'KIL_FETCHED',
                'FLD_FETCHED']  # Job states in DB considered final

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.processing_q = Queue.Queue(0)  # no limit to queue, the queue holds the GRISes
                                            # that will be queried + their insertion_time
        self.qlock = Lock()                 # processing queue lock
        self.proc_hosts = []                # keeps track of host(names) in processing_q
        self.blacklist = {}                 # cluster blacklist
        self.block = Lock()                 # blacklist lock
        self.last_cycle_meta = []           # metatdata about last/previous run, used to see
                                            # whether new clusters/queues got added/removed
        self.last_users_update = time.time() - Gris2db.USER_UPDATE_PERIOD
        self.stop_threads = False
        self.log.debug("Initialization finished")


    def _is_cluster_blacklisted(self, hostname):
        """ check whether cluster is blacklisted. If
            so blacklist-counter will be decreased

            returns True - if blacklisted.
                    False - else
        """
        blacklisted = False

        self.block.acquire()
        if self.blacklist.has_key(hostname):
            if self.blacklist[hostname] == 0:
                self.blacklist.pop(hostname) 
            else:
                self.blacklist[hostname] -= 1
                blacklisted = True
        self.block.release()
        return blacklisted 


    def _add_cluster2blacklist(self, hostname):
        """ black-lists a cluster. A cluster is
            only blacklisted on unexpected behavior (that's 
            different from setting the status to 'inactive'.
         """
        self.block.acquire()
        if not self.blacklist.has_key(hostname):
            self.blacklist[hostname] = Gris2db.BLACK_COUNTER
        self.block.release()
        self.log.warn('Cluster %s has been blacklisted' % hostname)
        
           
    def _cache_jobs(self, gris_url):
        """ caches jobs of specified cluster. Returns
            time it took.
        """
        session = meta.Session()
        timestamp = time.time()
        try:
            arc_jobs = GetClusterJobs(gris_url)
            arc_job_ids = list()            

            for job in arc_jobs:
                try:
                    _new_db_job = schema.NGJob(job) # easiest way to update all job entries in db 
                except: # no handling
                    self.log.error("Job %s will be ingored as it will not fit in db schema." % job.id)
                    continue

                arc_job_ids.append(job.id)
                
                db_job = session.query(schema.NGJob).filter_by(global_id=job.id).first()

                if not db_job: # case: new job
                    session.add(_new_db_job)
                elif db_job.status in Gris2db.JOB_FIN_STATES: # case: final db state -> don't touch
                    continue
                elif job.status == 'DELETED': 
                    if db_job.status in ['FINISHED', 'KILLED', 'FAILED']:
                        if (db_job.sessiondir_erase_time >= datetime.utcfromtimestamp(0)) and \
                            (db_job.sessiondir_erase_time <= datetime.utcnow()): # not feched
                            suffix = '_DELETED'
                        else:
                            suffix = '_FETCHED'
                        if db_job.status == 'FINISHED':
                            _new_db_job.status = 'FIN_' + suffix
                        elif db_job.status == 'FAILLED':
                            _new_db_job.status = 'FLD_' + suffix
                        elif db_job.status == 'KILLED':
                            _new_db_job.status = 'KIL_' + suffix
                    else: # not in any final state
                        _new_db_job.status = 'LOST'
                    session.merge(_new_db_job)
                else: # case db_job non-final, arc_job not DELETED -> upate to new state
                    session.merge(_new_db_job)
            
            # update db jobs that are not anymore advertized by the Grid infosys
            for db_job in session.query(schema.NGJob).filter(
                AND(schema.NGJob.cluster_name == gris_url.Host(),
                    schema.NGJob.status != 'DELETED',
                    schema.NGJob.status != 'LOST',
                    schema.NGJob.status != 'FIN_FETCHED',
                    schema.NGJob.status != 'FIN_DELETED',
                    schema.NGJob.status != 'KIL_FETCHED',
                    schema.NGJob.status != 'KIL_DELETED',
                    schema.NGJob.status != 'FLD_FETCHED',
                    schema.NGJob.status != 'FLD_DELETED')).all():
                if db_job.global_id not in arc_job_ids: # job got fetched
                    if db_job.status == 'FINISHED':
                        db_job.status = 'FIN_FETCHED'
                    elif db_job.status == 'FAILLED':
                        db_job.status = 'FLD_FETCHED'
                    elif db_job.status == 'KILLED':
                        db_job.status = 'KIL_FETCHED'
                    else:
                        db_job.status = 'LOST'
                    #db_job.db_lastmodified = datetime.utcnow()
                    session.add(db_job)
                
            session.commit()
        except Input_Error, er:
            self.log.error("Could not insert jobs for cluster %s into db, got %s. Rolling back" % \
                    (gris_url, er.message))
            session.rollback() 
        except Exception, er2:
            self.log.error("Could not insert jobs cluster %s into db, got %r. Rolling back" % \
                    (gris_url.Host(), er2))
            session.rollback() 
        finally:
            return time.time() - timestamp

    def _cache_cluster_info(self, gris_url):
        """ 'thread-save' """
        if not self._is_cluster_blacklisted(gris_url.Host()):
            session = meta.Session() 
            try:

                hostname = gris_url.Host()
                    

                timestamp = time.time()
                arc_cluster = GetClusterInfo(gris_url)
                arc_queues = arc_cluster.queues

                cl_meta = ClusterMeta()
                cl_meta.set_response_time(time.time() - timestamp)
                cl_meta.set_processing_time(self._cache_jobs(gris_url))
                cl_meta.whitelisting()
            
           
                db_cluster = schema.NGCluster(arc_cluster)
                self.log.debug("Updading cluster: %s" % db_cluster.get_name())
                db_cluster.set_metadata(cl_meta)
                db_cluster = session.merge(db_cluster)
     
                for q in arc_queues:
                    db_queue = schema.NGQueue(q, arc_cluster.hostname)
                    db_queue.db_lastmodified = datetime.utcnow()
                    db_queue = session.merge(db_queue)
                
                session.commit()

            except Input_Error, er:
                self.log.error("Could not insert cluster %s into db, got %s. Rolling back" % \
                        (gris_url, er.message))
                session.rollback() 
                self._add_cluster2blacklist(arc_cluster.hostname)
            except Exception, er2:
                self.log.error("Could not insert cluster %s into db, got %r. Rolling back" % \
                        (gris_url.Host(), er2))
                
                self._add_cluster2blacklist(arc_cluster.hostname)
                session.rollback() 
                 
    def _query_grises(self):
        """
        Fetches information about gris'es (clusters), about the queues
        and the jobs.
        """
        
        while not self.stop_threads:
            try:
                gris_url, insert_time = self.processing_q.get(True, 30) # blocking
            except Queue.Empty:
                continue

            # start doing job
            self.log.debug("Current queueing time: %s seconds" % (time.time() - insert_time))
            self._cache_cluster_info(gris_url)

            # end doing job

            self.qlock.acquire()
            host = gris_url.Host()
            self.proc_hosts.remove(host)
            self.qlock.release() 

    def _basic_housekeeping(self, active_clusters):
        """ Do some basic 'cleanup' of DB entries. Should
            be called once per processing cycle.
            active_gris_list -- list of currently active clusters. 
        """
        self.block.acquire()
        blacklisted = self.blacklist.keys()
        self.block.release()
        
        self.log.debug("BLACKLISTED: %r" % blacklisted)

        session = meta.Session()
        change = False
        for cluster in session.query(schema.NGCluster).filter_by(status='active').all():
            self.log.debug("checking %s" % cluster.hostname)
            if (cluster.hostname not in active_clusters):
                cluster.status = 'inactive'
                change = True
                for q in session.query(schema.NGQueue).filter_by(hostname=cluster.hostname).all():
                    q.status = 'inactive'
                    q.db_lastmodified = datetime.utcnow()
                    session.add(q) 
                self.log.info("Deactivating cluster %s" % cluster.hostname)
                session.add(cluster)
                self.log.info("Removing users from cluster access list")
                session.query(schema.UserAccess).filter_by(hostname=cluster.hostname).\
                    delete(synchronize_session='fetch')
            if cluster.hostname in blacklisted:
                cluster.blacklisted = True
                change = True
                session.add(cluster)
        if change:
            session.commit()
    
    def _populate_user_access(self, _active_clusters):
        """ Queries all clusters (respectivley their queues) 
            for the users that are advertized to have
            access. 
        """
        now = time.time() 

        if (now - self.last_users_update) >= Gris2db.USER_UPDATE_PERIOD:
            self.last_cycle_meta = _active_clusters[:]
            
            db_time_thresh = datetime.utcfromtimestamp(now - 300) # to be on the save side... 
            self.last_users_update = now
            
            self.log.debug("Repopulating user access lists for all active clusters")
            for hostname in _active_clusters:
                try:
                    user_access = ClusterAccess(hostname)
                    user_access.write_allowed_users2db()
                except: # XXX handle exception
                    pass 
            # remove all old entries
            self.log.debug("Removing old user access lists") 
            session = meta.Session()
            n = session.query(schema.UserAccess).\
                        filter(schema.UserAccess.db_lastmodified <= db_time_thresh).\
                        delete(synchronize_session='fetch')
            self.log.debug("Removed %d access entries for entire Grid" % n)
        else:
            # else just those that were added /removed since last run
            active_clusters = set(_active_clusters)
            old_clusters = set(self.last_cycle_meta)
            self.last_cycle_meta = _active_clusters[:]

            new_clusters = active_clusters - old_clusters
            rm_clusters = old_clusters - active_clusters

            for hostname in new_clusters:
                try:
                    user_access = ClusterAccess(hostname)
                    user_access.write_allowed_users2db()
                except: # XXX handle exception
                    pass 
            
            if rm_clusters:
                session = meta.Session()
                for hostname in rm_clusters:
                    n = session.query(schema.UserAccess).\
                        filter_by(hostname=hostname).delete(synchronize_session='fetch')
                    self.log.debug("Removed %d access entries for host %s" % \
                        (n, hostname))
                session.commit()
            

    
    def add_urls2queue(self, url_list):
        """ Adding list of GRIS URLs to the processing queue. 
            Duplicate entries are avoided.

            url_list -- list of cluster/Gris URLs of arclib.URL type
        """
        active_clusters = list()
        self.qlock.acquire()
        for url in url_list:
            host = url.Host()
            active_clusters.append(host)
            self.log.debug("Inserting GRIS %s in processing queue", host)
            self.block.acquire()
            black_listed_clusters = self.blacklist.keys() 
            self.block.release()
            if (host not in self.proc_hosts) and (host not in black_listed_clusters):
                self.proc_hosts.append(host)
                self.processing_q.put((url, time.time()))
        self.qlock.release()
            
        self.log.debug("Starting basic housekeeping")
        self._basic_housekeeping(active_clusters)
        
        self.log.debug("Collecting current usage statistics")
        self._populate_statistics()

        self.log.debug("Populating User Access lists.")
        self._populate_user_access(active_clusters)


        

        
    def stop(self):
        """ Stop all processing."""
        self.stop_threads = True

    def start(self):
        """ start processing. """
        self.stop_threads = False
        
        for n in xrange(Gris2db.THREAD_LIMIT):
            tr = Thread(target=self._query_grises)
            tr.start()


    def _populate_statistics(self):
        """ collects statistics about clusters and queues """
        # XXX currently we only support stats for one grid. Name set to SMSCG
        #     maybe we need to change this

        self.log.debug("Start populating grid usage statistics")
        gstats = NGStats('SMSCG', 'grid')
        
        session = meta.Session()
        query = session.query(schema.NGCluster)
        try:
            for cluster in query.filter_by(status='active').all():
                cstats = NGStats(cluster.hostname, 'cluster')

                for attr_name in NGStats.CSTATS_ATTRS:
                    cval = eval("cluster.%s" % attr_name)
                    cstats.set_attribute(attr_name, cval)
                    gstats.set_attribute(attr_name, cval + gstats.get_attribute(attr_name))

                for queue in cluster.queues:                
                    if queue.status != 'active':
                        continue
                    qstats = NGStats(queue.name, 'queue')

                    for attr_name in NGStats.QSTATS_ATTRS:
                        qval = eval("queue.%s" % attr_name)
                        qstats.set_attribute(attr_name, qval)
                        cstats.set_attribute(attr_name, qval + cstats.get_attribute(attr_name))
                        gstats.set_attribute(attr_name, qval + gstats.get_attribute(attr_name))

                    qstats.pickle_init()
                    cstats.add_child(qstats)
                cstats.pickle_init()
                gstats.add_child(cstats)
        
    
            gstats.pickle_init()
            dbgstats = schema.GridStats('SMSCG')
            dbgstats.pickle_object = cPickle.dumps(gstats)
            dbgstats.db_lastmodified = datetime.utcnow()
            dbgstats = session.merge(dbgstats)
            session.commit()
        except Exception, e:
            self.log.error("Unexpected error: %r", e)
            session.rollback()
            self.log.info("Rolled back session.")




