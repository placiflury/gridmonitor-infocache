"""
The Gris2db class is used to store the  
information from the grid information system into a local database. 
Notice, it will only populate database with jobs that were submitted since
the giis2db daemon was started.
"""
from __future__ import with_statement

__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "10.12.2009"
__version__ = "0.2.0"

import logging, pickle
import time, threading, Queue
from datetime import datetime
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR
from sqlalchemy import orm

from gris import *
from statistics import * 

from infocache.errors.gris import *
from infocache.errors.stats import *
from infocache.voms.voms import * 
import infocache.db.meta as meta
import infocache.db.ng_schema as schema  

class Gris2db():
    
    THREAD_LIMIT = 3   # number of GRIS'es that will be queried in parallel
    TIMES = ['completiontime','proxyexpirationtime',\
            'sessiondirerasetime','submissiontime'] 
    DELAY = 180   # [seconds], how far back we query for 'new' jobs
    FINISHED_CHECK_CYCLE = 10  # every x cycle we check wether finished jobs get fetched
    ALLOWED_USERS_CHECK_CYCLE = 15  # every x cycle update allowed users

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.gris_q = Queue.Queue(0) # no limit to queue
        self.whitelist = dict()
        self.set_last_query_time(0)  # keeps time of last SUCCESSFUL update
        self.last_gris_refresh_time = None
        self.finished_jobs_check = Gris2db.FINISHED_CHECK_CYCLE  
        self.allowed_users_check = Gris2db.ALLOWED_USERS_CHECK_CYCLE  
        self.__populate_user_vo_map() # XXX call it every x hours
        self.Session = orm.scoped_session(meta.Session)
        self.log.debug("Initialization finished")

    def refresh_gris_list(self, grislist):
        for gris in grislist:
            self.gris_q.put(gris)
        self.last_gris_refresh_time = time.time()
 
    def set_last_query_time(self, t):
        self.last_query_time_iso= time.strftime('%Y%m%d%H%M%SZ', time.gmtime(t))
        self.last_query_time = datetime.utcfromtimestamp(t)
        self.last_query_time_save_iso = time.strftime('%Y%m%d%H%M%SZ', time.gmtime(t-Gris2db.DELAY)) 

    def get_last_query_time(self):
        return self.last_query_time

    def set_blacklist(self, blacklist):
        
        session = self.Session()
        for entry in session.query(schema.Grisblacklist).all():
            session.delete(entry)
        session.flush()
        session.commit()
        
        if blacklist:
            self.log.debug("Deactivation blacklist %s" % blacklist)
            for cluster_name, port  in blacklist:
                dbgris = schema.Grisblacklist()
                dbgris.hostname = cluster_name
                session.add(dbgris)
                
                self.log.debug("Deactivating cluster %s" % cluster_name)
                query = session.query(schema.Cluster)
                dbcluster= query.filter_by(hostname=cluster_name).first()
                if not dbcluster:
                    continue
                dbcluster.status = 'inactive'
                dbcluster.db_lastmodified = datetime.utcnow() 
                
                self.log.debug("Deactivating all queues of cluster %s" % cluster_name)
                for q in session.query(schema.Queue).filter_by(hostname=cluster_name).all():
                    q.status = 'inactive'
                    q.db_lastmodified = datetime.utcnow() 
                
                self.log.debug("Updating users access for cluster %s" % cluster_name)
    
                for entry in session.query(schema.UserAccess).filter_by(hostname=cluster_name).all():
                    session.delete(entry)
                self.log.debug("User access list updated") 
            try:
                session.flush() 
                session.commit()
                session.close()
            except Exception, e:
                self.log.error("%r" % e)

    def set_giis_proctime(self, proctime_dict):
        session = self.Session()
        self.log.debug("Resetting GIIS entries in db...") 
        for entry in session.query(schema.Giis).all(): 
            session.delete(entry)
        
        for hostname, t in proctime_dict.items():
            self.log.info("GIIS: %s processing time: %f" % (hostname, t))
            dbgiis = schema.Giis()
            dbgiis.hostname = hostname
            dbgiis.processing_time = t
            dbgiis.db_lastmodified = datetime.utcnow()
            session.add(dbgiis)
        session.flush()
        session.commit()
        session.close()

    
    
    def set_whitelist(self, whitelist):
        self.log.debug("Whitelist for clusters: %r" % whitelist.keys())
        self.whitelist= whitelist
   

    def _populate(self):
        """ Populates db with information that gets directly
            queried from the information system. 
        """
        try:
            session = self.Session()
            self.log.debug("created session %s" % session)
            self.__populate_gris_info()
            self.set_last_query_time(self.last_gris_refresh_time)
        except Exception, e:
            self.log.error("While populating db got '%r'" % e)
            self.log.info("Rolling back session and ignoring exception")
            session.rollback()
            self.log.info("Session rolled back")
        finally:
            self.log.debug("closing session %s" % session)
            session.close()
                 
    def __populate_gris_info(self):
        """
        Fetches information about gris'es (clusters), about the cluster's queues
        and the users that are allowed on the queues. For the later it creates 
        access entries. 
        All this information is stored in the (local) db.
        For each queue jobs are queried as well. 
        """
        session = self.Session()
        while True:
            try:
                gris, port = self.gris_q.get(False) # non-blocking
            except Queue.Empty:
                break 
            try:
                self.log.debug("Trying gris: %s" % gris)
                timestamp = time.time()
                ng = NGCluster(gris, port)
            except Exception, e: # we do not set cluster to inactive! -> done by other module
                self.log.error("Gris '%s', got:  %r " % (gris, e))
                continue
           
            cluster_name = ng.get_name()
            queues = ng.get_queues()
            
            for q in queues:
                qname = q.get_name()
                if self.allowed_users_check == 0:
                    allowed_user_list = list()  
                    for dn in q.get_allowed_users():
                        if dn in allowed_user_list:  # avoid duplicates 
                            continue
                        allowed_user_list.append(dn)
                        
                        # write/update users and user_access entries to/on db
                        query = session.query(schema.User)
                        dbuser = query.filter_by(DN=dn).first()
                        if not dbuser:
                            dbuser = schema.User()
                            dbuser.DN = dn
                            session.add(dbuser)
                        dbuser.db_lastmodified = datetime.utcnow()
                        session.flush()
                
                        query = session.query(schema.UserAccess)
                        access_entry = query.filter_by(hostname=cluster_name, queuename=qname, user=dn).first()
                        if not access_entry:
                            access_entry = schema.UserAccess()
                            access_entry.hostname = cluster_name
                            access_entry.queuename= qname
                            access_entry.user = dn
                            session.add(access_entry)
                        access_entry.db_lastmodified=datetime.utcnow()
                        session.flush()
                        session.commit()
                
                self.__populate_jobs(q, cluster_name)  # get the jobs

                proctime= time.time() - timestamp 

                # write/update queue to db
                q.pickle_init()
                query = session.query(schema.Queue)
                dbqueue = query.filter_by(hostname=cluster_name, name=qname).first()
                if not dbqueue:
                    dbqueue = schema.Queue() 
                    dbqueue.name = qname
                    dbqueue.hostname = cluster_name
                    session.add(dbqueue)
                    self.allowed_users_check = -1 # on next cycle allowed_users will be populated
                dbqueue.status='active'
                dbqueue.db_lastmodified=datetime.utcnow()
                dbqueue.pickle_object = pickle.dumps(q)
                session.flush()
                session.commit()

            # write/update cluster to db
            ng.pickle_init()
            query = session.query(schema.Cluster)
            dbcluster= query.filter_by(hostname=cluster_name).first()
            if not dbcluster:
                self.log.info("Cluster %s created in DB." % cluster_name)
                dbcluster = schema.Cluster()
                dbcluster.hostname = cluster_name 
                session.add(dbcluster)
                self.allowed_users_check = -1 # on next cycle allowed_users will be populated
            dbcluster.alias = ng.get_alias()
            dbcluster.status = 'active'
            dbcluster.pickle_object = pickle.dumps(ng)
            dbcluster.db_lastmodified = datetime.utcnow() 
            dbcluster.processing_time = proctime
            self.log.info("GRIS: %s processing time: %f" % (cluster_name, proctime))
            if self.whitelist.has_key(cluster_name):
                dbcluster.response_time = self.whitelist[cluster_name]
            session.flush() 
            session.commit()

        
    def __populate_jobs(self, q, hostname):
          
        session = self.Session()
        processing_start_time = datetime.utcnow()
        qname = q.get_name() 
        # A -- every cycle
        # A1.) get jobs submitted since last query (from gris) [keep them
        # in memory (or age timestamp), so we don't query things twice]

        filter = q.set_maxsubtime_status_filter(subtime=self.last_query_time_save_iso, 
            status='DELETED', negation_of_status='True')
        gris_latest_jobs = q.get_jobs(filter)

        self.log.debug("Got %d new  jobs on (%s:%s)" % (len(gris_latest_jobs), hostname, qname))

        for grisjob in gris_latest_jobs:
            id = grisjob.get_globalid()
            query = session.query(schema.Job)
            query= query.filter_by(globalid=id)
            dbjob = query.first()
            if not dbjob:
                dbjob = schema.Job()
                dbjob.globalid = id
                session.add(dbjob)

            for attr in grisjob.get_attribute_names():
                attr_values = grisjob.get_attribute_values(attr)
                if attr_values: # we only go for the first value if any
                    fval = attr_values[0]
                else:
                    fval = None
                # re-mapping of values
                if attr == 'execcluster':
                    dbjob.cluster_name = fval
                elif attr == 'execqueue':
                    dbjob.queue_name = fval
                else:
                    # convert times (they are already in UTC)
                    if attr in Gris2db.TIMES and fval:
                        t = time.strptime(fval, '%Y%m%d%H%M%SZ')
                        fval = datetime.fromtimestamp(time.mktime(t))
                    assignment = "dbjob.%s = fval" % attr
                    exec(assignment)
            dbjob.db_lastmodified = datetime.utcnow()
            session.flush()
            
        # A2.) for all jobs in local db that are not in a final state, check
        # whether status changed (don't check jobs we just fetched)
        query = session.query(schema.Job)
        dbjobs = query.filter(AND(schema.Job.queue_name==qname, schema.Job.cluster_name==hostname,
            schema.Job.submissiontime<=self.last_query_time, 
            schema.Job.status!='FINISHED', 
            schema.Job.status!='FAILED', 
            schema.Job.status!='KILLED',
            schema.Job.status!='KIL_DELETED',
            schema.Job.status!='FIN_DELETED',
            schema.Job.status!='FLD_DELETED',
            schema.Job.status!='KIL_FETCHED',
            schema.Job.status!='FIN_FETCHED',
            schema.Job.status!='FLD_FETCHED',
            schema.Job.status!='LOST',
            schema.Job.status!='DELETED')).all()
            
        self.log.debug("Querying for %d (non-final) jobs (%s:%s)" % (len(dbjobs), hostname, qname))
        
        for dbjob in dbjobs:
            grisjob =q.get_job(dbjob.globalid)
            if grisjob: #  check whether any of the entries did change
                for attr in grisjob.get_attribute_names():
                    attr_values = grisjob.get_attribute_values(attr)
                    if attr_values:
                        fval = attr_values[0]
                    else:
                        fval = None
                    if attr == 'execcluster':
                        if dbjob.cluster_name != fval:
                            dbjob.cluster_name = fval
                            dbjob.db_lastmodified = datetime.utcnow()
                        continue
                    elif attr == 'execqueue':
                        if dbjob.queue_name != fval:
                            dbjob.queue_name = fval
                            dbjob.db_lastmodified = datetime.utcnow()
                        continue
                    else:
                        # convert times (they are already in UTC)
                        if attr in Gris2db.TIMES and fval:
                            t = time.strptime(fval, '%Y%m%d%H%M%SZ')
                            fval = datetime.fromtimestamp(time.mktime(t))
                        oldval = eval('dbjob.%s' % attr)
                        if oldval != fval:
                            assignment = "dbjob.%s = fval" % attr
                            exec(assignment)
                dbjob.db_lastmodified = datetime.utcnow()
                session.flush()
            else:  # XXX could job have been feched? (check!!!)
                """
                Jobs landing here have slipped through. We take the *assumption*
                that these jobs may have executed successfully (exepct those that 
                were in KILLING state).
                """
                self.log.info("A2: Job %s (%s) could not be tracked anymore." %
                (dbjob.globalid, dbjob.status))

                if dbjob.status == 'KILLING':
                    dbjob.status = 'KIL_FETCHED'
                else:
                    dbjob.status = 'LOST'   # XXX improve

                dbjob.completiontime = datetime.utcnow()
                dbjob.db_lastmodified = datetime.utcnow()
                session.flush()

        # A3.) all non-fetched jobs in final state, check whether they got erased 
        # i.e. if they have been moved to state DELETED
        query = session.query(schema.Job)
        dbjobs = query.filter(AND(schema.Job.queue_name==qname, schema.Job.cluster_name==hostname,
            schema.Job.sessiondirerasetime < self.last_query_time, 
            OR(schema.Job.status=='FINISHED', schema.Job.status=='FAILED', 
            schema.Job.status=='KILLED'))).all()
        if dbjobs: 
            self.log.debug("Querying for %d finished jobs, that should have been moved to DELETED state on (%s:%s)." 
                % (len(dbjobs), hostname, qname))
            for dbjob in dbjobs :
                grisjob =q.get_job(dbjob.globalid)
                if grisjob: 
                    status = grisjob.get_status() # only update status -> no further info loss about job
                    if status == 'DELETED':
                        if dbjob.status == 'FINISHED':
                            dbjob.status = 'FIN_DELETED'  # special state 
                        elif dbjob.status == 'FAILED':
                            dbjob.status = 'FLD_DELETED'  # special state 
                        elif dbjob.status == 'KILLED':
                            dbjob.status = 'KIL_DELETED'  # special state 
                        session.flush()
                    elif status != dbjob.status: # job could have been rerun
                        self.log.debug("Found job that got rerun")
                        dbjob.status = status
                        dbjob.db_lastmodified = datetime.utcnow()
                        session.flush()
                else:  # job not anymore found
                    self.log.info("A3: Job %s (%s) could not be tracked anymore." %
                     (dbjob.globalid, dbjob.status))
                    dbjob.status = 'LOST'   # XXX improve
                    dbjob.complentiontime = datetime.utcnow()
                    dbjob.db_lastmodified = datetime.utcnow()
                    session.flush()

        # A4.) check whether final state jobs got fetched. If a job got fetched
        #      it simply does not show up in information system.
        if self.finished_jobs_check == 0:
            query = session.query(schema.Job)
            dbjobs = query.filter(AND(schema.Job.queue_name==qname, schema.Job.cluster_name==hostname,
                schema.Job.db_lastmodified <= processing_start_time, 
                OR(schema.Job.status=='FINISHED', schema.Job.status=='FAILED', 
                schema.Job.status=='KILLED'))).all()
            if dbjobs:
                self.log.debug("Querying for %d final state jobs on (%s:%s)" % (len(dbjobs), hostname, qname))
                for dbjob in dbjobs:
                    grisjob =q.get_job(dbjob.globalid)
                    if not grisjob: # job got fetched
                        if dbjob.status == 'FINISHED':
                            dbjob.status = 'FIN_FETCHED'  # special state 
                        elif dbjob.status == 'FAILED':
                            dbjob.status = 'FLD_FETCHED'  # special state 
                        elif dbjob.status == 'KILLED':
                            dbjob.status = 'KIL_FETCHED'  # special state 
                        session.flush()
                    elif grisjob.get_status() != dbjob.status: # job could have been rerun
                        self.log.debug("Found job that got rerun")
                        dbjob.status = grisjob.get_status()
                        dbjob.db_lastmodified = datetime.utcnow()
                        session.flush()
        session.commit()
                
 
    def populate(self):
        if self.finished_jobs_check >=  Gris2db.FINISHED_CHECK_CYCLE:
            self.finished_jobs_check = 0 
        if self.allowed_users_check >=Gris2db.ALLOWED_USERS_CHECK_CYCLE:       
            self.allowed_users_check = 0
 
        if self.gris_q.qsize() < Gris2db.THREAD_LIMIT:
            num_threads = self.gris_q.qsize()
        else:
            num_threads = Gris2db.THREAD_LIMIT
         
        for n in xrange(num_threads):
            t = threading.Thread(target=self._populate)
            t.start()
            
        while threading.activeCount() > 1: 
            time.sleep(2)
        
        self._populate_statistics()
        self.finished_jobs_check +=1
        self.allowed_users_check +=1
 

    def __populate_user_vo_map(self):
        """
        XXX: Call this every x hours:

        """
        # fetch all users of VOs  -> dict( DN of user -> VOs she's member of)
        user_dict = dict()
        try:
            voms = VOMSConnector()
        except VOMSException, e:
            voms = None
            self.log.error("Could not connect to VOMS, got %r" % e)
            self.user_vo_map= dict()
            return

        vo_list = voms.get_vos()
        self.log.info("Got VO-list: %s" % vo_list)

        for vo in vo_list:
            ulist = voms.listUsers(vo)
            for user_record in ulist:
                user = user_record._DN
                if not user_dict.has_key(user):
                    user_dict[user]=list()
                if  user_dict[user].count(vo) < 1:
                    user_dict[user].append(vo)
        self.user_vo_map = user_dict

    
    def _populate_statistics(self):
        """ collects statistics about clusters and queues """
        
        # XXX currently we only support stats for one grid. Name set to SMSCG
        #     maybe we need to change this

        # XXX populate VO usage

        self.log.debug("Start populating grid usage statistics")
        gstats = NGStats('SMSCG', 'grid')  
       
        # read fetch active clusters from db
        session = self.Session() 
        query = session.query(schema.Cluster)
        try:
            grid_vo_usage = dict()
            dbclusters = query.filter_by(status='active').all() 
            for dbcluster in dbclusters:
                cl = pickle.loads(dbcluster.pickle_object)
                cluster_name = dbcluster.hostname
                cstats = NGStats(cluster_name, 'cluster')
            
                for attr_name in NGStats.CSTATS_ATTRS:
                    fct_sig ="cl.get_%s()" % attr_name
                    cstats.set_attribute(attr_name, eval(fct_sig))
                    gstats.set_attribute(attr_name, eval(fct_sig) + gstats.get_attribute(attr_name))
                    
                query = session.query(schema.Queue)
                dbqueues = query.filter_by(hostname=cluster_name, status='active').all() 
               
             
                cluster_vo_usage = dict()
                for dbqueue in dbqueues:
                    q = pickle.loads(dbqueue.pickle_object)
                    qstats = NGStats(dbqueue.name, 'queue') 
                    for attr_name in NGStats.QSTATS_ATTRS:
                        fct_sig = "q.get_%s()" % attr_name
                        qstats.set_attribute(attr_name, eval(fct_sig))
                        cstats.set_attribute(attr_name, eval(fct_sig) + cstats.get_attribute(attr_name))   
                        gstats.set_attribute(attr_name, eval(fct_sig) + gstats.get_attribute(attr_name))   
                    # stats about VO usage   
                    query = session.query(schema.Job)
                    dbjobs = query.filter(AND(schema.Job.cluster_name == cluster_name,
                            schema.Job.queue_name==dbqueue.name,
                            schema.Job.completiontime>=(time.time()-86400))).all() # jobs of last 24 hours
                    queue_vo_usage = dict()
                    for dbjob in dbjobs:
                        owner = dbjob.globalowner
                        if not owner or not self.user_vo_map.has_key(owner): # orphaned job
                            continue
                        walltime = dbjob.usedwalltime
                        if not walltime:
                            continue
                        vlist = self.user_vo_map[owner] # get VOs user belongs to
                        vos = str(vlist)
                        if not queue_vo_usage.has_key(vos):
                            queue_vo_usage[vos] = dict(num_jobs=0, walltime=0)
                        queue_vo_usage[vos]['num_jobs'] +=1
                        queue_vo_usage[vos]['walltime'] += walltime
                        
                        if not cluster_vo_usage.has_key(vos):
                            cluster_vo_usage[vos] = dict(num_jobs=0, walltime=0)
                        cluster_vo_usage[vos]['num_jobs'] +=1
                        cluster_vo_usage[vos]['walltime'] += walltime
                        
                        if not grid_vo_usage.has_key(vos):
                            grid_vo_usage[vos] = dict(num_jobs=0, walltime=0)
                        grid_vo_usage[vos]['num_jobs'] +=1
                        grid_vo_usage[vos]['walltime'] += walltime

                    qstats.set_attribute("vo_usage", queue_vo_usage)
                    qstats.pickle_init()
                    cstats.add_child(qstats)
                cstats.set_attribute("vo_usage", cluster_vo_usage)
                cstats.pickle_init()
                gstats.add_child(cstats)
            gstats.set_attribute("vo_usage", grid_vo_usage)
            gstats.pickle_init() 
            
            # check whether stats object exists already, if so overwrite
            query= session.query(schema.GridStats)
            dbgstats = query.filter_by(gridname='SMSCG').first()

            if not dbgstats:
                dbgstats = schema.GridStats()
                dbgstats.gridname='SMSCG'
                session.add(dbgstats)
            dbgstats.pickle_object = pickle.dumps(gstats)
            dbgstats.db_lastmodified=datetime.utcnow()
            session.flush()
            session.commit()
        except Exception, e:
            self.log.error("OHA: %r", e)
            session.rollback()
        finally:
            session.close()

        



