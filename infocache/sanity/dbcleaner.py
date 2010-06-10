"""
Cleans up old records in db
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="08.01.2010"
__version__="0.1.0"

import logging
import time
from datetime import datetime
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR
from sqlalchemy import orm

import infocache.db.mon_meta as mon_meta
import infocache.db.ng_schema as schema
from infocache.rrd.jobs import Jobs



class Cleanex(object):

    FETCHED_RECORD_AGE = 3600 * 12  # max age of db records of jobs that got fetched


    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.Session = orm.scoped_session(mon_meta.Session)
        self.set_last_query_time(datetime.utcnow())
        self.log.debug("Initialization finished")
   
    def set_age_threshold(self,threshold):
        self.age_threshold = threshold


    def set_last_query_time(self,t):
        self.last_query_time = t
 
    def check_clusters(self):
        self.log.debug("Checking for expired cluster records")
        session = self.Session()
        query = session.query(schema.Cluster)
        
        clusters= query.filter(AND(schema.Cluster.db_lastmodified <= self.last_query_time,
            schema.Cluster.status == 'active')).all()
        
        for cluster in clusters:
            self.log.debug("Deactivating cluster '%s'" % cluster.hostname)
            cluster.status = 'inactive'
            cluster.db_lastmodified = datetime.utcnow()
            for q in session.query(schema.Queue).filter_by(hostname=cluster.hostname).all():
                q.status = 'inactive'
                q.db_lastmodified = datetime.utcnow()
            for entry in session.query(schema.UserAccess).filter_by(hostname=cluster.hostname).all():
                session.delete(entry)
        session.flush()
        session.commit()
    
    def check_queues(self):
        self.log.debug("Checking for expired queue records")
        session = self.Session()
        
        query = session.query(schema.Queue)
        
        queues= query.filter(AND(schema.Queue.db_lastmodified <= self.last_query_time,
            schema.Queue.status == 'active')).all()
        
        for q in queues:
            self.log.debug("Deactivating queue '%s' of cluster '%s'" % (q.name,q.hostname))
            q.status = 'inactive'
            q.db_lastmodified = datetime.utcnow()
            for entry in session.query(schema.UserAccess).filter(AND(schema.UserAccess.hostname==q.hostname, 
                schema.UserAccess.queuename==q.name)).all():
                session.delete(entry)
        session.flush()
        session.commit()

    def check_users(self):
        pass

    def check_jobs(self):

        # 1.) remove db records of jobs that got fetched
        session = self.Session()
 
        fetched_before = datetime.utcfromtimestamp(time.time() - Cleanex.FETCHED_RECORD_AGE)
        
        fjobs = session.query(schema.Job).filter(AND(schema.Job.db_lastmodified<=fetched_before, 
                    OR(schema.Job.status == 'FIN_FETCHED', 
                        schema.Job.status == 'FLD_FETCHED',
                        schema.Job.status == 'KIL_FETCHED'))).all()

        if fjobs:
            self.log.info("Removing %d jobs from db that got fetched." % len(fjobs)) 
            for j in fjobs:
                session.delete(j)
            session.flush()
            session.commit()
        
        # 2.) remove records that got deleted (and not fetched)
 
        djobs = session.query(schema.Job).filter(AND(schema.Job.db_lastmodified<=fetched_before, 
                    OR(schema.Job.status == 'FIN_DELETED', 
                        schema.Job.status == 'FLD_DELETED',
                        schema.Job.status == 'KIL_DELETED'))).all()

        if djobs:
            self.log.info("Removing %d jobs from db that got not fetched." % len(djobs)) 
            for j in djobs:
                session.delete(j)
            session.flush()
            session.commit()
      

        # 3.) remove 'LOST' records.
        ljobs = session.query(schema.Job).filter(AND(schema.Job.completiontime<=fetched_before, 
                    schema.Job.status == 'LOST')).all() 
        if ljobs:
            self.log.info("Removing %d jobs from db in state LOST." % len(ljobs)) 
            for j in ljobs:
                session.delete(j)
            session.flush()
            session.commit()
 


