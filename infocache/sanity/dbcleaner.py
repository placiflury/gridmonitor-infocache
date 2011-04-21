"""
Cleans up old records in db
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "08.01.2010"
__version__ = "0.1.0"

import logging
import time
from datetime import datetime
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR

from infocache.db import meta, schema

class Cleanex(object):

    FETCHED_RECORD_AGE = 3600 * 24  # max age of db records of jobs that got fetched
    INACTIVE_CLUSTER_MAX_AGE = 3600 * 24 * 14 # max age of inactive cluster before removal (2 weeks)

    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.log.debug("Initialization finished")
   
    def check_clusters(self):
        self.log.debug("Checking for expired cluster records")
        session = meta.Session()
        query = session.query(schema.NGCluster)
        
        # remove old inactive clusters
        inactive_since = datetime.utcfromtimestamp(time.time() - Cleanex.INACTIVE_CLUSTER_MAX_AGE)
        inactive_clusters = query.filter(AND(schema.NGCluster.db_lastmodified <= inactive_since,
            schema.NGCluster.status == 'inactive')).all()

        for cluster in inactive_clusters:
            self.log.info("Removing inactivate cluster '%s'" % cluster.hostname)
            session.delete(cluster)

        if inactive_clusters:
            session.commit()

        
    def check_queues(self):
        pass
        
    def check_users(self):
        pass

    def check_jobs(self):

        # 1.) remove db records of jobs that got fetched
        session = meta.Session()
 
        fetched_before = datetime.utcfromtimestamp(time.time() - Cleanex.FETCHED_RECORD_AGE)
        
        fjobs = session.query(schema.NGJob).filter(AND(schema.NGJob.db_lastmodified <= fetched_before, 
                    OR(schema.NGJob.status == 'FIN_FETCHED', 
                        schema.NGJob.status == 'FLD_FETCHED',
                        schema.NGJob.status == 'KIL_FETCHED'))).delete()
        
        if fjobs > 0:
            self.log.info("Removing %d jobs that got fetched (from db)." % fjobs) 

        # 2.) remove records that got deleted (and not fetched)
 
        djobs = session.query(schema.NGJob).filter(AND(schema.NGJob.db_lastmodified <= fetched_before, 
                    OR(schema.NGJob.status == 'FIN_DELETED', 
                        schema.NGJob.status == 'FLD_DELETED',
                        schema.NGJob.status == 'KIL_DELETED'))).delete()
        if djobs > 0:
            self.log.info("Removing %d 'DELETED' jobs that got from db." % djobs) 

        # 3.) remove 'LOST' records.
        #ljobs = session.query(schema.NGJob).filter(AND(schema.NGJob.completion_time <= fetched_before, 
        ljobs = session.query(schema.NGJob).filter(AND(schema.NGJob.db_lastmodified <= fetched_before, 
                    schema.NGJob.status == 'LOST')).delete()
        if ljobs > 0:
            self.log.info("Removing %d 'LOST' jobs from db." % ljobs) 

        # 4.) remove job's we did not track properly (e.g. which were in a final
        # state when we started populating the database etc.
        
        old_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.db_lastmodified <= fetched_before, 
                    schema.NGJob.status == 'DELETED')).delete()
        if old_jobs > 0:
            self.log.info("Removing %d 'old' jobs from db." % old_jobs) 
        
        
 

    def main(self):
        self.check_clusters()
        self.check_jobs() 
