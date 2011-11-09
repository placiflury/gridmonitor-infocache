"""
Queries NorduGrid's ldap-based information system (GRIS/GIIS) and stores 
queried data persistently in a (local) database.
"""

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "10.04.2011"
__version__ = "0.3.0"

import sys
#XXX change the following
sys.path.append('/opt/nordugrid/lib/python2.6/site-packages')
import arclib 

import time
import logging
from datetime import datetime

from daemon import Daemon
from gris.giis import NGGiis
from gris.gris2db import Gris2db
from errors.giis import GIISError

from db import meta, schema


class Giis2db(Daemon):
    """ Daemon process to either populate a database (cache)
        with snapshot of Grid as currently reported/advertized
        by the Grid information system.
        The same daemon might also be called simply for generating
        RRD plots from the cache (i.e. database entries)
    """

    DEFAULT_GIIS_PORT = 2135
    GIIS_REFRESH_PERIOD = 5     # every x cycles GIIS list gets repopulated
    BLACK_LIST_COUNTER = 10     # cycles a black-listed GRIS/GIIS is not checked.
                                

    def __init__(self, pidfile="/var/run/giis2db.pid", **kwargs):
        self.log = logging.getLogger(__name__)
        Daemon.__init__(self, pidfile)
        self.periodicity = kwargs['periodicity']
        self.top_giis_list = kwargs['top_giis_list']
        self.mds_vo_name = kwargs['mds_vo_name']
        
        self.giis_list = list()
        self.giis_response = dict()         # giis response times
        self.giis_blacklisted = dict()     # blacklisted giis (servers)
        self.gris_list = list()
        
        self.gris2db = Gris2db()        
        self.log.debug("Initialization finished")


    def change_state(self, state):
        """ Changing daemon state. """
        if state == 'start':
            self.log.info("starting daemon...")
            Daemon.start(self)
        elif state == 'stop':
            self.log.info("stopping daemon...")
            self.gris2db.stop()
            Daemon.stop(self)
            self.log.info("stopped")
        elif state == 'restart':
            self.log.info("restarting daemon...")
            self.gris2db.stop()
            Daemon.restart(self)

    def _populate_giis_list(self, giislist):
        """
        Populates/completes recursively list of GIISes, starting from given 
        giislist of type [(host,port,mds_vo_name), ...], which should
        contain higher order GIISes.
        """
        for host, port, mds_vo_name in giislist:
            if self.giis_blacklisted.has_key(host): 
                self.giis_blacklisted[host] -= 1
                if self.giis_blacklisted[host] <= 1:
                    self.giis_blacklisted.pop(host)
                continue
            try:
                timestamp = time.time()
                ng = NGGiis(host, mds_vo_name = mds_vo_name)
                self.giis_response[host] = time.time() - timestamp 
                self.giis_list.append((host, port, mds_vo_name))
                self._populate_giis_list(ng.get_giis_list())
            except GIISError:
                self.log.warn("GIIS %s %s (mds_vo_name=%s) not accessible" % (host, port, mds_vo_name))
                self.giis_blacklisted[host] = Giis2db.BLACK_LIST_COUNTER / Giis2db.GIIS_REFRESH_PERIOD
            except Exception, e:
                self.log.info("Got exception %r", e)
                self.giis_blacklisted[host] = Giis2db.BLACK_LIST_COUNTER / Giis2db.GIIS_REFRESH_PERIOD
            finally:
                ng.close()
            
    def _refresh_giis_list(self):
        """ refreshes GIIS servers list"""
        del(self.giis_list)
        self.giis_list = list()
        self.giis_response = dict()
        self._populate_giis_list(self.top_giis_list)

        timestamp = datetime.utcnow()
        session = meta.Session()
        for g_host, g_port, g_mds_vo_name in self.giis_list:
            self.log.debug(" %s %s %s" % (g_host, g_port, g_mds_vo_name))
            giis = schema.GiisMeta(g_host, g_port, g_mds_vo_name)
            giis.set_response_time(self.giis_response[g_host])
            session.merge(giis)
      
        _tmpl = list() 
        for g_host in self.giis_blacklisted.keys():
            self.log.debug("Blacklisted: %s" % g_host)
            giis = session.query(schema.GiisMeta).filter_by(hostname = g_host).first()
            if giis:
                self.log.debug("got GIIS")
                giis.blacklisting()
                giis.set_db_lastmodified()
                session.add(giis)
            else:
                _tmpl.append(g_host) # clean up later
                self.log.debug("Did not exist in DB.... skipping")

        for g_host in _tmpl:
            self.giis_blacklisted.pop(g_host)


        for giis in session.query(schema.GiisMeta).filter(schema.GiisMeta.db_lastmodified <  timestamp):
            giis.set_status('inactive')
            giis.set_db_lastmodified()
            session.add(giis)

        session.commit()

    def _refresh_gris_list(self):
        """ Repopulates list of GRIS'es """

        if not self.giis_list:
            self.log.warn("No GIISes around, falling back to 'old' GRIS'es list") 
        else:
            self.gris_list = list()
            
            session = meta.Session()
            for g_host, g_port, g_mds_vo_name  in self.giis_list:
                db_giis = session.query(schema.GiisMeta).filter_by(hostname = g_host).first()
                self.log.debug("Updating GRIS'es announced by '%s'" % db_giis.hostname) 
                timestamp = time.time()
                giis_url = arclib.URL(('ldap://%s:%s/o=grid/mds-vo-name=%s') % \
                    (g_host, g_port, g_mds_vo_name))
                
                gris_urls = arclib.GetClusterResources(giis_url)
                proc_time = time.time() - timestamp
                self.log.debug("Query of GIIS for GRIS URLs took %s seconds" % proc_time)
                db_giis.set_processing_time(proc_time)
                db_giis.set_db_lastmodified()
                session.add(db_giis)
                for gris_url in  gris_urls:
                    if gris_url not in self.gris_list:
                        self.gris_list.append(gris_url)
            session.commit()

    def run(self):
        self.gris2db.start() 
        cycle = 1
        while True:
            try:
                self.log.info("New 'caching' cycle run.")
                cycle -= 1
                timestamp = time.time()
                if not self.giis_list or cycle <= 1: 
                    cycle = Giis2db.GIIS_REFRESH_PERIOD
                    self._refresh_giis_list() 
                self._refresh_gris_list()            
                self.gris2db.add_urls2queue(self.gris_list)
                self.log.debug("Refreshed GRISes list")
                proctime = time.time() - timestamp
                self.log.info("Current run took  %s seconds" % proctime)

                if proctime > self.periodicity:
                    continue
                else:
                    time.sleep(self.periodicity - proctime)

            except Exception, e:
                self.log.error("RUN-loop: Got exception %r", e)
