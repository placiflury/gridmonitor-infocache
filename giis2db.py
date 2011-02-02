#!/usr/bin/python
##############################################################################
# Copyright (c) 2008, SMSCG - Swiss Multi Science Computing Grid.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of SMSCG nor the names of its contributors may be 
#      used to endorse or promote products derived from this software without 
#      specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# $Id$
###############################################################################
"""
Queries NorduGrid's ldap-based information system (GRIS/GIIS) and stores 
queried data persistently in a (local) database.

Notice: currently only supporting Nordugrid's GIIS/GRIS schema. 
todo: generalize so it supports also Glue schema(s)
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "07.06.2010"
__version__ = "0.2.1"

from optparse import OptionParser
from sqlalchemy import engine_from_config
import sys, os, os.path, ldap, time
import logging, logging.config
from infocache.daemon import Daemon

from infocache.db import init_model
from infocache.utils import init_config
import infocache.utils.config_parser as config_parser
from infocache.sanity.dbcleaner import Cleanex
from infocache.rrd.rrd import RRD
from infocache.gris.giis import NGGiis
from infocache.gris.gris2db import Gris2db


class Giis2db(Daemon):
    
    BLACK_LIST_CYCL = 2    # blacklisted gris'es checked frequency
    NETWORK_TIMEOUT = 10    # ldap network timeout [seconds]
    LDAP_TIMEOUT = 20       # ldap timeout for any request [seconds]

    def __init__(self, pidfile="/var/run/giis2db.pid"):
        self.log = logging.getLogger(__name__)
        Daemon.__init__(self, pidfile)
        self.gris_list = list()
        self.gris_blacklist = dict()
        self.gris_whitelist = dict() 
        self.options = None
        self.mds_vo_name = None
        self.rrd_directory = None
        self.periodicity = None
        self.top_giis_list = None
        self.plot_directory = None
        self.command = None
        self.giis_proctime = None

        self.__get_options()
            
        try:
            engine = engine_from_config(config_parser.config.get(),'sqlalchemy_infocache.')
            init_model(engine)
            self.log.info("Session object to local database created")
        except Exception, ex:
            self.log.error("Session object to local database failed: %r", ex)
        
        self.log.debug("Initialization finished")

    def __get_options(self):
        usage = "usage: %prog [options] start|stop|restart \n\nDo %prog -h for more help."

        parser = OptionParser(usage = usage, version = "%prog " + __version__)
        
        parser.add_option("" , "--config_file", action = "store",
            dest = "config_file", type = "string",
            default = "/opt/smscg/infocache/etc/config.ini",
            help = "File holding the smscg specific configuration for this site (default=%default)")

        (options, args) = parser.parse_args()
        self.log.debug("Invocation with args: %r and options: %r" % (args, options))
        
        self.options = options        

        # checking options and parameters.
        if (not args):
            parser.error("Argument is missing.") 
        
        if (args[0] not in ('start', 'stop', 'restart')):
            parser.error("Uknown argument")
        self.command = args[0]

        # initialize configuration
        try:
            init_config(options.config_file)
        except Exception, e:
            self.log.error("While reading configuration %s got: %r" % (options.config_file, e))
            sys.exit(-1) 
       
        # check whether mandatory settings of configuration are there 
        
        self.mds_vo_name = config_parser.config.get('mds_vo_name')
        if not self.mds_vo_name:
            self.log.error("'mds_vo_name' option missing in %s." % (options.config_file))
            sys.exit(-1)
        
        giis_raw = config_parser.config.get('giis')
        if not giis_raw:
            self.log.error("'giis' option missing in %s." % (options.config_file))
            sys.exit(-1)

        self.top_giis_list = giis_raw.split(',')
        self.log.info("Using following top level GIIS'es: %r" % self.top_giis_list)
        
        periodicity = config_parser.config.get('periodicity')
        if not periodicity:
            self.log.info("No periodicity option defined in %s. Setting it to default (120 secs)"
                    % (options.confif_file))
            self.periodicity = 120
        else:
            try:
                self.periodicity = int(periodicity)
            except Exception, e:
                self.log.error("Could not set periodicity to '%s'. Please check option in %s"
                    % (periodicity,options.confif_file))
                sys.exit(-1)
       
        self.rrd_directory = config_parser.config.get('rrd_directory') 
        self.plot_directory = config_parser.config.get('plot_directory') 

    def __del__(self):
        pass

    def change_state(self):
        """ Changing daemon state. """
        if self.command == 'start':
            self.log.info("starting daemon...")
            daemon.start()
            self.log.info("started...")
        elif self.command == 'stop':
            self.log.info("stopping daemon...")
            daemon.stop()
            self.log.info("stopped")
        elif self.command == 'restart':
            self.log.info("restarting daemon...")
            daemon.restart()
            self.log.info("restarted")

    def is_gris_reacheable(self, host, port):
        """ Checking whether GRIS'es are up and reacheable. """
        host = host.strip()
        if not host.startswith("ldap://"):
            host = "ldap://" + host
        port_suffix = ":" + str(port)
        if not host.endswith(port_suffix):
            host += port_suffix
        try:
            timestamp = time.time()
            con = ldap.initialize(host)
            con.set_option(ldap.OPT_NETWORK_TIMEOUT, Giis2db.NETWORK_TIMEOUT)
            con.set_option(ldap.OPT_TIMEOUT, Giis2db.LDAP_TIMEOUT)
            con.simple_bind_s()
            con.unbind()
            gris = host.split(':')[1][2:]  # black-magic for getting hostname (dns)
            self.gris_whitelist[gris] = time.time() - timestamp  # response time
            return True
        except Exception, e:
            self.log.debug("Can't reach %s: %r" % (host, e))
            return False        

    def get_giis_gris(self,giis_server, port, mds_vo_name):
        """ XXX maybe need to detect loops """
        try:
            timestamp = time.time()
            
            ng = NGGiis(giis_server, mds_vo_name = mds_vo_name)
            ng_gris_list = ng.get_gris_list() # list of (server, port)
            ng_giis_list = ng.get_giis_list() # list of (server, port, mds_vo_name)
            
            self.giis_proctime[giis_server] = time.time() - timestamp 
            ng.close()

            for gris in ng_gris_list:
                if self.gris_list.count(gris) == 0: # avoid duplicates
                    if gris in self.gris_blacklist.keys():
                        self.gris_blacklist[gris] -= 1   # decrease counter
                        if self.gris_blacklist[gris] == 0:
                            self.gris_blacklist.pop(gris)
                    elif self.is_gris_reacheable(gris[0], gris[1]):
                        self.gris_list.append(gris)
                    else:
                        self.log.info("blacklisting ('%s','%s') because not reacheable." % (gris[0], gris[1])) 
                        self.gris_blacklist[gris] = Giis2db.BLACK_LIST_CYCL
            
            for server, port, mds_vo_name in ng_giis_list:
                self.get_giis_gris(server, port, mds_vo_name)

        except Exception, e:
            # XXX exception handling, or at least better reporting
            self.log.info("got exception %r", e)
            
            
    def __refresh_gris_list(self):
        port = 2135
        del(self.gris_list)
        self.gris_list = list()
        self.giis_proctime = dict()
        for giis_server in self.top_giis_list:
            self.log.info("Querying top-level GIIS server:'%s'" % giis_server)
            self.get_giis_gris(giis_server, port, mds_vo_name = self.mds_vo_name)
        


    def __old_refresh_gris_list(self):
        del(self.gris_list)
        self.gris_list = list()
        self.giis_proctime = dict()
        for giis_server in self.top_giis_list:
            ng = None
            mds_vo_name = self.mds_vo_name
            self.log.info("Querying top-level GIIS server:'%s'" % giis_server)
            try:
                timestamp = time.time()
                ng = NGGiis(giis_server, mds_vo_name = mds_vo_name)
                ng_gris_list = ng.get_gris_list()
                self.giis_proctime[giis_server] = time.time() - timestamp 
                ng.close()
                for gris in ng_gris_list:
                    if self.gris_list.count(gris) == 0: # avoid duplicates
                        if gris in self.gris_blacklist.keys():
                            self.gris_blacklist[gris] -= 1   # decrease counter
                            if self.gris_blacklist[gris] == 0:
                                self.gris_blacklist.pop(gris)
                        elif self.is_gris_reacheable(gris[0], gris[1]):
                            self.gris_list.append(gris)
                        else:
                            self.log.info("blacklisting ('%s','%s') because not reacheable." % (gris[0], gris[1])) 
                            self.gris_blacklist[gris] = Giis2db.BLACK_LIST_CYCL
            except Exception, e:
                # XXX exception handling, or at least better reporting
                self.log.info("got exception %r", e)

    def run(self):
        db = Gris2db()
        cleaner = Cleanex()
        rrd = RRD(self.rrd_directory, self.plot_directory)
        while True:
            last_query_time = db.get_last_query_time() 
            self.gris_whitelist = dict()
            timestamp = time.time()
            self.log.info("NEW RUN")
            self.__refresh_gris_list() 
            db.refresh_gris_list(self.gris_list)
            if self.gris_blacklist:
                db.set_blacklist(self.gris_blacklist.keys())
            else:
                db.set_blacklist(list())
            db.set_whitelist(self.gris_whitelist)
            db.set_giis_proctime(self.giis_proctime)
            self.log.debug("Starting populating db")
            db.populate()
        
            # cleaning up db 
            self.log.debug("Start cleaning up db")
            cleaner.set_last_query_time(last_query_time)
            cleaner.check_clusters()
            cleaner.check_queues()
            cleaner.check_jobs()
            rrd.generate_plots()
            proctime = time.time() - timestamp
            self.log.debug("CURRENT RUN TOOK %s seconds" % proctime)
            if proctime > self.periodicity:
                continue
            else:
                time.sleep(self.periodicity - proctime)

if __name__ == "__main__":
    
    logging.config.fileConfig("/opt/smscg/infocache/etc/logging.conf")
    daemon = Giis2db()
    daemon.change_state()


