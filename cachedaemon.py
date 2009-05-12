#!/usr/bin/env python
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
queried data persistently in local file system (local caching).
Other processes might speed up their 'read' access by processing these (cached) files instead
of querying the GIIS or GRISes themselves.

Todo: currently this daemon is bound to GRIS/GIIS -> refactor it so it can handle
other information system flavor and types, while the type and flavor shall be specified by
either an entry in a config file or an invocation flag.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="9.3.2009"
__version__="0.1.0"

from optparse import OptionParser
import sys, os.path
import time
import logging,logging.config

from daemon import Daemon

log = logging.getLogger(__name__)

GRID_MONITOR_PATH = "/opt/GridMonitor"  # used to fetch APIs that need to be implemented

if os.path.exists(GRID_MONITOR_PATH) and os.path.isdir(GRID_MONITOR_PATH):
    log.info("Adding '%s' to PYTHONPATH in order to fetch GridMonitor API." % GRID_MONITOR_PATH)
else:
    log.error("PYTHONPATH could not be set so if finds GridMonitor APIs.")
sys.path.append(GRID_MONITOR_PATH)

from gris.giis import NGGiis 
from gris.griscache import GrisCache

class GiisCacher(Daemon):


    def __init__(self, pidfile="/var/run/giiscacher.pid"):
      
        Daemon.__init__(self,pidfile)
        self.dbase = None
        self.gris_list=[]
        self.__get_options()

    def __get_options(self):
        usage= "usage: %prog [options] start|stop|restart \n\nDo %prog -h for more help."

        parser = OptionParser(usage=usage, version ="%prog " + __version__)

        parser.add_option("-g","--GIIS", action="store",
                        dest="giis", type="string",
                        help="Comma-separated lisf of GIIS server(s).")

        parser.add_option("-s","--shelve_file", action="store",
                        dest="shelve_file", type="string",
                        default="/tmp/giiscache.shelve",
                        help="Name of local file for persistent storage of queried data. (default='%default')")
        
        parser.add_option("-m","--mds_vo_name", action="store",
                        dest="mds_vo_name", type="string",
                        default="Switzerland",
                        help="LDAP/GIIS base value for 'Mds-Vo-name=<your_choice>', (default='%default')")
        
        parser.add_option("-p","--periodicity", action="store",
                        dest="periodicity", type="int",
                        default="30",
                        help="Queries GIIS every 'periodicity'[seconds].(default='%default')")

        (options,args) = parser.parse_args()
        log.debug("Invocation with args: %r and options: %r" % (args,options))
        
        self.options = options        

        # checking options and parameters.
        if (not args):
            parser.error("Argumnet is missing.") 
        
        if (args[0] not in ('start','stop','restart')):
            parser.error("Uknown argument")
        self.command = args[0]

        if not options.giis and args[0] != 'stop':
            print "Nothing to do, since no GIIS servers have been specified. Please\n"
            print "do use the --GIIS option in case you want to change this."
            sys.exit(0)
        
        self.giis_list = options.giis.split(',')
        log.info("Using following GIIS'es: %r" % self.giis_list)

    def __del__(self):
        if self.dbase:
            self.dbase.close() 

    def change_state(self):
        if self.command == 'start':
            log.info("starting daemon...")
            daemon.start()
        elif self.command == 'stop':
            log.info("stopping daemon...")
            daemon.stop()
        elif self.command == 'restart':
            log.info("restarting daemon...")
            daemon.restart()
   
    
    def __refresh_gris_list(self):
        for giis_server in self.giis_list:
            ng = None
            mds_vo_name = self.options.mds_vo_name
            log.info("querying giis server:'%s'" % giis_server)
            try:
                ng = NGGiis(giis_server, mds_vo_name=mds_vo_name)
                ng_gris_list = ng.get_gris_list()
                ng.close()
                for gris in ng_gris_list:
                    if self.gris_list.count(gris) == 0: # avoid duplicates
                        self.gris_list.append(gris)
            except Exception, e:
                # XXX exception handling, or at least better reporting
                log.debug("got exception %r", e)
    def run(self):
        
        while True:
            self.__refresh_gris_list() 
            log.debug("gris-list %r" % self.gris_list)
            db = GrisCache(self.options.shelve_file,self.gris_list)
            db.create()
            db.populate()
            db.close()
            time.sleep(self.options.periodicity)


if __name__ == "__main__":

    logging.config.fileConfig("logging.conf")
    daemon = GiisCacher()
    daemon.change_state()


