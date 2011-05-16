#!/usr/bin/env python
"""
Init-script for infocache. Depending on the configuration
it starts either the giis2db our housekeeper daemon process 
(or even both).
"""

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "11.04.2011"
__version__ = "0.1.0"

import os
import sys
import logging
import logging.config
from optparse import OptionParser
from sqlalchemy import engine_from_config

from infocache.factory import DaemonFactory
from infocache.db import init_model
import infocache.utils.config_parser as config_parser


class Infocache(object):
    """
        'User interface' for starting infocache daemon 
        processes. 
        The current processes are:
        - giis2db, which is a daemon for populationg a database
                    cache with snapshot of Grid as currently 
                    reported/advertized by the Grid information system.
        - housekeeper, which is a daemon for processing and cleaning up
                    the information stored in the cache. The housekeeper
                    does for example generate RRD usage plots.
    """
    def __init__(self):
        self.log = logging.getLogger(__name__)
        self.command, options = self.get_options()
        factory = DaemonFactory()
        config = options.config_file
        self.daemons = factory.newDaemon(config)

    def get_options(self):
        usage = "usage: %prog [options] start|stop|restart \n\nDo %prog -h for more help."

        parser = OptionParser(usage = usage, version = "%prog " + __version__)
        parser.add_option("" , "--config_file", action = "store",
            dest = "config_file", type = "string",
            default = "/opt/smscg/infocache/etc/config.ini",
            help = "File holding the smscg specific configuration for this site (default=%default)")

        (options, args) = parser.parse_args()
        self.log.debug("Invocation with args: %r and options: %r" % (args, options))
        
        if (not args):
            parser.error("Argument is missing.") 
        
        if (args[0] not in ('start', 'stop', 'restart')):
            parser.error("Uknown argument")

        return args[0], options


    def change_state(self):
        """ Changing daemon state. """

        for daemon in self.daemons:
            pid = os.fork()
            if pid > 0: #  parent takes next daemon
                continue
            # initialize db session for each daemon 'separately'
            try:
                engine = engine_from_config(config_parser.config.get(),'sqlalchemy_infocache.')
                init_model(engine)
                self.log.info("Session object to local/remote database created")
            except Exception, ex:
                self.log.error("Session object to local/remote database failed: %r", ex)

            daemon.change_state(self.command)

if __name__ == "__main__":
    
    logging.config.fileConfig("/opt/smscg/infocache/etc/logging.conf")
    IC = Infocache()
    IC.change_state()


