"""
Generates RRD databases and plots and 
keeps information system cache clean.
"""

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "10.04.2011"
__version__ = "0.1.0"

import logging
import time

from daemon import Daemon
from rrd.rrd import RRD 
from sanity.dbcleaner import Cleanex

class Housekeeper(Daemon):
    """
        Daemon for generating RRD databases and plots from 
        the information system cache (i.e. database entries)
        and doing some cleaning of the cache (db). 
    """

    RRD_PERIODICITY = 120  # RRD plot resolution (Hardcoded)

    def __init__(self, pidfile="/var/run/housekeeper.pid", **kwargs):
        self.log = logging.getLogger(__name__)
        Daemon.__init__(self, pidfile)
        rrd_dir = kwargs['rrd_dir']
        plot_dir = kwargs['plot_dir']

        self.rrd = RRD(rrd_dir, plot_dir)
        self.cleaner = Cleanex() 

        self.log.debug("Initialization finished")


    def change_state(self, state):
        """ Changing daemon state. """
        if state == 'start':
            self.log.info("starting daemon...")
            Daemon.start(self)
        elif state == 'stop':
            self.log.info("stopping daemon...")
            Daemon.stop(self)
            self.log.info("stopped")
        elif state == 'restart':
            self.log.info("restarting daemon...")
            Daemon.restart(self)

    def run(self):
        while True:
            try:
                timestamp = time.time()
                self.rrd.generate_plots()
                self.cleaner.main()
                
                proctime = time.time() - timestamp
                self.log.debug("Housekeeper current run took  %s seconds" % proctime)
                if proctime > Housekeeper.RRD_PERIODICITY:
                    continue
                else:
                    time.sleep(Housekeeper.RRD_PERIODICITY - proctime)

            except Exception, e:
                self.log.error("RUN-loop: Got exception %r", e)
                time.sleep(Housekeeper.RRD_PERIODICITY)
