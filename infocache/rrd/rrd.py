"""
Generation of RRD plots.
BEWARE: the intervalls with which data is written to the rrd 
databases is 'hardcoded'. It's assumed that data samples arrive
at 120 seconds intervals. 

"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "09.11.2010"
__version__ = "0.2.0"

import logging, os, os.path
from infosys import GrisGiis
from jobs import Jobs
from load import GridLoad

class RRD(object):


    def __init__(self, rrddir, plotdir):
        
        self.log = logging.getLogger(__name__)
        self.log.debug("Checking whether rrddir exits: %s" % rrddir)
        self._check_create_dir(rrddir)

        ldir = os.path.join(rrddir, 'load')
        self.log.debug("Checking whether rrddir2 exits: %s" % ldir)
        self._check_create_dir(ldir)

        self.log.debug("Checking whether plotdir exits: %s " % plotdir)
        self._check_create_dir(plotdir)

        self.log.info("RDD databases will be stored in directories '%s' and '%s' " % \
                (rrddir, ldir))
        self.log.info("RDD plots  will be stored in directory '%s'" % plotdir)

        self.infosys = GrisGiis(rrddir, plotdir)
        self.gridload = GridLoad(ldir, plotdir)
        self.jobs = Jobs(rrddir, plotdir)
        
        self.log.debug("Initialization finished")


    def _check_create_dir(self,dirname):
        """ Check whether directory exists. If not create it. """

        if os.path.exists(dirname) and os.path.isdir(dirname):
            return True
    
        if os.path.exists(dirname) and not os.path.isdir(dirname):
            self.log.warn("'%s' exits and is *NOT* a directory." % dirname)
            return False
        
        try:
            self.log.info("Creating directory '%s'" % dirname)
            os.mkdir(dirname)
            return True
        except OSError, e:
            self.log.warn("Got '%r' while trying to create directory '%s'" % \
                (e, dirname))
            return False
        except Exception, e1:
            self.log.warn("Unexpected error '%r' while trying to create directory '%s'" % \
                (e1, dirname))
            return False
               

    def generate_plots(self):
        # 1. infosys plots
        self.infosys.generate_plots()
        # 2. plot of jobs in final state 
        self.jobs.generate_plots()
        # 3. plot  grid, clusert and queue  load
        self.gridload.generate_plots()
