"""
Generation of RRD plots.
BEWARE: the intervalls with which data is written to the rrd 
databases is 'hardcoded'. It's assumed that data samples arrive
at 120 seconds intervals. 

"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "08.01.2010"
__version__ = "0.1.0"

import logging, os.path
from infosys import GrisGiis
from jobs import Jobs
from load import GridLoad

class RRD(object):

    def __init__(self, rrddir, plotdir):
        self.log = logging.getLogger(__name__)
        if os.path.exists(rrddir) and os.path.isdir(rrddir):
            self.log.warn("RDD database dir %s does not exist" % rrddir)
            pass
        else: 
            rrddir = '/tmp'
        
        if os.path.exists(plotdir) and os.path.isdir(plotdir):
            self.log.warn("RDD plot dir %s does not exist" % rrddir)
            pass
        else: 
            plotdir = '/tmp'

        self.log.info("RDD databases will be stored in directory '%s'" % rrddir)
        self.log.info("RDD plots  will be stored in directory '%s'" % plotdir)

        self.infosys = GrisGiis(rrddir, plotdir)
        self.jobs = Jobs(rrddir,plotdir)
        ldir = os.path.join(rrddir, 'load')
        self.gridload = GridLoad(ldir, plotdir)
        self.log.debug("Initialization finished")
       

    def generate_plots(self):
        # 1. infosys plots
        self.infosys.generate_plots()
        # 2. plot of jobs in final state 
        self.jobs.generate_plots()
        # 3. plot  grid, clusert and queue  load
        self.gridload.generate_plots()
