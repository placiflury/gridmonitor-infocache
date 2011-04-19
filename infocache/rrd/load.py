"""
Generation of RRD plots of grid, cluster and queue loads.
"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "16.04.2010"
__version__ = "0.2.0"

import logging
import time
import os.path
import  commands # XXX change to subprocess
import cPickle
from datetime import datetime

from infocache.db import meta, schema

class GridLoad(object):

    def __init__(self, rrddir, plotdir):
        self.log = logging.getLogger(__name__)
        self.rrddir = rrddir
        self.plotdir = plotdir
        self.log.debug("Initialization finished")
        
    
    def create_rrd(self, dbname):
        """
        RRA: 24 hours (120 * 720)   with 120 sec resolution
        RRA: 1 week (120 * 15  * 336)  with 30 min resolution
        RRA: 12 months (120 * 360 * 365 ) with 12 hours resolution
        """
        cmd = "rrdtool create %s --step 120 \
         DS:totalcpus:GAUGE:240:0:U\
         DS:usedcpus:GAUGE:240:0:U\
         DS:gridrunning:GAUGE:240:0:U\
         DS:running:GAUGE:240:0:U\
         DS:gridqueued:GAUGE:240:0:U\
         DS:localqueued:GAUGE:240:0:U\
         DS:prelrmsqueued:GAUGE:240:0:U\
         RRA:AVERAGE:0.5:1:720\
         RRA:AVERAGE:0.5:15:336\
         RRA:AVERAGE:0.5:360:365\
         RRA:MAX:0.5:1:720\
         RRA:MAX:0.5:15:336:\
         RRA:MAX:0.5:360:365" %  (dbname) 

        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.info("Created RDD database '%s'" % dbname)
    
    def _make_cmd(self, fig_name, start, end, rrd_file, type='queued',grid_cluster_queue_name='Grid'):
        # XXX -o option for logarithmic graphs fails since update to  debian squeeze ???
        if type == 'queued':
            cmd = "rrdtool graph %s -s %d -e %d --title='Actual queue backlogs on %s' \
                 DEF:gridq=%s:gridqueued:AVERAGE \
                 DEF:localq=%s:localqueued:AVERAGE \
                 DEF:plrmsq_tmp=%s:prelrmsqueued:AVERAGE \
                 CDEF:plrmsq=plrmsq_tmp,gridq,- \
                 VDEF:gridq_max=gridq,MAXIMUM \
                 VDEF:gridq_avg=gridq,AVERAGE \
                 VDEF:gridq_min=gridq,MINIMUM \
                 VDEF:localq_max=localq,MAXIMUM \
                 VDEF:localq_avg=localq,AVERAGE \
                 VDEF:localq_min=localq,MINIMUM \
                 VDEF:plrmsq_max=plrmsq_tmp,MAXIMUM \
                 VDEF:plrmsq_avg=plrmsq_tmp,AVERAGE \
                 VDEF:plrmsq_min=plrmsq_tmp,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800 \
                 -v \[jobs\] \
                 COMMENT:\"                  \"\
                 COMMENT:\"Minimum  \"\
                 COMMENT:\"Average  \"\
                 COMMENT:\"Maximum  \l\"\
                 COMMENT:\"    \"\
                 AREA:gridq#FF9000:'GRID QUEUED'\
                 GPRINT:gridq_min:\"   %%2.2lf\" \
                 GPRINT:gridq_avg:\"     %%2.2lf\" \
                 GPRINT:gridq_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:plrmsq#D9F38E:'PRELRMS QUEUED:STACK'\
                 GPRINT:plrmsq_min:\"   %%2.2lf\" \
                 GPRINT:plrmsq_avg:\"     %%2.2lf\" \
                 GPRINT:plrmsq_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:localq#CCFFFF:'NON-GRID QUEUED':STACK\
                 GPRINT:localq_min:\"   %%2.2lf\" \
                 GPRINT:localq_avg:\"     %%2.2lf\" \
                 GPRINT:localq_max:\"      %%2.2lf\l\"" % (fig_name, start, end, 
                 grid_cluster_queue_name, rrd_file, rrd_file, rrd_file)
        else:
            cmd = "rrdtool graph %s -s %d -e %d --title='Actual load on %s' \
                 DEF:ntotcpus=%s:totalcpus:AVERAGE \
                 DEF:nusedcpus=%s:usedcpus:AVERAGE \
                 DEF:ngrun=%s:gridrunning:AVERAGE \
                 DEF:nrun_tmp=%s:running:AVERAGE \
                 CDEF:nrun=nrun_tmp,ngrun,- \
                 VDEF:ntotcpus_max=ntotcpus,MAXIMUM \
                 VDEF:ntotcpus_avg=ntotcpus,AVERAGE \
                 VDEF:ntotcpus_min=ntotcpus,MINIMUM \
                 VDEF:nusedcpus_max=nusedcpus,MAXIMUM \
                 VDEF:nusedcpus_avg=nusedcpus,AVERAGE \
                 VDEF:nusedcpus_min=nusedcpus,MINIMUM \
                 VDEF:ngrun_max=ngrun,MAXIMUM \
                 VDEF:ngrun_avg=ngrun,AVERAGE \
                 VDEF:ngrun_min=ngrun,MINIMUM \
                 VDEF:nrun_max=nrun_tmp,MAXIMUM \
                 VDEF:nrun_avg=nrun_tmp,AVERAGE \
                 VDEF:nrun_min=nrun_tmp,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800 \
                 -v \['cores or jobs'\] \
                 LINE2:ntotcpus#CCFFFF:\"TOTAL COREs \l\"\
                 COMMENT:\"                  \"\
                 COMMENT:\"Minimum  \"\
                 COMMENT:\"Average  \"\
                 COMMENT:\"Maximum  \l\"\
                 COMMENT:\"    \"\
                 AREA:ngrun#FF9000:'GRID RUN'\
                 GPRINT:ngrun_min:\"     %%2.2lf\" \
                 GPRINT:ngrun_avg:\"     %%2.2lf\" \
                 GPRINT:ngrun_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:nrun#D9F38E:'NON-GRID RUN':STACK\
                 GPRINT:nrun_min:\" %%2.2lf\" \
                 GPRINT:nrun_avg:\"     %%2.2lf\" \
                 GPRINT:nrun_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 LINE2:nusedcpus#990000:'USED COREs'\
                 GPRINT:nusedcpus_min:\"    %%2.2lf\" \
                 GPRINT:nusedcpus_avg:\"     %%2.2lf\" \
                 GPRINT:nusedcpus_max:\"      %%2.2lf\l\"" % (fig_name, start, end, 
                 grid_cluster_queue_name, rrd_file, rrd_file, rrd_file, 
                 rrd_file)
        return cmd
        
    def create_plots(self, grid_cluster_queue_name):
        
        rrd_file = os.path.join(self.rrddir, grid_cluster_queue_name+'.rrd')
        h24_e = time.time()
        h24_s = h24_e - 24 * 3600
        
        fig24_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_qh24.png') # 24 hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file, 'queued', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error(output)
        
        fig24_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_ch24.png') # 24 hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file, 'cpu', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error(output)
        
        hw1_e = time.time()
        hw1_s = hw1_e - 24 * 3600 * 7 

        figw1_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_qw1.png') # 1 week  plot
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file,'queued', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        figw1_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_cw1.png') # 1 week  plot
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file, 'cpu', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error(output)
        
        hm12_e = time.time()
        hm12_s = hm12_e - 24 * 3600 * 365
        
        figm12_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_qy1.png') # 1year  plot
        cmd = self._make_cmd(figm12_name, hm12_s, hm12_e, rrd_file, 'queued',grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error(output)

        figm12_name = os.path.join(self.plotdir, grid_cluster_queue_name + 'stats_cy1.png') # 1 year  plot
        cmd = self._make_cmd(figm12_name, hm12_s, hm12_e, rrd_file, 'cpu', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

    def statistics(self):
        self.log.debug("Populating load statistics RDD.")
        session = meta.Session()
        
        stats = session.query(schema.GridStats).first()
        if not stats:
            return

        gstats = cPickle.loads(stats.pickle_object)      
        #grid_name = gstats.get_name()
        grid_name = 'Grid' # using this instead of grid-name
        self._statistics(grid_name, gstats)

        for cluster in gstats.get_children():
            cluster_name = cluster.get_name()
            # XXX do same for queues ... if desired/requested
            self._statistics(cluster_name, cluster)


    def _statistics(self, name, stats):

        dbn = os.path.join(self.rrddir, name+'.rrd')
        if not os.path.exists(dbn):
                self.create_rrd(dbn)

        totalcpus = stats.get_attribute('total_cpus')
        usedcpus = stats.get_attribute('used_cpus')
        gridrunning = stats.get_attribute('grid_running')
        running = stats.get_attribute('running')
        gridqueued = stats.get_attribute('grid_queued')
        localqueued = stats.get_attribute('local_queued')
        prelrmsqueued = stats.get_attribute('prelrms_queued')

        cmd = 'rrdtool update %s -t\
             totalcpus:usedcpus:gridrunning:running:gridqueued:localqueued:prelrmsqueued \
             %d:%d:%d:%d:%d:%d:%d:%d' \
             % (dbn, time.time(), totalcpus, usedcpus, gridrunning, running, gridqueued, localqueued, prelrmsqueued)

        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.debug("Updated RDD database '%s'" % dbn)

        self.create_plots(name)
    
    def generate_plots(self):
        self.statistics()

