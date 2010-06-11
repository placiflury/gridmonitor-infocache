"""
Generation of RRD plots of grid, cluster and queue loads.
"""
__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "16.04.2010"
__version__ = "0.1.0"

import logging, pickle
import infocache.db.meta as meta
import infocache.db.ng_schema as schema

import time, os.path, commands
from sqlalchemy import orm
import sqlalchemy as sa


class GridLoad(object):

    def __init__(self, rrddir, plotdir):
        self.log = logging.getLogger(__name__)
        self.Session = orm.scoped_session(meta.Session)
        """
        db = 'mysql://exp:lap3ns@localhost/experimental'
        engine = sa.create_engine(db)
        meta.metadata.bind = engine

        self.Session = orm.sessionmaker(autoflush=False, transactional=False, bind=engine)
        """
        self.rrddir = rrddir
        self.plotdir = plotdir
        self.log.debug("Initialization finished")
        
    
    def create_rrd(self, dbname):
        """
        RRA: 24 hours (120 * 720)   with 120 sec resolution
        RRA: 1 week (120 * 30  * 168)  with 1 hour resolution
        RRA: 6 months (120 * 360 * 182 ) with 12 hours resolution
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
         RRA:AVERAGE:0.5:30:168\
         RRA:AVERAGE:0.5:360:182\
         RRA:MAX:0.5:1:180\
         RRA:MAX:0.5:30:168:\
         RRA:MAX:0.5:360:182" %  (dbname) 

        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.info("Created RDD database '%s'" % dbname)
    
    def _make_cmd(self, fig_name, start, end, rrd_file, type='queued',grid_cluster_queue_name='Grid'):
        if type == 'queued':
            cmd = "rrdtool graph %s -s %d -e %d --title='Actual queue backlogs on %s' \
                 DEF:ngqueued=%s:gridqueued:AVERAGE \
                 DEF:nlqueued=%s:localqueued:AVERAGE \
                 DEF:npqueued=%s:prelrmsqueued:AVERAGE \
                 VDEF:ngqueued_max=ngqueued,MAXIMUM \
                 VDEF:ngqueued_avg=ngqueued,AVERAGE \
                 VDEF:ngqueued_min=ngqueued,MINIMUM \
                 VDEF:nlqueued_max=nlqueued,MAXIMUM \
                 VDEF:nlqueued_avg=nlqueued,AVERAGE \
                 VDEF:nlqueued_min=nlqueued,MINIMUM \
                 VDEF:npqueued_max=npqueued,MAXIMUM \
                 VDEF:npqueued_avg=npqueued,AVERAGE \
                 VDEF:npqueued_min=npqueued,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800 \
                 -o \
                 -v \[number\] \
                 COMMENT:\"                  \"\
                 COMMENT:\"Minimum  \"\
                 COMMENT:\"Average  \"\
                 COMMENT:\"Maximum  \l\"\
                 COMMENT:\"    \"\
                 AREA:ngqueued#FF9000:'GRID QUEUED'\
                 GPRINT:ngqueued_min:\"   %%2.2lf\" \
                 GPRINT:ngqueued_avg:\"     %%2.2lf\" \
                 GPRINT:ngqueued_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:npqueued#D9F38E:'PRELRMS QUEUED':STACK\
                 GPRINT:npqueued_min:\"   %%2.2lf\" \
                 GPRINT:npqueued_avg:\"     %%2.2lf\" \
                 GPRINT:npqueued_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:nlqueued#CCFFFF:'NON-GRID QUEUED':STACK\
                 GPRINT:nlqueued_min:\"   %%2.2lf\" \
                 GPRINT:nlqueued_avg:\"     %%2.2lf\" \
                 GPRINT:nlqueued_max:\"      %%2.2lf\l\"" % (fig_name, start, end, 
                 grid_cluster_queue_name, rrd_file, rrd_file, rrd_file)
        else:
            cmd = "rrdtool graph %s -s %d -e %d --title='Actual load on %s' \
                 DEF:ntotcpus=%s:totalcpus:AVERAGE \
                 DEF:nusedcpus=%s:usedcpus:AVERAGE \
                 DEF:ngrun=%s:gridrunning:AVERAGE \
                 DEF:nrun=%s:running:AVERAGE \
                 VDEF:ntotcpus_max=ntotcpus,MAXIMUM \
                 VDEF:ntotcpus_avg=ntotcpus,AVERAGE \
                 VDEF:ntotcpus_min=ntotcpus,MINIMUM \
                 VDEF:nusedcpus_max=nusedcpus,MAXIMUM \
                 VDEF:nusedcpus_avg=nusedcpus,AVERAGE \
                 VDEF:nusedcpus_min=nusedcpus,MINIMUM \
                 VDEF:ngrun_max=ngrun,MAXIMUM \
                 VDEF:ngrun_avg=ngrun,AVERAGE \
                 VDEF:ngrun_min=ngrun,MINIMUM \
                 VDEF:nrun_max=nrun,MAXIMUM \
                 VDEF:nrun_avg=nrun,AVERAGE \
                 VDEF:nrun_min=nrun,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800 \
                 -v \[number\] \
                 LINE2:ntotcpus#CCFFFF:\"TOTAL CPUS \l\"\
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
                 LINE2:nusedcpus#990000:'USED CPUS'\
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
        
        fig24_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_qh24.png') # 24 hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file, 'queued', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        fig24_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_ch24.png') # 24 hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file, 'cpu', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        hw1_e = time.time()
        hw1_s = hw1_e - 24 * 3600 * 7 

        figw1_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_qw1.png') # 1 week  plot
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file,'queued', grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        figw1_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_cw1.png') # 1 week  plot
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file, 'cpu',grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        hm6_e = time.time()
        hm6_s = hm6_e - 24 * 3600 * 182
        
        figm6_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_qm6.png') # 1/2 year  plot
        cmd = self._make_cmd(figm6_name, hm6_s, hm6_e, rrd_file, 'queued',grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

        figm6_name = os.path.join(self.plotdir, grid_cluster_queue_name+'stats_cm6.png') # 1/2 year  plot
        cmd = self._make_cmd(figm6_name, hm6_s, hm6_e, rrd_file, 'cpu',grid_cluster_queue_name)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

    def statistics(self):
        self.log.debug("Populating load statistics RDD.")
        session = self.Session()
        
        stats = session.query(schema.GridStats).first()
        if not stats:
            return

        gstats = pickle.loads(stats.pickle_object)      
        #grid_name = gstats.get_name()
        grid_name = 'Grid' # using this instead of grid-name
        self._statistics(grid_name, gstats)

        for cluster in gstats.get_children():
            cluster_name = cluster.get_name()
            # XXX do same for queues ... if wanted
            self._statistics(cluster_name, cluster)


    def _statistics(self, name, stats):

        dbn = os.path.join(self.rrddir, name+'.rrd')
        if not os.path.exists(dbn):
                self.create_rrd(dbn)

        
        totalcpus = stats.get_attribute('totalcpus')
        usedcpus = stats.get_attribute('usedcpus')
        gridrunning = stats.get_attribute('gridrunning')
        running = stats.get_attribute('running')
        gridqueued = stats.get_attribute('gridqueued')
        localqueued = stats.get_attribute('localqueued')
        prelrmsqueued = stats.get_attribute('prelrmsqueued')

        cmd = 'rrdtool update %s -t\
             totalcpus:usedcpus:gridrunning:running:gridqueued:localqueued:prelrmsqueued \
             %d:%d:%d:%d:%d:%d:%d:%d' \
             % (dbn,time.time(), totalcpus, usedcpus, gridrunning, running, 
                gridqueued, localqueued, prelrmsqueued)

        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.debug("Updated RDD database '%s'" % dbn)

        self.create_plots(name)
    
    def generate_plots(self):
        self.statistics()


if __name__ == '__main__':
    import logging.config
    LOG_FILENAME = 'test.log'
    logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)



    rrd_db = '/opt/GridMonitor/gridmonitor/rrd/load/'
    rrd_plot = '/opt/GridMonitor/gridmonitor/public/rrd'

    ld = GridLoad(rrd_db, rrd_plot)
    ld.generate_plots() 
