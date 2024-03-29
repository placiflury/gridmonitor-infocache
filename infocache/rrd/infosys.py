"""
Generation of GRIS and GIIS  RRD plots.
"""
__author__="Placi Flury grid@switch.ch"
__date__="22..01.2010"
__version__="0.2.0"

import logging
import time
import os.path
import  commands # XXX change to subprocess
from datetime import datetime

from infocache.db import meta, schema

class GrisGiis(object):

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
        now = time.time() - 3600  # let's be on the save side
        cmd = "rrdtool create %s --step 120 --start %d\
         DS:response_time:GAUGE:240:0:U\
         DS:processing_time:GAUGE:240:0:U\
         RRA:AVERAGE:0.5:1:720\
         RRA:AVERAGE:0.5:15:336\
         RRA:AVERAGE:0.5:360:365\
         RRA:MAX:0.5:1:720\
         RRA:MAX:0.5:15:336:\
         RRA:MAX:0.5:360:365" %  (dbname, now) 

        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.info("Created RDD database '%s'" % dbname)
        

    def _make_cmd(self, fig_name, start, end, cluster_name, _type, rrd_file, _kind = 'processing'):
        if _kind == 'processing':
            cmd = "rrdtool graph %s -s %d -e %d --title='%s processing times for %s' \
                     DEF:proc=%s:processing_time:AVERAGE \
                     VDEF:proc_max=proc,MAXIMUM \
                     VDEF:proc_avg=proc,AVERAGE \
                     VDEF:proc_min=proc,MINIMUM \
                     -c BACK#F8F7FF \
                     -c CANVAS#4682B4 \
                     -c SHADEA#F0EDFF \
                     -c SHADEB#E9E4FF \
                     -c GRID#CCFFFF \
                     -c MGRID#FFFFC0 \
                     -c ARROW#000000 \
                     -c FONT#000000 \
                     -h 80  \
                     -v \[sec\] \
                     GPRINT:proc_min:'MIN\:'%%2.1lf%%Ss\
                     GPRINT:proc_avg:'AVG\:'%%2.1lf%%Ss\
                     GPRINT:proc_max:'MAX\:'%%2.1lf%%Ss\
                     AREA:proc#CCFFFF:'Processing Time'" % \
                        (fig_name, start, end, _type, cluster_name, rrd_file)
        else:
             cmd = "rrdtool graph %s -s %d -e %d --title='%s response times for %s' \
                     DEF:resp=%s:response_time:AVERAGE \
                     VDEF:resp_max=resp,MAXIMUM \
                     VDEF:resp_avg=resp,AVERAGE \
                     VDEF:resp_min=resp,MINIMUM \
                     -c BACK#F8F7FF \
                     -c CANVAS#4682B4 \
                     -c SHADEA#F0EDFF \
                     -c SHADEB#E9E4FF \
                     -c GRID#CCFFFF \
                     -c MGRID#FFFFC0 \
                     -c ARROW#000000 \
                     -c FONT#000000 \
                     -h 80  \
                     -v \[sec\] \
                     GPRINT:resp_min:'MIN\:'%%2.1lf%%Ss\
                     GPRINT:resp_avg:'AVG\:'%%2.1lf%%Ss\
                     GPRINT:resp_max:'MAX\:'%%2.1lf%%Ss\
                     AREA:resp#CCFFFF:'Processing Time'" % \
                        (fig_name, start, end, _type, cluster_name, rrd_file)
        return cmd

    def create_plots(self, cluster_name, _type='GRIS'):
        
        rrd_file = os.path.join(self.rrddir, cluster_name+'.rrd')

        fig24_name_r = os.path.join(self.plotdir, cluster_name+'_h24r.png') # 24hours plot (response times)
        fig24_name_p = os.path.join(self.plotdir, cluster_name+'_h24p.png') # 24hours plot (processing times)
        h24_e = time.time()
        h24_s = h24_e - 24 * 3600

        cmd = self._make_cmd(fig24_name_r, h24_s, h24_e, cluster_name, _type, rrd_file, 'response')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        cmd = self._make_cmd(fig24_name_p, h24_s, h24_e, cluster_name, _type, rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        figw1_name_r = os.path.join(self.plotdir, cluster_name+'_w1r.png') # 1 week  plot
        figw1_name_p = os.path.join(self.plotdir, cluster_name+'_w1p.png') 
        hw1_e = time.time()
        hw1_s = hw1_e - 24 * 3600 * 7 
        
        cmd = self._make_cmd(figw1_name_r, hw1_s, hw1_e, cluster_name, _type, rrd_file, 'response')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

        cmd = self._make_cmd(figw1_name_p, hw1_s, hw1_e, cluster_name, _type, rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        figy1_name_r = os.path.join(self.plotdir, cluster_name+'_y1r.png') # 1 year  plot
        figy1_name_p = os.path.join(self.plotdir, cluster_name+'_y1p.png')
        hy1_e = time.time()
        hy1_s = hy1_e - 24 * 3600 * 365

        cmd = self._make_cmd(figy1_name_r, hy1_s, hy1_e, cluster_name, _type, rrd_file, 'response')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        cmd = self._make_cmd(figy1_name_p, hy1_s, hy1_e, cluster_name, _type, rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

 
    def gris(self):
        """ Populating RRD with Gris response and processing times. """

        self.log.debug("Populating RDD with gris response and processing times.")
        session = meta.Session()
       
        """
        query = session.query(schema.NGCluster)
        clusters= query.filter_by(status='active').all()
        """
        
        for cluster in session.query(schema.NGCluster).all():
            # check whether rrd db exists 
            dbn = os.path.join(self.rrddir, cluster.hostname+'.rrd')
            if not os.path.exists(dbn):
                self.create_rrd(dbn) 

            cm = cluster.get_metadata()
            if cm.get_status() != 'active':
                continue
            response_time = cm.get_response_time()
            processing_time = cm.get_processing_time()


            dt = cluster.db_lastmodified # GMT time 
            t_epoch = time.mktime(dt.timetuple()) + \
                 dt.microsecond/1000000.0 - time.timezone # datetime does not care about microsecs

            cmd = 'rrdtool update %s -t response_time:processing_time %d:%f:%f' \
                % (dbn, t_epoch, response_time, processing_time)
         
            (code,output) = commands.getstatusoutput(cmd)
            if code != 0:
                self.log.error( output)
            else:
                self.log.debug("UpdatedRDD database '%s'" % dbn)

            self.create_plots(cluster.hostname)
        session.expunge_all()

    def giis(self):
        """ Populating RDD with GIIS response and processing times. """  

        self.log.debug("Populating RDD with GIIS response and processing times.")
        session = meta.Session()
        
        """
        last_query_time = datetime.utcfromtimestamp(time.time() - 120)  # assuming 120 sec query cycle
        giises = session.query(schema.GiisMeta).filter(schema.GiisMeta.db_lastmodified>=last_query_time).all()
        """
        
        for giis in session.query(schema.GiisMeta).all():
            dbn = os.path.join(self.rrddir, giis.get_hostname()+'.rrd')
            if not os.path.exists(dbn):
                self.create_rrd(dbn) # we use same func for giis
            response_time = giis.get_response_time()
            processing_time = giis.get_processing_time()
            
            dt = giis.get_db_lastmodified() # GMT time 
            t_epoch = time.mktime(dt.timetuple()) + \
                 dt.microsecond/1000000.0 - time.timezone # datetime does not care about microsecs
            cmd = 'rrdtool update %s -t response_time:processing_time %d:%f:%f' \
                % (dbn, t_epoch, response_time, processing_time)
         
            (code, output) = commands.getstatusoutput(cmd)
            if code !=0:
                self.log.error( output)
            else:
                self.log.debug("UpdatedRDD database '%s'" % dbn)
            
            self.create_plots(giis.get_hostname(), _type='GIIS')
    
    def generate_plots(self):
        self.gris()
        self.giis() 
