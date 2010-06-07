"""
Generation of GRIS and GIIS  RRD plots.
"""
__author__="Placi Flury placi.flury@switch.ch"
__date__="22..01.2010"
__version__="0.1.0"

import logging
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR
from sqlalchemy import orm

import db.mon_meta as mon_meta
import db.ng_schema as schema
import time, os.path, commands
from datetime import datetime



class GrisGiis(object):

    def __init__(self,rrddir, plotdir):
        self.log = logging.getLogger(__name__)
        self.Session = orm.scoped_session(mon_meta.Session)
        self.rrddir = rrddir
        self.plotdir = plotdir
        self.log.debug("Initialization finished")
        
    
    def create_rrd(self,dbname):
        """
        RRA: 6 hours (120 * 180)   with 120 sec resolution
        RRA: 1 week (120 * 30  * 168)  with 1 hour resolution
        RRA: 12 months (120 * 360 * 365 ) with 12 hours resolution
        """
        now = time.time() - 3600  # let's be on the save side
        cmd = "rrdtool create %s --step 120 --start %d\
         DS:response_time:GAUGE:240:0:U\
         DS:processing_time:GAUGE:240:0:U\
         RRA:AVERAGE:0.5:1:180\
         RRA:AVERAGE:0.5:30:168\
         RRA:AVERAGE:0.5:360:365\
         RRA:MAX:0.5:1:180\
         RRA:MAX:0.5:30:168:\
         RRA:MAX:0.5:360:365" %  (dbname,now) 

        (code,output) = commands.getstatusoutput(cmd)
        if code !=0:
            self.log.error( output)
        else:
            self.log.info("Created RDD database '%s'" % dbname)
        

    def _make_cmd(self,fig_name,start,end,cluster_name,type,rrd_file):
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
                 AREA:proc#CCFFFF:'Processing Time'" % (fig_name, start, end, type, cluster_name,rrd_file)
        return cmd

    def create_plots(self,cluster_name, type='GRIS'):
        
        rrd_file = os.path.join(self.rrddir,cluster_name+'.rrd')
        fig6_name = os.path.join(self.plotdir,cluster_name+'_h6.png') # 6hours plot
        h6_e = time.time()
        h6_s = h6_e - 6 * 3600

        cmd = self._make_cmd(fig6_name,h6_s,h6_e,cluster_name,type,rrd_file)
        (code,output) = commands.getstatusoutput(cmd)
        if code !=0:
            self.log.error( output)
        
        figw1_name = os.path.join(self.plotdir,cluster_name+'_w1.png') # 1 week  plot
        hw1_e = time.time()
        hw1_s = hw1_e - 24 * 3600 * 7 

        cmd = self._make_cmd(figw1_name,hw1_s,hw1_e,cluster_name,type,rrd_file)
        (code,output) = commands.getstatusoutput(cmd)
        if code !=0:
            self.log.error( output)
        
        figy1_name = os.path.join(self.plotdir,cluster_name+'_y1.png') # 1 year  plot
        hy1_e = time.time()
        hy1_s = hy1_e - 24 * 3600 * 365

        cmd = self._make_cmd(figy1_name,hy1_s,hy1_e,cluster_name,type,rrd_file)
        (code,output) = commands.getstatusoutput(cmd)
        if code !=0:
            self.log.error( output)

 
    def gris(self):
        self.log.debug("Populating RDD with gris response and processing times.")
        session = self.Session()
        query = session.query(schema.Cluster)
        
        clusters= query.filter_by(status='active').all()
        
        for cluster in clusters:
            # check whether rrd db exists 
            dbn = os.path.join(self.rrddir,cluster.hostname+'.rrd')
            if not os.path.exists(dbn):
                self.create_rrd(dbn) 
            response_time = cluster.response_time
            processing_time = cluster.processing_time
            dt = cluster.db_lastmodified # GMT time 
            t_epoch = time.mktime(dt.timetuple()) + \
                 dt.microsecond/1000000.0 - time.timezone # datetime does not care about microsecs
            cmd = 'rrdtool update %s -t response_time:processing_time %d:%f:%f' \
                % (dbn, t_epoch, response_time, processing_time)
         
            (code,output) = commands.getstatusoutput(cmd)
            if code !=0:
                self.log.error( output)
            else:
                self.log.debug("UpdatedRDD database '%s'" % dbn)

            self.create_plots(cluster.hostname)

        session.clear()

    def giis(self):
        self.log.debug("Populating RDD with gris response and processing times.")
        session = self.Session()
        last_query_time = datetime.utcfromtimestamp(time.time() - 120)  # assuming 120 sec query cycle
        giises = session.query(schema.Giis).filter(schema.Giis.db_lastmodified>=last_query_time).all()
        
        for giis in giises:
            dbn = os.path.join(self.rrddir,giis.hostname+'.rrd')
            if not os.path.exists(dbn):
                self.create_rrd(dbn) # we use same func for giis
            response_time = giis.processing_time
            processing_time = giis.processing_time
            
            dt = giis.db_lastmodified # GMT time 
            t_epoch = time.mktime(dt.timetuple()) + \
                 dt.microsecond/1000000.0 - time.timezone # datetime does not care about microsecs
            cmd = 'rrdtool update %s -t response_time:processing_time %d:%f:%f' \
                % (dbn, t_epoch, response_time, processing_time)
         
            (code,output) = commands.getstatusoutput(cmd)
            if code !=0:
                self.log.error( output)
            else:
                self.log.debug("UpdatedRDD database '%s'" % dbn)
            
            self.create_plots(giis.hostname, type='GIIS')
    
    def generate_plots(self):
        self.gris()
        self.giis() 