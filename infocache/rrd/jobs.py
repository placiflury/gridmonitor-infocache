"""
Generation of RRD plots of job status.
"""
__author__ = "Placi Flury grid@switch.ch"
__date__ = "22.01.2010"
__version__ = "0.1.0"

import logging
import time
import os.path
import  commands # XXX change to subprocess
from datetime import datetime
from sqlalchemy import and_ as AND
from sqlalchemy import or_ as OR

from  infocache.db import meta, schema

class Jobs(object):
    """
    +----....---|<---T_INTERVAL-->|------.... ---|----> t
               t_s               t_e            t_now
    
    plotting of jobs that are in a final status. Since we do not control the 
    processing time for collecting job statuses and all other time-effects events 
    like time skews between systems, infosys instabilities (clusters appearing and 
    disappearing, we do only collect data of jobs that have completed within the 
    interval of t_s and t_e. 

    t_s is SAFETY_DELAY before the current time t_now._

    """
    
    SAFETY_DELAY = 3600     # in seconds (see above for more details)
    T_INTERVAL = 120        # in seconds -> should stay to 120s, which is RRD resolution
    FINAL_JOBS_RRD = 'grid_final_jobs.rrd'  # RRD for final grid jobs

    def __init__(self, rrddir, plotdir):
        self.log = logging.getLogger(__name__)
        self.rrddir = rrddir
        self.plotdir = plotdir
        t_now = time.time()
        self.last_t_s_epoch_eff = t_now - Jobs.SAFETY_DELAY  # minimize  time-skew effects
        self.log.debug("Initialization finished")
    
    
    def create_rrd(self, dbname):
        """
        RRA: 24 hours (120 * 720)   with 120 sec resolution
        RRA: 1 week (120 * 15  * 336)  with 30 min resolution
        RRA: 12 months (120 * 360 * 365 ) with 12 hours resolution
        """
        now = time.time() - Jobs.SAFETY_DELAY - 3600  # let's be on the save side
        cmd = "rrdtool create %s --step 120 --start %d\
         DS:failed:GAUGE:240:0:U\
         DS:failed_walltime:GAUGE:240:0:U\
         DS:killed:GAUGE:240:0:U\
         DS:killed_walltime:GAUGE:240:0:U\
         DS:finished:GAUGE:240:0:U\
         DS:finished_walltime:GAUGE:240:0:U\
         DS:deleted:GAUGE:240:0:U\
         DS:deleted_walltime:GAUGE:240:0:U\
         DS:lost:GAUGE:240:0:U\
         DS:lost_walltime:GAUGE:240:0:U\
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
   

    def _get_num_walltime(self, joblist):
        """
        XXX break it down per  cluster
        """
        if not joblist:
            return 0, 0

        walltime=0
        for job in joblist:
            w = job.used_wall_time
            if w:
                walltime += w

        return len(joblist), walltime
        
 
    def final_jobs(self):
        """
        Creation of RRD db with gridjobs that are in a final state
        """
        self.log.debug("Populating RDD with jobs in final status.")
        session = meta.Session()
        query = session.query(schema.NGJob)
       
        last_t_s_epoch = time.time() - Jobs.SAFETY_DELAY 

        # correct time-skew if we lag behind more than Jobs.T_INTERVAL
        if (last_t_s_epoch - self.last_t_s_epoch_eff) > Jobs.T_INTERVAL:
            self.log.debug("Correcting time skew of %d" % (last_t_s_epoch - self.last_t_s_epoch_eff))
            t_e_epoch = last_t_s_epoch + Jobs.T_INTERVAL 
        else:
            t_e_epoch = self.last_t_s_epoch_eff + Jobs.T_INTERVAL
        
        t_s = datetime.utcfromtimestamp(self.last_t_s_epoch_eff)
        t_e = datetime.utcfromtimestamp(t_e_epoch)
        self.last_t_s_epoch_eff = t_e_epoch

        dbn = os.path.join(self.rrddir, Jobs.FINAL_JOBS_RRD)
        if not os.path.exists(dbn):
            self.create_rrd(dbn) 
        
        # jobs that failed:  FLD_DELETED -> we do not really care
        failed_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.completion_time > t_s,
            schema.NGJob.completion_time <= t_e, 
            OR(schema.NGJob.status == 'FAILED', schema.NGJob.status == 'FLD_DELETED',
            schema.NGJob.status == 'FLD_FETCHED'))).all()
        nfailed, wfailed = self._get_num_walltime(failed_jobs)

        
        # jobs that got killed: KIL_DELETED -> we do not really care 
        killed_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.completion_time > t_s,
            schema.NGJob.completion_time <= t_e, 
            OR(schema.NGJob.status == 'KILLED',schema.NGJob.status == 'KIL_DELETED',
            schema.NGJob.status == 'KIL_FETCHED'))).all()
        nkilled, wkilled = self._get_num_walltime(killed_jobs)
        

        #finished jobs (fetched and not yet fetched by user)
        finished_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.completion_time > t_s,
            schema.NGJob.completion_time <= t_e, 
            OR(schema.NGJob.status == 'FINISHED', schema.NGJob.status =='FIN_FETCHED'))).all()
        nfinished, wfinished = self._get_num_walltime(finished_jobs)

        # finished jobs that got deleted before they got fetched-> wasted jobs
        wasted_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.sessiondir_erase_time > t_s,
            schema.NGJob.sessiondir_erase_time <= t_e, schema.NGJob.status == 'FIN_DELETED')).all()
        ndeleted, wdeleted = self._get_num_walltime(wasted_jobs)
        
        # jobs we lost track
        lost_jobs = session.query(schema.NGJob).filter(AND(schema.NGJob.completion_time > t_s,
            schema.NGJob.completion_time <= t_e, schema.NGJob.status == 'LOST')).all()
        nlost, wlost = self._get_num_walltime(lost_jobs)
        self.log.debug("JOB-time: %s" % datetime.utcfromtimestamp(t_e_epoch))
        self.log.debug("JOBS: faild: %d (%d) killd: %d (%d) finishd %d (%d), deletd: %d (%d), lost: %d (%d)"
             % (nfailed, wfailed, nkilled, wkilled, nfinished, wfinished, ndeleted, wdeleted,nlost,wlost))
        
        cmd = 'rrdtool update %s -t \
             failed:failed_walltime:killed:killed_walltime:finished:finished_walltime:deleted:deleted_walltime:lost:lost_walltime \
             %d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d' \
             % (dbn, t_e_epoch, nfailed, wfailed, nkilled, 
            wkilled, nfinished, wfinished, ndeleted, 
            wdeleted, nlost, wlost)
       
 
        (code,output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        else:
            self.log.debug("Updated RDD database '%s'" % dbn)

        self.create_plots()
        session.expunge_all()

    
    def generate_plots(self):
        self.final_jobs()
        

    def _make_cmd(self, fig_name, start, end, rrd_file, _type='num_jobs'):
   
        if _type== 'num_jobs':
            cmd = "rrdtool graph %s -s %d -e %d --title='Number of Grid jobs per minute  in final states' \
                 DEF:n2fined=%s:finished:AVERAGE \
                 DEF:n2killed=%s:killed:AVERAGE \
                 DEF:n2failed=%s:failed:AVERAGE \
                 DEF:n2deleted=%s:deleted:AVERAGE \
                 DEF:n2lost=%s:lost:AVERAGE \
                 CDEF:nfined=n2fined,0.5,\* \
                 CDEF:nkilled=n2killed,0.5,\* \
                 CDEF:nfailed=n2failed,0.5,\* \
                 CDEF:ndeleted=n2deleted,0.5,\* \
                 CDEF:nlost=n2lost,0.5,\* \
                 VDEF:nfined_max=nfined,MAXIMUM \
                 VDEF:nfined_avg=nfined,AVERAGE \
                 VDEF:nfined_min=nfined,MINIMUM \
                 VDEF:nkilled_max=nkilled,MAXIMUM \
                 VDEF:nkilled_avg=nkilled,AVERAGE \
                 VDEF:nkilled_min=nkilled,MINIMUM \
                 VDEF:nfailed_max=nfailed,MAXIMUM \
                 VDEF:nfailed_avg=nfailed,AVERAGE \
                 VDEF:nfailed_min=nfailed,MINIMUM \
                 VDEF:ndeleted_max=ndeleted,MAXIMUM \
                 VDEF:ndeleted_avg=ndeleted,AVERAGE \
                 VDEF:ndeleted_min=ndeleted,MINIMUM \
                 VDEF:nlost_max=nlost,MAXIMUM \
                 VDEF:nlost_avg=nlost,AVERAGE \
                 VDEF:nlost_min=nlost,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800  \
                 -v \[jobs\] \
                 COMMENT:\"                  \"\
                 COMMENT:\"Minimum  \"\
                 COMMENT:\"Average  \"\
                 COMMENT:\"Maximum  \l\"\
                 COMMENT:\"    \"\
                 AREA:nkilled#990000:'KILLED'\
                 GPRINT:nkilled_min:\"     %%2.2lf\" \
                 GPRINT:nkilled_avg:\"     %%2.2lf\" \
                 GPRINT:nkilled_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:ndeleted#FFCC00:'DELETED':STACK\
                 GPRINT:ndeleted_min:\"    %%2.2lf\" \
                 GPRINT:ndeleted_avg:\"     %%2.2lf\" \
                 GPRINT:ndeleted_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:nfailed#FF6633:'FAILED':STACK\
                 GPRINT:nfailed_min:\"     %%2.2lf\" \
                 GPRINT:nfailed_avg:\"     %%2.2lf\" \
                 GPRINT:nfailed_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:nlost#B4D5D5:'INDEFINITE':STACK\
                 GPRINT:nlost_min:\" %%2.2lf\" \
                 GPRINT:nlost_avg:\"     %%2.2lf\" \
                 GPRINT:nlost_max:\"      %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:nfined#CCFFFF:'FINISHED:STACK'\
                 GPRINT:nfined_min:\"   %%2.2lf\" \
                 GPRINT:nfined_avg:\"     %%2.2lf\" \
                 GPRINT:nfined_max:\"      %%2.2lf\l\"" % \
                    (fig_name, start, end, rrd_file,
                    rrd_file, rrd_file, rrd_file, rrd_file)
        else:
            cmd  = "rrdtool graph %s -s %d -e %d --title='Walltime of Grid jobs in final states' \
                 DEF:wfined=%s:finished_walltime:AVERAGE \
                 DEF:wkilled=%s:killed_walltime:AVERAGE \
                 DEF:wfailed=%s:failed_walltime:AVERAGE \
                 DEF:wdeleted=%s:deleted_walltime:AVERAGE \
                 DEF:wlost=%s:lost_walltime:AVERAGE \
                 VDEF:wfined_max=wfined,MAXIMUM \
                 VDEF:wfined_avg=wfined,AVERAGE \
                 VDEF:wfined_min=wfined,MINIMUM \
                 VDEF:wkilled_max=wkilled,MAXIMUM \
                 VDEF:wkilled_avg=wkilled,AVERAGE \
                 VDEF:wkilled_min=wkilled,MINIMUM \
                 VDEF:wfailed_max=wfailed,MAXIMUM \
                 VDEF:wfailed_avg=wfailed,AVERAGE \
                 VDEF:wfailed_min=wfailed,MINIMUM \
                 VDEF:wdeleted_max=wdeleted,MAXIMUM \
                 VDEF:wdeleted_avg=wdeleted,AVERAGE \
                 VDEF:wdeleted_min=wdeleted,MINIMUM \
                 VDEF:wlost_max=wlost,MAXIMUM \
                 VDEF:wlost_avg=wlost,AVERAGE \
                 VDEF:wlost_min=wlost,MINIMUM \
                 -c BACK#F8F7FF \
                 -c CANVAS#4682B4 \
                 -c SHADEA#F0EDFF \
                 -c SHADEB#E9E4FF \
                 -c GRID#CCFFFF \
                 -c MGRID#FFFFC0 \
                 -c ARROW#000000 \
                 -c FONT#000000 \
                 -w 800  \
                 -v \[min\] \
                 COMMENT:\"                  \"\
                 COMMENT:\"Minimum  \"\
                 COMMENT:\"Average  \"\
                 COMMENT:\"Maximum  \l\"\
                 COMMENT:\"    \"\
                 AREA:wkilled#990000:'KILLED'\
                 GPRINT:wkilled_min:\"      %%2.2lf\" \
                 GPRINT:wkilled_avg:\"     %%2.2lf\" \
                 GPRINT:wkilled_max:\"     %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:wdeleted#FFCC00:'DELETED':STACK\
                 GPRINT:wdeleted_min:\"     %%2.2lf\" \
                 GPRINT:wdeleted_avg:\"     %%2.2lf\" \
                 GPRINT:wdeleted_max:\"     %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:wfailed#FF6633:'FAILED':STACK\
                 GPRINT:wfailed_min:\"      %%2.2lf\" \
                 GPRINT:wfailed_avg:\"     %%2.2lf\" \
                 GPRINT:wfailed_max:\"     %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:wlost#B4D5D5:'INDEFINITE':STACK\
                 GPRINT:wlost_min:\"  %%2.2lf\" \
                 GPRINT:wlost_avg:\"     %%2.2lf\" \
                 GPRINT:wlost_max:\"     %%2.2lf\l\"\
                 COMMENT:\"    \"\
                 AREA:wfined#CCFFFF:'FINISHED:STACK'\
                 GPRINT:wfined_min:\"    %%2.2lf\" \
                 GPRINT:wfined_avg:\"     %%2.2lf\" \
                 GPRINT:wfined_max:\"     %%2.2lf\l\"" % \
                     (fig_name, start, end, rrd_file,
                    rrd_file, rrd_file, rrd_file, rrd_file)

        return cmd

    def create_plots(self):
        
        rrd_file = os.path.join(self.rrddir, Jobs.FINAL_JOBS_RRD)
        h24_e = time.time()
        h24_s = h24_e - 24 * 3600
        
        fig24_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'nj_h24.png') # 24hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        fig24_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'wj_h24.png') # 24hours plot
        cmd = self._make_cmd(fig24_name, h24_s, h24_e, rrd_file, _type='walltime')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        hw1_e = time.time()
        hw1_s = hw1_e - 24 * 3600 * 7 

        figw1_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'nj_w1.png') 
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        figw1_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'wj_w1.png')
        cmd = self._make_cmd(figw1_name, hw1_s, hw1_e, rrd_file, _type='walltime')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)
        
        hy1_e = time.time()
        hy1_s = hy1_e - 24 * 3600 * 365
        
        figy1_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'nj_y1.png') # one year  plot
        cmd = self._make_cmd(figy1_name, hy1_s, hy1_e,  rrd_file)
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

        figy1_name = os.path.join(self.plotdir, Jobs.FINAL_JOBS_RRD[:-4] + 'wj_y1.png') # one year  plot
        cmd = self._make_cmd(figy1_name, hy1_s, hy1_e,  rrd_file, _type='walltime')
        (code, output) = commands.getstatusoutput(cmd)
        if code != 0:
            self.log.error( output)

 
