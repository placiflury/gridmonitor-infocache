"""
Metadata tables (sqlalchemy) for storing information about
the Nordugrid Information system. 

We assume that the Nordugrid Schema is in use. For the Glue 1.2 
schema the tables might need to be adapted.
"""

__author__="Placi Flury grid@switch.ch"
__date__="4.04.2011"
__version__="1.2.0"

import sqlalchemy as sa
from sqlalchemy.orm import mapper, relationship
from datetime import datetime


import meta
from cluster import NGCluster
from queue import NGQueue
from job import NGJob
from giis import GiisMeta


"""
status = [active,passive], tells whether cluster still up or not
response_time : infosys (GRIS)  response time
processing_time: time it takes infosys (GRIS) to report on jobs etc.
"""
t_cluster = sa.Table("cluster",meta.metadata,
        sa.Column('hostname',sa.types.VARCHAR(255), primary_key=True),
        sa.Column('alias',sa.types.VARCHAR(255)),
        sa.Column('status',sa.types.VARCHAR(31)),
        sa.Column('response_time',sa.types.FLOAT,default=-1.0),
        sa.Column('processing_time',sa.types.FLOAT,default=-1.0),
        sa.Column('blacklisted', sa.types.BOOLEAN),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow), 
        sa.Column('comment',sa.types.VARCHAR(511)),
        sa.Column('owners',sa.types.VARCHAR(511)),
        sa.Column('support',sa.types.VARCHAR(511)),
        sa.Column('contact',sa.types.VARCHAR(511)),
        sa.Column('location',sa.types.VARCHAR(255)),
        sa.Column('issuer_ca',sa.types.VARCHAR(511)),
        sa.Column('cert_expiration', sa.types.VARCHAR(63)),
        sa.Column('architecture',sa.types.VARCHAR(255)),
        sa.Column('homogeneity',sa.types.BOOLEAN),
        sa.Column('node_cpu',sa.types.VARCHAR(255)),
        sa.Column('node_memory',sa.types.Integer),
        sa.Column('middlewares',sa.types.VARCHAR(511)),
        sa.Column('operating_systems',sa.types.VARCHAR(255)),
        sa.Column('lrms_config',sa.types.VARCHAR(255)),
        sa.Column('lrms_type',sa.types.VARCHAR(255)),
        sa.Column('lrms_version',sa.types.VARCHAR(255)),
        sa.Column('prelrms_queued',sa.types.Integer, default=0),
        sa.Column('queued_jobs',sa.types.Integer, default = 0),
        sa.Column('total_jobs',sa.types.Integer, default = 0),
        sa.Column('used_cpus',sa.types.Integer, default = 0),
        sa.Column('total_cpus',sa.types.Integer, default = 0),
        sa.Column('session_dir',sa.types.VARCHAR(255)),
        sa.Column('cache',sa.types.VARCHAR(255)),
        sa.Column('benchmarks',sa.types.VARCHAR(255)),
        sa.Column('runtime_environments',sa.types.TEXT)
)

t_queue = sa.Table('queue', meta.metadata,
        sa.Column('name', sa.types.VARCHAR(255), primary_key=True),
        sa.Column('hostname', None, sa.ForeignKey('cluster.hostname'), primary_key=True),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow),
        sa.Column('status',sa.types.VARCHAR(127)),
        sa.Column('comment',sa.types.VARCHAR(511)),
        sa.Column('cpu_freq',sa.types.Float),
        sa.Column('min_cpu_time',sa.types.Integer),
        sa.Column('default_cpu_time',sa.types.Integer),
        sa.Column('max_cpu_time',sa.types.Integer),
        sa.Column('max_total_cpu_time',sa.types.Integer),
        sa.Column('min_wall_time',sa.types.Integer),
        sa.Column('default_wall_time',sa.types.Integer),
        sa.Column('max_wall_time',sa.types.Integer),
        sa.Column('grid_queued',sa.types.Integer, default=0),
        sa.Column('grid_running',sa.types.Integer, default=0),
        sa.Column('local_queued',sa.types.Integer, default=0),
        sa.Column('homogeneity',sa.types.BOOLEAN),
        sa.Column('max_queuable',sa.types.Integer),
        sa.Column('max_running',sa.types.Integer),
        sa.Column('max_user_run',sa.types.Integer),
        sa.Column('node_cpu',sa.types.VARCHAR(255)),
        sa.Column('node_memory',sa.types.Integer),
        sa.Column('prelrms_queued',sa.types.Integer, default=0),
        sa.Column('queued',sa.types.Integer, default=0),
        sa.Column('running',sa.types.Integer, default=0),
        sa.Column('total_cpus',sa.types.Integer, default=0),
        sa.Column('scheduling_policy',sa.types.VARCHAR(63))
)



t_job = sa.Table("job",meta.metadata,
        sa.Column("global_id", sa.types.VARCHAR(255), primary_key=True),
        sa.Column("global_owner", sa.types.VARCHAR(255), nullable=True),
        sa.Column("status", sa.types.VARCHAR(127), nullable=True),
        sa.Column("job_name", sa.types.VARCHAR(255)),
        sa.Column("client_software", sa.types.VARCHAR(63)),
        sa.Column("cluster_name", None, sa.ForeignKey('cluster.hostname')),
        sa.Column("queue_name", None, sa.ForeignKey('queue.name')),
        sa.Column("completion_time", sa.types.DateTime),
        sa.Column("cpu_count", sa.types.SMALLINT),
        sa.Column("sessiondir_erase_time", sa.types.DateTime),
        sa.Column("errors", sa.types.VARCHAR(1023)),
        sa.Column("execution_nodes", sa.types.VARCHAR(511)),
        sa.Column("exit_code", sa.types.SMALLINT),
        sa.Column("gmlog", sa.types.VARCHAR(511)),
        sa.Column("proxy_expiration_time", sa.types.DateTime),
        sa.Column("queue_rank", sa.types.SMALLINT),
        sa.Column("requested_cpu_time", sa.types.Integer),
        sa.Column("requested_wall_time", sa.types.Integer),
        sa.Column('runtime_environments',sa.types.VARCHAR(1024)),
        sa.Column("stderr", sa.types.VARCHAR(255)),
        sa.Column("stdin", sa.types.VARCHAR(255)),
        sa.Column("stdout", sa.types.VARCHAR(255)),
        sa.Column("submission_time", sa.types.DateTime),
        sa.Column("submission_ui", sa.types.VARCHAR(127)),
        sa.Column("used_cpu_time", sa.types.Integer),
        sa.Column("used_memory", sa.types.Integer),
        sa.Column("used_wall_time", sa.types.Integer),
        sa.Column("db_lastmodified", sa.types.DateTime, default=datetime.utcnow)
)

t_giis = sa.Table('giis', meta.metadata,
        sa.Column('hostname', sa.types.VARCHAR(255), primary_key=True),
        sa.Column('port', sa.types.SMALLINT),
        sa.Column('status',sa.types.VARCHAR(31)),
        sa.Column('mds_vo_name', sa.types.VARCHAR(255)),
        sa.Column('response_time', sa.types.FLOAT, default=-1.0),
        sa.Column('processing_time', sa.types.FLOAT, default=-1.0),
        sa.Column('blacklisted', sa.types.BOOLEAN, default = False),
        sa.Column("db_lastmodified", sa.types.DateTime, default=datetime.utcnow)
)

t_gridstats = sa.Table("gridstats",meta.metadata,
        sa.Column('gridname',sa.types.VARCHAR(32),primary_key=True),
        sa.Column("pickle_object", sa.types.PickleType),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

"""
t_user = sa.Table("user",meta.metadata,
        sa.Column('DN', sa.types.VARCHAR(255),primary_key=True),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)
"""


t_user_access = sa.Table("user_access",meta.metadata,
        sa.Column('hostname', None, sa.ForeignKey('cluster.hostname'), primary_key=True),
        sa.Column('queuename', None, sa.ForeignKey('queue.name'), primary_key = True),
        sa.Column('user', sa.types.VARCHAR(255), primary_key = True),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)



class UserAccess(object):
    
    def __init__(self, dn, hostname, queuename):
        self.user = dn
        self.hostname = hostname
        self.queuename = queuename
        self.db_lastmodified = datetime.utcnow()
        

class GridStats(object):

    def __init__(self, gridname):
            self.gridname = gridname


mapper(GridStats, t_gridstats)
mapper(GiisMeta, t_giis)
mapper(NGJob, t_job)

"""
mapper(NGJob,t_job,
        properties=dict(access=relationship(UserAccess,
        foreign_keys=[t_job.c.globalowner],
        primaryjoin=(sa.and_(t_job.c.globalowner == t_user_access.c.user,
                    t_job.c.cluster_name == t_user_access.c.hostname,
                    t_job.c.queue_name == t_user_access.c.queuename))))
)
"""

mapper(UserAccess, t_user_access)

mapper(NGQueue, t_queue, 
        properties = dict(authusers = relationship(UserAccess, backref='queues', cascade='delete')))

mapper(NGCluster,t_cluster,
        properties = dict(queues = relationship(NGQueue, backref='cluster', cascade="delete")))
