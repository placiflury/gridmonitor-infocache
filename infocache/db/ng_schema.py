#!/usr/bin/env python
"""
Metadata tables (sqlalchemy) for storing information about
the Nordugrid Information system. 

We assume that the Nordugrid Schema is in use. For the Glue 1.2 
schema the tables might need to be adapted.

"""
__author__="Placi Flury grid@switch.ch"
__date__="02.12.2009"
__version__="1.0.0"

import sqlalchemy as sa
from sqlalchemy.orm import mapper,relation
from datetime import datetime

import db.mon_meta as mon_meta
#import gridmonitor.model.giisdb.meta as mon_meta


t_job = sa.Table("job",mon_meta.metadata,
        sa.Column("globalid",sa.types.VARCHAR(255),primary_key=True),
        sa.Column("globalowner",sa.types.VARCHAR(255), nullable=False),
        sa.Column("status",sa.types.VARCHAR(32), nullable=False),
        sa.Column("jobname",sa.types.VARCHAR(255), nullable=False),
        sa.Column("exitcode",sa.types.SMALLINT),
        sa.Column("cluster_name",None, sa.ForeignKey('cluster.hostname')),
        sa.Column("queue_name",None, sa.ForeignKey('queue.name')),
        sa.Column("cpucount",sa.types.SMALLINT),
        sa.Column("completiontime",sa.types.DateTime),
        sa.Column("proxyexpirationtime",sa.types.DateTime),
        sa.Column("sessiondirerasetime",sa.types.DateTime),
        sa.Column("submissiontime",sa.types.DateTime),
        sa.Column("usedcputime",sa.types.Integer),
        sa.Column("usedwalltime",sa.types.Integer),
        sa.Column("reqcputime",sa.types.Integer),
        sa.Column("reqwalltime",sa.types.Integer),
        sa.Column("stdout",sa.types.VARCHAR(255)),
        sa.Column("stderr",sa.types.VARCHAR(255)),
        sa.Column("submissionui",sa.types.VARCHAR(255)),
        sa.Column("clientsoftware",sa.types.VARCHAR(64)),
        sa.Column("executionnodes",sa.types.VARCHAR(64)),
        sa.Column("cpucount",sa.types.SMALLINT),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

"""
status = [active,passive], tells whether queue can be accessed or not
"""
t_queue = sa.Table("queue",mon_meta.metadata,
        sa.Column('name',sa.types.VARCHAR(255),primary_key=True),
        sa.Column('hostname', None, sa.ForeignKey('cluster.hostname'),primary_key=True),
        sa.Column('pickle_object',sa.types.PickleType),
        sa.Column('status',sa.types.VARCHAR(32)),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

"""
status = [active,passive], tells whether cluster still up or not
response_time : infosys (GRIS)  response time
processing_time: time it takes infosys (GRIS) to report on jobs etc.
"""
t_cluster = sa.Table("cluster",mon_meta.metadata,
        sa.Column('hostname',sa.types.VARCHAR(255), primary_key=True),
        sa.Column('alias',sa.types.VARCHAR(255)),
        sa.Column('pickle_object',sa.types.PickleType),
        sa.Column('status',sa.types.VARCHAR(32)),
        sa.Column('response_time',sa.types.FLOAT,default=-1.0),
        sa.Column('processing_time',sa.types.FLOAT,default=-1.0),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

t_user = sa.Table("user",mon_meta.metadata,
        sa.Column('DN', sa.types.VARCHAR(255),primary_key=True),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

t_user_access = sa.Table("user_access",mon_meta.metadata,
        sa.Column('id',sa.types.Integer, primary_key=True, autoincrement=True),
        sa.Column('hostname', None, sa.ForeignKey('cluster.hostname')), 
        sa.Column('queuename', None, sa.ForeignKey('queue.name')),
        sa.Column('user',None,sa.ForeignKey('user.DN')),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
) 

t_gridstats = sa.Table("gridstats",mon_meta.metadata,
        sa.Column('gridname',sa.types.VARCHAR(32),primary_key=True),
        sa.Column("pickle_object", sa.types.PickleType),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)


t_grisblacklist = sa.Table('grisblacklist', mon_meta.metadata,
        sa.Column('hostname', sa.types.VARCHAR(255),primary_key=True),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
)

t_giis = sa.Table('giis', mon_meta.metadata,
        sa.Column('hostname', sa.types.VARCHAR(255),primary_key=True),
        sa.Column('processing_time',sa.types.FLOAT,default=-1.0),
        sa.Column("db_lastmodified",sa.types.DateTime, default=datetime.utcnow)
) 



class Job(object):
    
    def get_attribute_names(self):
        return ['globalid','jobname','globalowner','status',
                'exitcode','cluster_name','queue_name',
                'cpucount','completiontime','proxyexpirationtime','sessiondirerasetime',
                'submissiontime','usedcputime','usedwalltime','reqcputime','reqwalltime',
                'stdout','stderr','submissionui','clientsoftware','executionnodes','cpucount']


    def get_globalid(self):
        return self.globalid

    def get_globalowner(self):
        return self.globalowner
    
    def get_status(self):
        return self.status

    def get_jobname(self):
        return self.jobname

    def get_exitcode(self):
        return self.exitcode

    def get_cluster_name(self):
        return self.cluster_name

    def get_queue_name(self):
        return self.queue_name

    def get_usedwalltime(self):
        return self.usedwalltime
    
    def get_attribute_values(self,name):
        """ returns a list"""
        attr= eval("self."+ name)
        if not attr:
            return []
        return [attr]

class Queue(object):
    pass

class Cluster(object):
    pass

class User(object):
    pass

class UserAccess(object):
    pass

class GridStats(object):
    pass

class Grisblacklist(object):
    pass

class Giis(object):
    pass


# XXX cascading delete for cluster, queue, user and user_access

# job - cluster/queue (1:1), job.owner - cluster/queue, job.owner -> user_access (1:N)

mapper(Job,t_job, 
        properties=dict(access=relation(UserAccess,
        foreign_keys=[t_job.c.globalowner],
        primaryjoin=(sa.and_(t_job.c.globalowner == t_user_access.c.user,
                    t_job.c.cluster_name == t_user_access.c.hostname,
                    t_job.c.queue_name == t_user_access.c.queuename))))
)
mapper(Cluster,t_cluster,
        properties=dict(queues=relation(Queue, backref='cluster'))
)
mapper(Queue,t_queue)

mapper(User,t_user, 
        properties=dict(access = relation(UserAccess,
        primaryjoin=(t_user.c.DN == t_user_access.c.user)))
)
mapper(UserAccess,t_user_access)
mapper(GridStats,t_gridstats)
mapper(Grisblacklist,t_grisblacklist)
mapper(Giis,t_giis)
