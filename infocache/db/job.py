import cPickle
from datetime import datetime
from gridmonitor.model.api.job_api import JobApi
from infocache.errors.db import Input_Error

class NGJob(object, JobApi):
    """ Class for storing information about a
        NorduGrid ARC queue. Supposedly the 
        store information is populated from the 
        ARC information system. 
    """
    # dict of valid attributes each listed as tupple (<name>, <max_len>)
    ATTRIBUTES = dict(global_id= 255,
        global_owner=255,
        status = 127,
        job_name = 255,
        client_software = 63,
        cluster_name = 255,
        queue_name  = 255,
        completion_time = None,
        cpu_count = None,
        sessiondir_erase_time = None,
        errors = 1023,
        execution_nodes = 1023,
        exit_code = None,
        gmlog = 511,
        proxy_expiration_time = None,
        queue_rank = None,
        requested_cpu_time = None,
        requested_wall_time = None,
        runtime_environments = 1024,
        stderr = 255,
        stdin = 255,
        stdout = 255, 
        submission_time = None,
        submission_ui = 127, 
        used_cpu_time = None,
        used_memory = None,
        used_wall_time = None)
    
    # list of attributes which are stored pickled
    PICKLED = ['runtime_environments', 'execution_nodes'] 

    def __add_attribute(self, name, value):
        """ Adds a new attribute with 'name' to the class and 
            assigns the 'value' to it. If the value 
            is a list -> pickle it. Will be unpickled when 
            called by get_attribute_values. 

            raises Input_Error-- if (pickled) value exeeds max size value
        """
        if not value or not NGJob.ATTRIBUTES.has_key(name):
            return

        if name in NGJob.PICKLED:        
            val = cPickle.dumps(value)
        else:
            val = value

        if type(val) == str:
            max_len = NGJob.ATTRIBUTES[name]
            if len(val) > max_len:
                raise Input_Error("Input too big", \
                    "(Pickled) value of '%s' exceeds len '%d'" % (name, max_len))

        assignment = 'self.%s=val' % name # sqlalchemy shall deal will sql injection
        exec(assignment)

    
    
    def __arclibtime2datetime(self,arc_t):
        """ conversion of arclib Time object to
            pythons datetime object.
        """
        try:
            t_epoch = arc_t.GetTime()
        except:
            t_epoch = 0
        
        return datetime.fromtimestamp(t_epoch)
        

    def __init__(self, arclib_job):
        """ arclib_job -- job object as provided by the arclib """
        

        aj = arclib_job

        exec_nodes = []
        for n in aj.execution_nodes:
            exec_nodes.append(n)
       
        rtes = [] 
        for rte in aj.runtime_environments:
            rtes.append('%s %s' % (rte.Name(), rte.Version()))

        self.__add_attribute('global_id', aj.id)
        self.__add_attribute('global_owner', aj.owner)
        self.__add_attribute('status', aj.status)
        self.__add_attribute('job_name', aj.job_name)
        self.__add_attribute('client_software', aj.client_software)
        self.__add_attribute('cluster_name', aj.cluster)
        self.__add_attribute('queue_name', aj.queue)
        self.__add_attribute('completion_time',self.__arclibtime2datetime(aj.completion_time))
        self.__add_attribute('cpu_count',aj.cpu_count)
        self.__add_attribute('sessiondir_erase_time',self.__arclibtime2datetime(aj.erase_time))
        self.__add_attribute('errors', aj.errors)
        self.__add_attribute('execution_nodes', exec_nodes)
        self.__add_attribute('exit_code', aj.exitcode)
        self.__add_attribute('gmlog', aj.gmlog)
        self.__add_attribute('proxy_expiration_time',self.__arclibtime2datetime(aj.proxy_expire_time))
        self.__add_attribute('queue_rank', aj.queue_rank)
        self.__add_attribute('requested_cpu_time', aj.requested_cpu_time)
        self.__add_attribute('requested_wall_time', aj.requested_wall_time)
        self.__add_attribute('runtime_environments', rtes)
        self.__add_attribute('stderr', aj.sstderr)
        self.__add_attribute('stdin', aj.sstdin)
        self.__add_attribute('stdout', aj.sstdout)
        self.__add_attribute('submission_time',self.__arclibtime2datetime(aj.submission_time))
        self.__add_attribute('submission_ui', aj.submission_ui)
        self.__add_attribute('used_cpu_time', aj.used_cpu_time)
        self.__add_attribute('used_memory', aj.used_memory)
        self.__add_attribute('used_wall_time', aj.used_wall_time)

    def get_globalid(self):
        return self.get_attribute_values('global_id')[0]
    
    def get_globalowner(self):
        return self.get_attribute_values('global_owner')[0]
 
    def get_status(self):
        return self.get_attribute_values('status')[0]
    
    def get_jobname(self):
        return self.get_attribute_values('job_name')[0]

    def get_exitcode(self):
        return self.get_attribute_values('exit_code')[0]
    
    def get_cluster_name(self):
        return self.get_attribute_values('cluster_name')[0]
    
    def get_queue_name(self):
        return self.get_attribute_values('queue_name')[0]
    
    def get_usedwalltime(self):
        return self.get_attribute_values('used_wall_time')[0]

    def get_attribute_names(self):
        return NGJob.ATTRIBUTES.keys()
    
    def get_attribute_values(self, name):
        """ returns a list (can be empty. We do not 
            check whether name is a valid attribute name.
        """
        attr = eval("self."+ name)
        if not attr:
            return []

        if name in NGJob.PICKLED:
            return cPickle.loads(attr)
        return [attr]
