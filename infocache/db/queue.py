from gridmonitor.model.api.queue_api import QueueApi
from infocache.errors.db import Input_Error
from infocache.utils import utils

class NGQueue(object, QueueApi):
    """ Class for storing information about a
        NorduGrid ARC queue. Supposedly the 
        store information is populated from the 
        ARC information system. 
    """
    # dict of valid attributes each listed as tupple (<name>, <max_len>)
    ATTRIBUTES = dict(name= 255, 
        comment =  511,
        cpu_freq = None,
        default_cpu_time = None,
        default_wall_time = None,
        grid_queued  = None,
        grid_running = None,
        homogeneity = None,
        local_queued = None,
        max_cpu_time = None,
        max_queuable = None,
        max_running  = None,
        max_total_cpu_time = None,
        max_user_run = None,
        max_wall_time = None,
        min_cpu_time   = None,
        min_wall_time  = None, 
        node_cpu  = 255, 
        node_memory = None,
        prelrms_queued = None,
        queued = None,
        running = None,
        scheduling_policy = 63,
        status = 127,
        cpus = None)
    
    def __add_attribute(self, name, value):
        """ Adds a new attribute with 'name' to the class and 
            assigns the 'value' to it. If the value 
            is a list -> pickle it. Will be unpickled when 
            called by get_attribute_values. 

            raises Input_Error-- if (pickled) value exeeds max size value
        """
        if not value or not NGQueue.ATTRIBUTES.has_key(name):
            return

        if type(value) == str:
            max_len = NGQueue.ATTRIBUTES[name]
            if len(value) > max_len:
                raise Input_Error("Input too big", \
                    "(Pickled) value of '%s' exceeds len '%d'" % (name, max_len))

        assignment = 'self.%s=value' % name # sqlalchemy shall deal will sql injection
        exec(assignment)

    

    def __init__(self, arclib_queue, hostname):
        """ arclib_queue -- queue object as provided by the arclib
            hostname -- name of the cluster (host)"""

        qi = arclib_queue
        
        self.hostname = hostname
        self.__add_attribute('name', qi.name)
        self.__add_attribute('comment',qi.comment)
        self.__add_attribute('cpu_freq',qi.cpu_freq)
        self.__add_attribute('default_cpu_time',qi.default_cpu_time)
        self.__add_attribute('default_wall_time', qi.default_wall_time)
        self.__add_attribute('grid_queued', qi.grid_queued)
        self.__add_attribute('grid_running',qi.grid_running)
        self.__add_attribute('homogeneity', qi.homogeneity)
        self.__add_attribute('local_queued',qi.local_queued)
        self.__add_attribute('max_cpu_time', qi.max_cpu_time)
        self.__add_attribute('max_queuable', qi.max_queuable)
        self.__add_attribute('max_running', qi.max_running)
        self.__add_attribute('max_total_cpu_time', qi.max_total_cpu_time)
        self.__add_attribute('max_user_run', qi.max_user_run)
        self.__add_attribute('max_wall_time', qi.max_wall_time)
        self.__add_attribute('min_cpu_time', qi.min_cpu_time)
        self.__add_attribute('min_wall_time', qi.min_wall_time)
        self.__add_attribute('node_cpu', qi.node_cpu)
        self.__add_attribute('node_memory', qi.node_memory)
        self.__add_attribute('prelrms_queued', qi.prelrms_queued)
        self.__add_attribute('queued', qi.queued)
        self.__add_attribute('running', qi.running)
        self.__add_attribute('scheduling_policy', qi.scheduling_policy)
        self.__add_attribute('status', qi.status)
        self.__add_attribute('cpus', qi.total_cpus) # notice re-named

    def get_name(self):
        return self.get_attribute_values('name')[0]
 

    def get_cname(self):
        """ returns a 'cannonical' representation of the
            name of the queue. The cannonical name must 
            be allowed to be used unchanged within a URL.
        """
        qname = self.get_name()
        return utils.str_cannonize(qname) 

    def get_attribute_names(self):
        return NGQueue.ATTRIBUTES.keys()

    def get_attribute_values(self, name):
        """ returns a list (can be empty. We do not 
            check whether name is a valid attribute name.
        """
        attr = eval("self."+ name)
        if not attr:
            return []

        return [attr]
