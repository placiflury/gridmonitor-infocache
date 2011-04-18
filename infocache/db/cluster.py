""" ORM object for cluster table """
import cPickle
from datetime import datetime

from gridmonitor.model.api.cluster_api import ClusterApi
from infocache.errors.db import Input_Error


class ClusterMeta(object):
    """ Holds metadata information
        about a 'cluster' respectively
        about the GRIS service, which 
        advertized the status of the cluster.
    """
    def __init__(self, status = 'active'):
        self.status = status
        self.response_time = -1.0
        self.processing_time = -1.0
        self.blacklisted = False
        self.db_lastmodified = datetime.utcnow()

    def set_status(self, status):
        """ Updates status of cluster/GRIS """
        self.status = status

    def set_db_lastmodified(self, utc_time=datetime.utcnow()):
        """ Updates db_lastmodified_time either to
            specified time or to utc now (if no
            time has been specified.

            utc_time -- dateteime object
        """
        self.db_lastmodified = utc_time

    def set_response_time(self, t):
        """ Sets the response time. The value of 
            't' must either be of float or integer type.
            raises Input_Error on wrong type
        """
        if (type(t) != float) and (type(t) != int):
            raise Input_Error("Wrong input type", \
                "Type must be 'float or 'int' got %s'" % (type(t)))

        self.response_time = t

    def set_processing_time(self, t):
        """ Sets the processing time. The value of 
            't' must either be of float or integer type.
            raises Input_Error on wrong type
        """
        if (type(t) !=  float) and (type(t) != int):
            raise Input_Error("Wrong input type", \
                "Type must be 'float or 'int' got %s'" % (type(t)))

        self.processing_time = t
        
    def blacklisting(self):
        """ Blacklists cluster/GRIS """
        self.blacklisted = True

    def whitelisting(self):
        """ Whitelists cluster/GRIS """
        self.blacklisted = False

    def is_blacklisted(self):
        """ Returns true if blacklisted, else if whitelisted """
        return self.blacklisted
       
    def get_status(self):
        """ Returns either 'active'/'inactive' """
        return self.status

    def get_db_lastmodified(self):
        """ Returns UTC time of last DB modification for
            this record. (python datetime object)"""
        return self.db_lastmodified

    def get_response_time(self):
        """ Returns response time 
            (either float or int type). """
        return self.response_time

    def get_processing_time(self): 
        """ Returns processing time 
            (either float or int type). """
        return self.processing_time
        

class NGCluster(ClusterApi, ClusterMeta):
    """ Class for storing information about a
        NorduGrid ARC cluster and the GRIS. Supposedly the 
        information we store is fetched from the 
        ARC information system. 
    """
    
    # dict of valid attributes each listed as tupple (<name>, <max_len>)
    ATTRIBUTES = dict(hostname= 255, 
        alias = 255,
        comment =  511,
        owners =  511,
        support =  511,
        contact =  511,
        location =  255,
        issuer_ca =  255,
        cert_expriation =  63,
        architecture =  255,
        homogeneity =  None,
        node_cpu =  255,
        node_memory =  None, 
        middlewares =  511, 
        operating_systems =  255, 
        lrms_config =  255, 
        lrms_type =  255, 
        lrms_version =  255, 
        prelrms_queued = None, 
        queued_jobs = None, 
        total_jobs = None,
        used_cpus = None, 
        total_cpus = None,
        session_dir =  255, 
        cache = 255, 
        benchmarks = 255,
        runtime_environments = 65535)
    
    # list of attributes which are stored pickled
    PICKLED = ['support', 'owners', 'middlewares', 
            'benchmarks', 'runtime_environments', 'operating_systems'] 

    def __add_attribute(self, name, value):
        """ Adds a new attribute with 'name' to the class and 
            assigns the 'value' to it. If the value 
            is a list -> pickle it. Will be unpickled when 
            called by get_attribute_values. 

            raises Input_Error-- if (pickled) value exeeds max size value
        """
        if not value or not NGCluster.ATTRIBUTES.has_key(name):
            return

        if name in NGCluster.PICKLED:        
            val = cPickle.dumps(value)
        else:
            val = value

        if type(val) == str:
            max_len = NGCluster.ATTRIBUTES[name]
            if len(val) > max_len:
                raise Input_Error("Input too big", \
                    "(Pickled) value of '%s' exceeds len '%d'" % (name, max_len))

        assignment = 'self.%s=val' % name # sqlalchemy shall deal will sql injection
        exec(assignment)


    def __init__(self, arclib_cluster):
        """ arclib_cluster -- cluster object as provided by the arclib"""

        ClusterMeta.__init__(self)

        ci = arclib_cluster
        
        ca = '%s (hash: %s)' % (ci.issuer_ca, ci.issuer_ca_hash)
        session_dir = "free: %s (total: %s) -- lifetime: %s " % \
            (ci.session_dir_free, ci.session_dir_total, ci.session_dir_lifetime)
        cache = "free: %s (total: %s)" % (ci.cache_free, ci.cache_total)
        support = []
        owners = []
        middlewares = [] 
        benchmarks = []
        operating_systems = []
        rtes = []
        
        for s in ci.support:
            support.append(s)
        for o in ci.owners:
            owners.append(o)
        for m in ci.middlewares:
            middlewares.append('%s %s' % (m.Name(), m.Version()))
        for b in ci.benchmarks:
            benchmarks.append(b) 
        for os in ci.operating_systems:
            operating_systems.append('%s %s' % (os.Name(), os.Version()))
        for rte in ci.runtime_environments:
            rtes.append('%s %s' % (rte.Name(), rte.Version()))
        
        self.__add_attribute('hostname', ci.hostname)
        self.__add_attribute('alias', ci.alias)
        self.__add_attribute('comment', ci.comment)
        self.__add_attribute('owners', owners)
        self.__add_attribute('support', support)
        self.__add_attribute('contact', ci.contact)            
        self.__add_attribute('location', ci.location)
        self.__add_attribute('issuer_ca', ca)
        if ci.cred_expire_time:
            utctime = "%s" % datetime.utcfromtimestamp(ci.cred_expire_time.GetTime())
            self.__add_attribute('cert_expiration', utctime) 
        
        self.__add_attribute('architecture', ci.architecture)
        self.__add_attribute('homogeneity', ci.homogeneity)
        self.__add_attribute('node_cpu', ci.node_cpu)
        self.__add_attribute('node_memory', ci.node_memory)
        self.__add_attribute('middlewares', middlewares)
        self.__add_attribute('operating_systems', operating_systems)
        self.__add_attribute('lrms_config', ci.lrms_config)
        self.__add_attribute('lrms_type', ci.lrms_type)
        self.__add_attribute('lrms_version', ci.lrms_version)

        self.__add_attribute('prelrms_queued', ci.prelrms_queued)
        self.__add_attribute('queued_jobs', ci.queued_jobs)
        self.__add_attribute('total_jobs', ci.total_jobs)
        self.__add_attribute('used_cpus', ci.used_cpus)
        self.__add_attribute('total_cpus', ci.total_cpus)

        self.__add_attribute('session_dir', session_dir)
        self.__add_attribute('cache', cache)

        self.__add_attribute('benchmarks', benchmarks)
        self.__add_attribute('runtime_environments', rtes)


    def get_name(self):
        return self.get_attribute_values('hostname')[0]

    def get_alias(self):
        return self.get_attribute_values('alias')[0]
        
    def get_attribute_names(self):
        return NGCluster.ATTRIBUTES.keys()

    def get_attribute_values(self, name):
        """ returns a list (can be empty. We do not 
            check whether name is a valid attribute name.
        """
        attr = eval("self."+ name)
        if not attr:
            return []

        if name in NGCluster.PICKLED:
            return cPickle.loads(attr)
        return [attr]


    def set_metadata(self, metadata):
        """ Sets metadata of DB record.
            metadata  -- ClusterMeta type instance 
        """
        self.status = metadata.get_status()
        self.response_time = metadata.get_response_time()
        self.processing_time = metadata.get_processing_time()
        self.db_last_modified = metadata.get_db_lastmodified() 
        self.db_blacklisted = metadata.is_blacklisted()
       
    def get_metadata(self):
        """ Returns ClusterMeta object for
            this respective DB record. """

        metadata = ClusterMeta(self.status)
        metadata.set_db_lastmodified(self.db_lastmodified)
        metadata.set_response_time(self.response_time)
        metadata.set_processing_time(self.processing_time)
        if self.blacklisted:
            metadata.blacklisting()
        else:
            metadata.whitelisting()

        return metadata
        
        
