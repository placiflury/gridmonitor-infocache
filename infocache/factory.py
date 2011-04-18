"""
Factory to instatiate the daemons that 
shall run in the background.
"""

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "11.04.2011"
__version__ = "0.3.0"

import sys
import logging
from sqlalchemy import engine_from_config

from db import init_model
from utils import init_config
import utils.config_parser as config_parser



class DaemonFactory(object):
    """ Factory to initialize and instatiate the 
        daemon classes of the infocache module.
        The known daemon classes are:
        - 'Giis2Db'
        - Houseekeeper' 
    """
        
    def __init__(self):
        """
        Only sets up of logging facility. The real job 
        is done by the newDaemon method, which creates
        a new daemon instance according to the configuration
        options.
        """
        self.log = logging.getLogger(__name__)


    def newDaemon(self, config_file):
        """
        returns a list of daemons instances
        """
        try:
            init_config(config_file)
        except Exception, e1:
            self.log.error("While reading configuration %s got: %r. Aborting!" % (config_file, e1))
            sys.exit(-1) 
      
        # get daemon_type
        d_types = config_parser.config.get('daemon_types')
        if not d_types:
            self.log.error("'daemon_types' option missing in %s.Aborting!" % (config_file))
            sys.exit(-1) 

        kwargs = dict()

        if 'giis2db' in d_types:
            from giis2db import Giis2db
            mds_vo_name = config_parser.config.get('mds_vo_name')
            if not mds_vo_name:
                self.log.error("'mds_vo_name' option missing in %s. Aborting!" % (options.config_file))
                sys.exit(-1)
            
            giis_raw = config_parser.config.get('giis')
            if not giis_raw:
                self.log.error("'giis' option missing in %s." % (options.config_file))
                sys.exit(-1)
       
            top_giis_list = list() 
            for giis_server in giis_raw.split(','):
                giis_server = giis_server.strip()
                if ':' in giis_server:
                    host, port = giis_server.split(':')
                else:
                    host = giis_server
                    port = Giis2db.DEFAULT_GIIS_PORT
                top_giis_list.append((host, port, mds_vo_name))
            
            _periodicity = config_parser.config.get('periodicity')
            if not _periodicity:
                self.log.info("No periodicity option defined in %s. Setting it to default (120 secs)"
                        % (options.confif_file))
                periodicity = 120
            else:
                try:
                    periodicity = int(_periodicity)
                except Exception:
                    self.log.error("Could not set periodicity to '%s'. Please check option in %s. Aborting!"
                        % (_periodicity, options.confif_file))
                    sys.exit(-1)

            
            kwargs['mds_vo_name'] = mds_vo_name
            kwargs['top_giis_list'] = top_giis_list
            kwargs['periodicity'] = periodicity
        
        if 'housekeeper' in d_types:
            from housekeeper import Housekeeper
            rrd_dir = config_parser.config.get('rrd_directory')
            if not rrd_dir:
                self.log.error("'rrd_directory' option missing in %s. Aborting!" % (options.config_file))
                sys.exit(-1)

            plot_dir = config_parser.config.get('plot_directory')
            if not plot_dir:
                self.log.error("'plot_directory' option missing in %s. Aborting!" % (options.config_file))
                sys.exit(-1)
            
            kwargs['rrd_dir'] = rrd_dir
            kwargs['plot_dir'] = plot_dir
        
        # initialize db session before instantiating daemons
        try:
            engine = engine_from_config(config_parser.config.get(),'sqlalchemy_infocache.')
            init_model(engine)
            self.log.info("Session object to local database created")
        except Exception, ex:
            self.log.error("Session object to local database failed: %r", ex)

        daemon_instances = list()
        for d in d_types.split(','):
            daemon = d.strip()
            if daemon == 'giis2db':
                daemon_instances.append(Giis2db(**kwargs))
            elif daemon == 'housekeeper':
                daemon_instances.append(Housekeeper(**kwargs))
            else:
                self.log.error("'%s' is not a supported 'daemon_type' option. Aborting!" % (daemon))
                sys.exit(-1)
        
        
        return daemon_instances 

