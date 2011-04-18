""" ORM object to model GIIS sever """

__author__="Placi Flury grid@switch.ch"
__date__="12.04.2011"
__version__="0.2.0"

from datetime import datetime
from infocache.errors.db import Input_Error

class GiisMeta(object):
    """
    Giis Metadata object. Keeps track of things like
    response and processing time of GIIS server.
    """

    def __init__(self,host,port,mds_vo_name):
        self.hostname = host
        self.port = port
        self.mds_vo_name = mds_vo_name
        self.status = 'active'
        self.response_time = -1.0
        self.processing_time = -1.0
        self.blacklisted = False
        self.db_lastmodified = datetime.utcnow()

    def get_hostname(self):
        return self.hostname

    def get_port(self):
        return self.port

    def get_mds_vo_name(self):
        return mds_vo_name

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

 
