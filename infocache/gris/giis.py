"""
Class for querying the Nordugrid Information system (GIIS).
Purpose is to get list of querieable  'GRIS' servers. 
"""
# Todo Check validity of ldap records (expiration time etc.)

__author__="Placi Flury placi.flury@switch.ch"
__date__="10.04.2009"
__version__="0.1.2"

import ldap
from infocache.utils.common import * 
from infocache.errors.giis import * 
import logging, logging.config

class NGGiis(LDAPCommon):
    """
    A class to query the GIIS ldap of NorduGrid in order
    to get a list of the available GRIS servers. The list 
    is consists of tuples of (hostname,port).
    """

    GIIS_ATTRS=["giisregistrationstatus"]
     
    def __init__(self, host, port=2135, mds_vo_name="Switzerland"):
        """
            Consturctor parameters: 

            host   -    DNS name of the GIIS server. It can also be passes as 
                        ldap://<name>:<port>
            port    -   port on which GIIS listens ( port 2135 default)
            mds_vo_name - ldap base string for Mds-Vo-name (default Mds-Vo-name=Switzerland)"  
            
            Execptions: 
            
            GIISConError - raised if ldap endpoint can't be reached
            GIISError    - raised for  any other ldap/GIIS related error
        """
        self.log = logging.getLogger(__name__)
        
        host = host.strip()
        if not host.startswith("ldap://"):
            host = "ldap://" + host
        port_suffix = ":" + str(port)
        
        if not host.endswith(port_suffix):
            host += port_suffix
             
        self.mds_vo_name = mds_vo_name
        self.giis_server = host
        self.gris_list = []
        self.giis_list = []

        try:
            self.ldap = ldap.initialize(host)
            self.ldap.simple_bind_s()
        except ldap.SERVER_DOWN:
            self.log.warn("GIIS server %s is down." % host)
            raise GIISConError("GIIS server down.","GIIS server at %s is down" %(host))
        except ldap.LDAPError, e:
            self.log.error("GIIS ldap error: %s." % e.desc())
            raise GIISError("GIIS ldap error.","GIIS ldap error:" % e.desc())
        
        self.__populate_giis_gris_list()
 
    def __del__(self):
        if self.ldap:
            self.close()
    
    def close(self):
        self.ldap.unbind()

    
    def __populate_giis_gris_list(self):
        
        if "mds-vo-name" in self.mds_vo_name.lower():
            base = self.mds_vo_name
        else:
            base="Mds-Vo-name=%s,o=grid" % self.mds_vo_name
        
        filter ="(Mds-Service-hn=*)"

        scope = ldap.SCOPE_BASE
        try:        
            res = self.ldap.search_st(base, scope,filter, NGGiis.GIIS_ATTRS, timeout=4)
        except ldap.NO_SUCH_OBJECT:
            self.log.error("GIIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %

            (self.giis_server,base,scope,filter,NGGiis.GIIS_ATTRS))
            raise GIISError("GIIS No such object",
                "GIIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.giis_server,base,scope,filter,NGGiis.GIIS_ATTRS))
        
        records = LDAPCommon.format_res(self,res)
        
        if self.giis_list:
            del self.giis_list
            self.giis_list = []

        if self.gris_list:
            del self.gris_list
            self.gris_list = []

        for rec in records:
            name =  rec.get_attr_values("Mds-Service-hn")[0]
            port  =  rec.get_attr_values("Mds-Service-port")[0]
            suffix = rec.get_attr_values("Mds-Service-Ldap-suffix")[0]
            if 'nordugrid-cluster-name' in suffix:
                if self.gris_list.count((name,port)) == 0:
                    self.log.debug("Found GRIS: '%s:%s'" % (name, port))
                    self.gris_list.append((name,port))
            else:
                if self.giis_list.count((name,port)) == 0:
                    self.log.debug("Found GIIS: '%s:%s'" % (name, port))
                    self.giis_list.append((name,port,suffix))

    def __refresh_gris_names(self):
        self.gris_list = [] 
        self.__populate_gris_list()

    def get_gris_list(self):
        """ Returns list of (gris_hostname, port) pairs."""
        return self.gris_list
    
    def get_giis_list(self):
        """ Returns list of (gris_hostname, port, base_name) pairs."""
        return self.giis_list


if __name__ == "__main__":

    #mds_vo_name="Switzerland"
    mds_vo_name="NorduGrid"
    logging.config.fileConfig("/opt/smscg/infocache/etc/logging.conf")
    try:
        #ng = NGGiis("elli.switch.ch:2135", mds_vo_name=mds_vo_name)
        ng = NGGiis("index1.nordugrid.org:2135", mds_vo_name=mds_vo_name)
        ng = NGGiis("grid.uio.no:2135", mds_vo_name='Norway')
        ng.get_gris_list()
        cnames =  ng.get_gris_list()
        for gris in cnames:
            print gris
        del ng    
    except GIISError, e:
        print e.desc()



