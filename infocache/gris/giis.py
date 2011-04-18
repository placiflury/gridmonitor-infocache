"""
Class for querying the Nordugrid Information system (GIIS).
Purpose is to recursively get all (secondary) GIIS addresses.
"""
# Todo Check validity of ldap records (expiration time etc.)

__author__ = "Placi Flury grid@switch.ch"
__copyright__ = "Copyright 2008-2011, SMSCG an AAA/SWITCH project"
__date__ = "10.04.2011"
__version__ = "0.2.1"

import ldap
from infocache.utils.common import LDAPCommon
from infocache.errors.giis import GIISError, GIISConError
import logging, logging.config

class NGGiis(LDAPCommon):
    """
    A class to query the GIIS ldap of NorduGrid in order
    to get a list of secondary  GIIS servers. The list 
    is consists of tuples of (hostname,port).
    """

    GIIS_ATTRS = ["giisregistrationstatus"]
     
    def __init__(self, host, port=2135, mds_vo_name="Switzerland"):
        """
            Consturctor parameters: 

            host   -    DNS name of the GIIS server. It can also be passes as 
                        ldap://<name>:<port>
            port    -   port on which GIIS listens ( port 2135 default)
            mds_vo_name - ldap base string for Mds-Vo-name (default Mds-Vo-name=Switzerland, o=grid)"  
            
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
        self.giis_list = []

        try:
            self.ldap = ldap.initialize(host)
            self.ldap.simple_bind_s()
        except ldap.SERVER_DOWN:
            self.log.warn("GIIS server %s is down." % host)
            raise GIISConError("GIIS server down.","GIIS server at %s is down" %(host))
        except ldap.LDAPError, e1:
            self.log.error("GIIS ldap error: %s." % e1.desc())
            raise GIISError("GIIS ldap error.","GIIS ldap error:" % e1.desc())
        
        self.__populate_giis_list()
 
    def __del__(self):
        if self.ldap:
            self.close()
    
    def close(self):
        """ unbinding ldap connection"""
        self.ldap.unbind()

    
    def __populate_giis_list(self):
        
        if "mds-vo-name" in self.mds_vo_name.lower():
            base = self.mds_vo_name
        else:
            base = "Mds-Vo-name=%s, o=grid" % self.mds_vo_name
        
        _filter = "(Mds-Service-hn=*)"

        scope = ldap.SCOPE_BASE
        try:        
            res = self.ldap.search_st(base, scope, _filter, NGGiis.GIIS_ATTRS, timeout=4)
        except ldap.NO_SUCH_OBJECT:
            self.log.error("GIIS query for '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %

            (self.giis_server, base, scope, _filter, NGGiis.GIIS_ATTRS))
            raise GIISError("GIIS No such object",
                "GIIS query for '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.giis_server, base, scope, _filter, NGGiis.GIIS_ATTRS))
        
        records = LDAPCommon.format_res(self, res)
        
        if self.giis_list:
            del self.giis_list
            self.giis_list = []

        for rec in records:
            name =  rec.get_attr_values("Mds-Service-hn")[0]
            port  =  rec.get_attr_values("Mds-Service-port")[0]
            suffix = rec.get_attr_values("Mds-Service-Ldap-suffix")[0]
            if 'nordugrid-cluster-name' in suffix: # GRIS -> ignore
                pass
            elif 'nordugrid-se-name' in suffix: # SE -> ignore
                pass
            elif 'nordugrid-rc-name' in suffix: # RC -> ignore
                pass
            else:
                if 'mds-vo-name' in suffix.lower():
                    suffix = suffix[len('mds-vo-name='):]    
                suffix = suffix.split(',')[0] # hack!!
                if self.giis_list.count((name, port, suffix)) == 0:
                    self.log.debug("Found GIIS: '%s:%s'" % (name, port))
                    # clean up the suffix bit.
                    self.giis_list.append((name, port, suffix))

    def __refresh_giis_list(self):
        self.giis_list = [] 
        self.__populate_giis_list()

    
    def get_giis_list(self):
        """ Returns list of (gris_hostname, port, base_name) pairs."""
        return self.giis_list


if __name__ == "__main__":

    _mds_vo_name = "NorduGrid, o=grid"
    logging.config.fileConfig("/opt/smscg/infocache/etc/logging.conf")
    try:
        ng = NGGiis("index1.nordugrid.org:2135", mds_vo_name = _mds_vo_name)
        ng = NGGiis("grid.uio.no:2135", mds_vo_name='Norway')
        cnames =  ng.get_giis_list()
        for giis in cnames:
            print giis
        del ng    
    except GIISError, e:
        print e.desc()



