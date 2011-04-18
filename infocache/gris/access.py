""" module for querying users """

__author__ = "Placi Flury placi.flury@switch.ch"
__date__ = "14.4.2011"
__version__ = "0.1.0"

import ldap as LDAP
import logging
from infocache.utils.common import LDAPCommon
from infocache.errors.gris import *

from infocache.db import meta, schema

class ClusterAccess(object, LDAPCommon):
    """ Class to query users which are
        allowed on the Grid resources.
    """

    USER_ATTRS = ["nordugrid-authuser-sn"]
    #                "nordugrid-authuser-queuelength"]

    NETWORK_TIMEOUT = 30    # ldap network timeout [seconds]
    LDAP_TIMEOUT = 60       # ldap timeout for any request [seconds]


    def __init__(self, hostname, port=2135):
        """ hostname -- cluster front-end to query
                        must not contain any protocol stuff like 'ldap://'
            port -- port on which to connect via ldap (default 2135)
        """
        self.log = logging.getLogger(__name__)
        self.hostname = hostname.strip()
        host = "ldap://" + self.hostname + ":" + str(port)
        self.queues = []

        try:
            self.ldap = LDAP.initialize(host)
            self.ldap.set_option(LDAP.OPT_NETWORK_TIMEOUT, ClusterAccess.NETWORK_TIMEOUT)
            self.ldap.set_option(LDAP.OPT_TIMEOUT, ClusterAccess.LDAP_TIMEOUT)
            self.ldap.simple_bind_s()
            self.log.debug("Connected to GRIS %s" % (host))
        except LDAP.SERVER_DOWN:
            self.log.warn("GRIS server %s is down." % host)
            raise CONNECT_ERROR("Connection error", "Could not connect to server %s" % host)
        except LDAP.LDAPError, e:
            self.log.error("GRIS ldap error: %s." % e.desc())
            raise GRIS_ERROR("GRIS Error", e.desc())

        self.get_queue_names()

    
    def __del__(self):
        if self.ldap:
            self.ldap.unbind()

    def get_queue_names(self):
        """ Populates internal list with names of cluster queues."""
        base = "nordugrid-cluster-name=%s,Mds-Vo-name=local,o=grid" % self.hostname
        _filter = "(objectClass=nordugrid-queue)"
        scope = LDAP.SCOPE_ONELEVEL
        attribute = ['nordugrid-queue-name']
        
        try:
            res = self.ldap.search_s(base, scope, _filter, attribute)
        except LDAP.NO_SUCH_OBJECT:
            self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." %
            (self.hostname, base, scope, _filter, attribute))

            raise GRIS_ERROR("GRIS No such object",
                "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
            (self.hostname, base, scope, _filter, attribute))
        records = LDAPCommon.format_res(self, res)

        if self.queues:
            del self.queues
            self.queues = []

        self.log.debug("Got %d queues for cluster %s." % (len(records), self.hostname))
        for rec in records:
            self.queues.append(rec.get_attr_values(attribute[0])[0])
        self.log.debug("Got queues %r" % self.queues)

    def write_allowed_users2db(self):        # fetch users that are allowed for this queue
        """
        Populates database with allowed users.
        """
        _filter = "(objectClass=nordugrid-authuser)"
        scope = LDAP.SCOPE_ONELEVEL
        
        for queuename in self.queues:

            base = "nordugrid-info-group-name=users,\
                nordugrid-queue-name=%s,\
                nordugrid-cluster-name=%s,\
                Mds-Vo-name=local,o=grid" \
                % (queuename, self.hostname)

            try:
                res = self.ldap.search_s(base, scope, _filter, ClusterAccess.USER_ATTRS)
            except LDAP.NO_SUCH_OBJECT:
                self.log.error("GRIS query for server '%s' with (base=%s,scope=%s,filter=%s,attributes=%r) failed with: 'No such object'." 
    %
                (self.hostname, base, scope, _filter, ClusterAccess.USER_ATTRS))

                raise GRIS_ERROR("GRIS No such object",
                    "GRIS query for server '%s' with (base=%s, scope=%s,filter=%s,attributes=%r)failed with: 'No such object'." %
                (self.hostname, base, scope, _filter, ClusterAccess.USER_ATTRS))
        

            records = LDAPCommon.format_res(self, res)
            self.log.debug("Got %d allowed users on %s ( %s)" % (len(records), self.hostname, queuename))

            allowed_users = []

            for rec in records:
                dn = rec.get_attr_values(ClusterAccess.USER_ATTRS[0])[0]
                if dn not in allowed_users:
                    allowed_users.append(dn)

            if allowed_users:
                session = meta.Session()
                for dn in allowed_users:
                    ua = schema.UserAccess(dn, self.hostname, queuename)
                    ua = session.merge(ua)
            session.commit()
