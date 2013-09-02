#!/usr/bin/env python

from distutils.core import setup
#from setuptools  import setup


setup(
    name = "smscg_infocacher",
    version = "1.1.1",
    description = "Database (MySQL) caching engine for ARC Information system data.",
    long_description = """
	This module provides a daemon that queries the top-level NorduGrid ARC 
	GIIS(es). From the GIIS it fetches the list of information system front-ends
	(GRIS'es), which get queried in 2 minutes intervals (configurable). 
    The information about cluster and queue status, as well as about user jobs etc. 
    is then stored in a local database (like MySQL). 
	The daemon can further be run to create rrd plots (and DBs) on GIIS and GRIS response
	and processing times and on the jobs (#number of jobs, queue backlogs etc.). 
    """,
    platforms = "Linux",
    license = "BSD. Copyright (c) 2008 - 2011, SMSCG - Swiss Multi Science Computing Grid. All rights reserved." ,
    author = "Placi Flury",
    author_email = "grid@switch.ch",
    url = "http://www.smscg.ch",
    download_url = "https://subversion.switch.ch/svn/smscg/smscg/ch.smscg.infocache",
    packages = ['infocache','infocache/db', 'infocache/gris', 'infocache/sanity',
                'infocache/errors', 'infocache/utils', 'infocache/rrd'],
    scripts = ['infocacher.py'],
    data_files=[('.',['config/config.ini','config/logging.conf'])]
)

