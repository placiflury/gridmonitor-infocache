#!/usr/bin/env python

from distutils.core import setup
#from setuptools  import setup


setup(
    name = "smscg_infocacher",
    version = "0.9.0",
    description = "Database (MySQL) cacher of ARC Infosys data.",
    long_description = """
	This module provides a daemon that queries the top-level NorduGrid ARC 
	GIIS. From the GIIS it fetches the list of information system front-ends
	(GRIS'es), which get quired in 2 minutes intevals. The information about
	cluster and queue status, as well as about user jobs etc. is then stored
	in a local database (like MySQL). 
	The daemon further creates rrd plots (and DBs) on GIIS and GRIS response
	and processing times and on the jobs (#number of jobs, queue backlogs etc.) 
    """,
    platforms = "Linux",
    license = "BSD. Copyright (c) 2008, SMSCG - Swiss Multi Science Computing Grid. All rights reserved." ,
    author = "Placi Flury",
    author_email = "grid@switch.ch",
    url = "http://www.smscg.ch",
    download_url = "http://repo.smscg.ch",
    packages = ['infocache','infocache/db', 'infocache/gris', 'infocache/sanity',
                'infocache/errors', 'infocache/utils', 'infocache/voms', 
                'infocache/voms/errors', 'infocache/rrd'],
    scripts = ['giis2db.py'],
    data_files=[('.',['config/config.ini','config/logging.conf'])]
)

