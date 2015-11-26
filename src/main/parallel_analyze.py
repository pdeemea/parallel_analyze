#!/usr/bin/python
#
# Copyright (C) Pivotal Inc 2014. All Rights Reserved.
# Alexey Grishchenko (agrishchenko@pivotal.io)
#
# This script can be used to run the "analyze" statement
# on a single schema in a multiple threads
#
# Instrunctions:
#
# Script performs paralle analyze of tables from database 
# Usage:
# ./parallel_analyze.py -n thread_number -s schema_name -d dbname [-p gpadmin_password]
# Parameters:
#     thread_number    - number of parallel threads to run
#     dbname           - name of the database
#     gpadmin_password - password of the gpadmin user
#
import os, sys, re, os.path, subprocess, csv, time

try:
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from gppylib.gpcoverage import GpCoverage
    from gppylib import userinput
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-d', '--dbname',   type='string')
    parser.add_option('-p', '--password', type='string')
    parser.add_option('-n', '--nthreads', type='int')
    (options, args) = parser.parse_args()
    if options.help or (not options.dbname and not options.filename):
        print """Script performs parallel analyze of all tables
Usage:
./parallel_analyze.py -n thread_number -d dbname [-p gpadmin_password]
Parameters:
    thread_number    - number of parallel threads to run
    dbname           - name of the database
    gpadmin_password - password of the gpadmin user"""
        sys.exit(0)
    if not options.dbname:
        logger.error('Failed to start utility. Please, specify database name with "-d" key')
        sys.exit(1)
    if not options.nthreads:
        logger.error('Failed to start utility. Please, specify number of threads with "-n" key')
        sys.exit(1)
    return options

def execute(dburl, query):
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
        return rows
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)

def analyze_tables(table_list, dbname, threads):
    analyze_command = "psql -d %s -c 'analyze %s;'"
    running = []
    isStopping = 0
    while len(table_list) > 0 or len(running) > 0:
        for pid in running:
            if not pid.poll() is None:
                if pid.returncode == 0:
                    running.remove(pid)
                else:
                    pidret = pid.communicate()
                    logger.error ('analyze failed for one of the table')
                    logger.error (redret[0])
                    logger.error (redret[1])
                    isStopping = 1
        if isStopping == 0 and len(running) < threads and len(table_list) > 0:
            tablename = table_list.pop()
            logger.info ('    Analyzing %s' % tablename)
            pid_analyze_command = analyze_command % (dbname, tablename)
            pid = subprocess.Popen(pid_analyze_command, shell=True, stdout=subprocess.PIPE)
            running.append(pid)
        if isStopping == 1 and len(running) == 0:
            break
    return

def prepare_tables(dburl):
    query = """
        select t.nspname || '.' || t.relname as tablename
            from (
                    select n.nspname,
                           c.relname
                        from pg_class as c,
                             pg_namespace as n
                        where c.relnamespace = n.oid
                            and c.relkind = 'r'
                            and c.relstorage in ('h', 'a', 'c') 
  and n.nspname not in ('pg_catalog','pg_toast','information_schema','gp_toolkit','madlib','pg_aoseg')
                 ) as t
                 left join pg_partitions as p
                    on p.partitiontablename = t.relname
                    and p.partitionschemaname = t.nspname
            where p.partitiontablename is null"""
    res = execute (dburl, query)
    for x in res:
        print 'Table to analyze: "%s"' % x[0]
    return [ x[0] for x in res ]

def orchestrator(options):
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = options.dbname,
                         username = 'gpadmin',
                         password = options.password)
    table_list = prepare_tables(dburl)
    logger.info ('=== Found %d tables to analyze ===' % len(table_list))
    analyze_tables(table_list[::-1], options.dbname, options.nthreads)
    logger.info ('=== Analysis complete ===')

#------------------------------- Mainline --------------------------------

#Initialization
coverage = GpCoverage()
coverage.start()
logger = get_default_logger()

#Parse input parameters and check for validity
options = parseargs()

#Print the partition list
orchestrator(options)

#Stopping
coverage.stop()
coverage.generate_report()

# nohup python parallel_analyze.py -n 8 -d gpdb_upgrade_test >parallel_analyze.log 2>parallel_analyze.err &
# python parallel_analyze.py -n 8 -d gpdb_upgrade_test
