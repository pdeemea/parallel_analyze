import os, sys, re, os.path, subprocess, csv, time

try:
    from optparse import Option, OptionParser
    from gppylib.gpparseopts import OptParser, OptChecker
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from pygresql.pg import DatabaseError
    from gppylib.gpcoverage import GpCoverage
    from gppylib import userinput
    from multiprocessing import Process,Queue
except ImportError, e:
    sys.exit('Cannot import modules. Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

def parseargs():
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help',          action='store_true')
    parser.add_option('-d', '--dbname',              type='string')
    parser.add_option('-u', '--user',                type='string')
    parser.add_option('-p', '--password',            type='string')
    parser.add_option('-n', '--nthreads',            type='int')
    parser.add_option('-s', '--stat_mem',            type='string')
    parser.add_option('-f', '--tablefile',           type='string')
    parser.add_option('-t', '--distkeyfile',         type='string')
    parser.add_option('-m', '--metadatatablesuffix', type='string')
    (options, args) = parser.parse_args()
    if options.help:
        print """Script performs analysis of table row number and number of
unique values of table distribution key
Usage:
./data_consistency_check.py -d dbname [-n thread_number] [-u user_name] [-p password]
                                      [-s statement_mem] [-f tablefile] [-t distkeyfile]
                                      [-m metadatatablesuffix]
Parameters:
    -d | --dbname    - name of the database to process
    -n | --nthreads  - number of parallel threads to run
    -u | --user      - user to connect to the database
    -p | --password  - password to connect to the database
    -s | --statement_mem    - the value of statement_mem to use
    -f | --tablefile        - file with the list of tables to process
    -t | --distkeyfile      - file with the tables which should be analyzed with
                              counting distinct values of distribution key
    -m | --metadatatablesuffix
                            - suffix for the table to store script metadata in
Metadata objects created are:
    public.__zz_pivotal_{suffix}   - view with the final information on row counts
    public.__zz_pivotal_{suffix}_l - list of tables to process
    public.__zz_pivotal_{suffix}_p - current progress of table row count calculation
After the run has finished for the second time, join two metadata tables by
the "tablename" field like this:
select  m1.tablename as table_first,
        m2.tablename as table_second,
        m1.rowcount  as rows_before,
        m2.rowcount  as rows_after,
        m1.distkeycount as dist_keys_before,
        m2.distkeycount as dist_keys_after
    from {metadatatable1} as m1
        full outer join {metadatatable2} as m2
        on m1.tablename = m2.tablename
    where m1.tablename is null
        or m2.tablename is null
        or m1.rowcount is distinct from m2.rowcount
        or m1.distkeycount is distinct from m2.distkeycount
"""
        sys.exit(0)
    if not options.dbname:
        logger.error('Failed to start utility. Please, specify database name with "-d" key')
        sys.exit(1)
    if not options.nthreads:
        logger.info('Number of threads is not specified. Using 1 by default')
        options.nthreads = 1
    if not options.stat_mem:
        logger.info('Statement memory is not specified. Using 125MB by default')
        options.stat_mem = '125MB'
    if not options.metadatatablesuffix:
        logger.info('Metadata table suffix is not specified. Using "table_list" by default')
        options.metadatatablesuffix = 'table_list'
    else:
        if not re.match('^[0-9a-z_]*$', options.metadatatablesuffix):
            logger.error ('Metadata suffix must contain only lowercase letters, numbers 0-9 and underscore sign')
            sys.exit(1)
    if not options.tablefile:
        logger.info('No tablefile specified. Will process all the tables in database by default')
    if not options.distkeyfile:
        logger.info('No distribution key table file specified. Will omit distribution key analysis')
    return options

def execute_noret(dburl, query):
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        conn.commit()
        conn.close()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
    return

def execute(dburl, query):
    rows = [[]]
    try:
        conn = dbconn.connect(dburl)
        curs = dbconn.execSQL(conn, query)
        rows = curs.fetchall()
        conn.commit()
        conn.close()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)
    return rows

def process_table_list(table_list, dburl, statement_mem, metadatatable):
    try:
        conn = dbconn.connect(dburl)
        dbconn.execSQL(conn, "SET statement_mem TO '%s'" % statement_mem)
        conn.commit()
        for table in table_list:
            logger.info('  processing table %s...' % table)
            curs     = dbconn.execSQL(conn, "select count(*) from %s" % table)
            rowcount = curs.fetchall()[0][0]
            curs     = dbconn.execSQL(conn, "select ischeckdistkey from %s where tablename = '%s'" % (metadatatable, table))
            ischeckdistkey = curs.fetchall()[0][0]
            distkeycnt = 'null'
            if ischeckdistkey == 1:
                logger.info('  getting distribution stats for table %s...' % table)
                query_distribution = """
                    select attname
                        from pg_class as c,
                             pg_namespace as n,
                             pg_attribute as a,
                             gp_distribution_policy as d
                        where c.relnamespace = n.oid
                            and a.attrelid = c.oid
                            and n.nspname || '.' || c.relname = '%s'
                            and d.localoid = c.oid
                            and a.attnum = ANY(d.attrnums)
                    """ % table
                curs = dbconn.execSQL(conn, query_distribution)
                distkeys = curs.fetchall()
                if len(distkeys) > 0:
                    distkeysstr = '"' + '","'.join([d[0] for d in distkeys]) + '"'
                    logger.info('    %s is distributed by: %s' % (table, distkeysstr))
                    query = """
                        select count(*)
                            from (
                                select %s
                                    from %s
                                    group by %s
                            ) as q
                    """ % (distkeysstr, table, distkeysstr)
                    curs  = dbconn.execSQL(conn, query)
                    distkeycnt = str(curs.fetchall()[0][0])
                else:
                    logger.info('    %s is distributed randomly, no analysis needed' % table)
            dbconn.execSQL(conn, """
                insert into %s_p (tablename, ischeckdistkey, rowcount, distkeycount)
                    values ('%s', %d, %d, %s)
                """ % (metadatatable, table, ischeckdistkey, rowcount, distkeycnt))
        conn.commit()
        conn.close()
    except DatabaseError, ex:
        logger.error('Failed to execute the statement on the database. Please, check log file for errors.')
        logger.error(ex)
        sys.exit(3)

def process_all_tables(table_list, threads, dburl, statement_mem, metadatatable):
    logger.info ('=== Processing %d tables in %d threads ===' % (len(table_list), threads))
    running = []
    isStopping = 0
    error_cnt = 0
    while len(table_list) > 0 or len(running) > 0:
        for pid in running:
            if pid.is_alive() is False:
                running.remove(pid)
                if pid.exitcode != 0:
                    pid.join()
                    logger.error ('Processing for one of the batches has failed, restart the script after this run finishes')
                    error_cnt += 1
        if isStopping == 0 and len(running) < threads and len(table_list) > 0:
            query_tables_lst = table_list[:10]
            table_list = table_list[10:]
            pid = Process(target=process_table_list, name="Process Tables", args=(query_tables_lst, dburl, statement_mem, metadatatable))
            running.append(pid)
            pid.start()
            if len(table_list) == 0:
                isStopping = 1
        if isStopping == 1 and len(running) == 0:
            break
    logger.info ('=== Processing complete ===')
    logger.info ('Please check the results in table %s' % metadatatable)
    if error_cnt > 0:
        logger.warning ('There were %d errors during the processing. Check the log. Restart is required' % error_cnt)
    return

def initialize(dburl, metadatatable, tables, stables):
    logger.info('Checking metadata tables %s...' % metadatatable)
    query = """
        select count(*)
            from pg_class as c,
                 pg_namespace as n
            where c.relnamespace = n.oid
                and n.nspname || '.' || c.relname in ('%s', '%s_p', '%s_l')
        """ % (metadatatable, metadatatable, metadatatable)
    res = execute (dburl, query)
    if res != [[3]]:
        if res != [[0]]:
            logger.error('Found objects with names matching target metadata object names')
            logger.error('Please, drop following objects before proceeding:')
            logger.error('    %s' % metadatatable)
            logger.error('    %s_l' % metadatatable)
            logger.error('    %s_p' % metadatatable)
            sys.exit(2)
        logger.info('Metadata table is missing. Creating it...')
        execute_noret(dburl, "drop table if exists %s_p cascade" % metadatatable)
        execute_noret(dburl, "drop table if exists %s_l cascade" % metadatatable)
        execute_noret(dburl, "drop view  if exists %s   cascade" % metadatatable)
        execute_noret(dburl, """
            create table %s_l (
                tablename       varchar,
                ischeckdistkey  smallint
            )
            distributed randomly
            """ % metadatatable)
        execute_noret(dburl, """
            create table %s_p (
                tablename       varchar,
                ischeckdistkey  smallint,
                rowcount        bigint,
                distkeycount    bigint
            )
            distributed randomly
            """ % metadatatable)
        execute_noret(dburl, """
            create or replace view %s as
                select  l.tablename,
                        case when p.tablename is not null then 1
                             else 0
                        end as isprocessed,
                        l.ischeckdistkey,
                        p.rowcount,
                        p.distkeycount
                    from %s_l as l
                        left join %s_p as p
                        on l.tablename = p.tablename
            """ % (metadatatable, metadatatable, metadatatable))
        stable_list = "'" + "','".join(stables) + "'"
        query = """
            insert into %s_l
                select  t.nspname || '.' || t.relname as tablename,
                        case
                            when t.nspname || '.' || t.relname in (%s)
                                or p3.schemaname || '.' || p3.tablename in (%s) then 1
                            else 0
                        end as ischeckdistkey
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
                        -- removing parent partition table
                        left join (
                                select  schemaname,
                                        tablename
                                    from pg_partitions
                                    group by 1, 2
                                ) as p1
                            on p1.tablename = t.relname and p1.schemaname = t.nspname
                        -- removing non-leaf partitions
                        left join (
                                select  partitionschemaname,
                                        parentpartitiontablename
                                    from pg_partitions
                                    group by 1, 2
                                ) as p2
                            on p2.parentpartitiontablename = t.relname and p2.partitionschemaname = t.nspname
                        -- adding the link to main table for partitions
                        left join pg_partitions as p3
                            on p3.partitiontablename = t.relname and p3.partitionschemaname = t.nspname
                    where p1.tablename is null
                        and p2.parentpartitiontablename is null
            """ % (metadatatable, stable_list, stable_list)
        if len(tables) > 0:
            table_list = "'" + "','".join(tables) + "'"
            query += """
                and (t.nspname || '.' || t.relname in (%s)
                    or p3.schemaname || '.' || p3.tablename in (%s))
            """ % (table_list, table_list)
        execute_noret(dburl, query)
    else:
        logger.info('Metadata table exists, omitting creation')
    logger.info('Metadata view %s contains summary on row counts' % metadatatable)
    logger.info('Metadata table %s_l contains full list of tables to process' % metadatatable)
    logger.info('Metadata table %s_p contains current processing status' % metadatatable)
    table_list = execute(dburl, "select tablename from %s where isprocessed = 0" % metadatatable)
    return [t[0] for t in table_list]

def read_table_file(filename):
    stables = []
    if filename is not None:
        f = open(filename)
        for line in f:
            stables.append(line.strip())
        f.close()
        logger.info('Read %d tables from tablefile %s' % (len(stables), filename))
    return stables

def orchestrator(options):
    dburl = dbconn.DbURL(hostname = '127.0.0.1',
                         port     = 5432,
                         dbname   = options.dbname,
                         username = options.user,
                         password = options.password)
    tables  = read_table_file(options.tablefile)
    stables = read_table_file(options.distkeyfile)
    metadatatable = 'public.__zz_pivotal_' + options.metadatatablesuffix
    table_list = initialize(dburl, metadatatable, tables, stables)
    process_all_tables(table_list, options.nthreads, dburl, options.stat_mem, metadatatable)


#------------------------------- Mainline --------------------------------

# Initialization
coverage = GpCoverage()
coverage.start()
logger = get_default_logger()

# Parse input parameters and check for validity
options = parseargs()

# Main processing
orchestrator(options)

# Stopping
coverage.stop()
coverage.generate_report()
