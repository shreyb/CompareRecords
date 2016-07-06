#!/usr/bin/python

import datetime
from os import path,remove,rename
import argparse
import logging
import certifi
from getpass import getpass

import mysql.connector
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

from indexpattern import indexpattern_generate 


#Gratia
def gratiasearch(conn,starttime,verbose=False):
    """Function that queries GRATIA database and returns number of hits between
    the specified start date and one day later"""
    endtime=starttime+datetime.timedelta(days=1)
    
    cursor=conn.cursor()
    query = ("SELECT COUNT(*) as sqlcount\
            FROM JobUsageRecord J INNER JOIN JobUsageRecord_Meta M on M.dbid=J.dbid\
            WHERE J.ENDTIME >= %s \
                AND J.ENDTIME < %s\
            ;")
    cursor.execute(query,(starttime,endtime))
    
    if verbose:
        print 'Query passed to GRATIA:\n{}'.format(cursor.statement)
    
    for sqlcount in cursor:
         count = sqlcount[0]

    return count


##GRACC - this block is deprecated in favor of graccquery
#def graccsearch(client,indexdate,verbose=False):
#    month_day = ['0' + str(elt) if len(str(elt)) == 1 else str(elt) for elt in (indexdate.month, indexdate.day)]
#    indexyear = str(indexdate.year)
#    index = 'gracc.osg.raw-{}.{}.{}'.format(indexyear,*month_day)
#    
#    if verbose:
#        print "Index being searched is {}".format(index)
#    
#    countrecord = client.indices.stats(metric='docs')
#    count = countrecord['indices'][index]['primaries']['docs']['count']
#
#    return count


#GRACC
def graccquery(client, starttime, verbose=False):
    """Function that queries GRACC database, returns number of hits between
    the specified start date and one day later"""
    endtime=starttime+datetime.timedelta(days=1)
    indexpattern=indexpattern_generate(starttime,endtime)
    
    start_date = str(starttime.isoformat())
    end_date = str(endtime.isoformat())

    if verbose:
        print "Date range used for GRACC is {} - {}".format(start_date,end_date)
        print "Index pattern used for search is {}".format(indexpattern)

    s = Search(using=client,index=indexpattern)
    s = s.filter('range',EndTime={"gte":start_date,"lt":end_date})
   
    if verbose:
        print s.to_dict()

    response = s.execute()
    return response.hits.total


def args_parser():
    parser = argparse.ArgumentParser(description = 'Script to compare GRACC and GRATIA record counts by day')
    parser.add_argument('-s','--start',\
                      help = 'Start Date in format yyyy-mm-dd (default is 31 days ago)',\
                      action = 'store',\
                      default = (datetime.date.today()-datetime.timedelta(days=31)).isoformat())
    parser.add_argument('-e','--end',\
                      help = 'End Date in format yyyy-mm-dd (default is yesterday)',\
                      action = 'store',\
                      default = (datetime.date.today()-datetime.timedelta(days=1)).isoformat())
    parser.add_argument('-p','--password',\
                      help = 'GRATIA DB password',\
                      action = 'store',\
                      default = None)
    parser.add_argument('-v','--verbose',\
                      help = 'Verbose',\
                      action = 'store_true')
                
    return parser.parse_args()
    

def date_parse(date_string):
    """Parses date input by user in yyyy-mm-dd format and returns datetime.date object"""
    return datetime.date(*[int(elt) for elt in date_string.split('-')])


def file_initialize(writefile, backupfile, verbose=False):
    """Initialize our files"""
    # If the writefile exists and was the result of a successful run, back it up
    if path.exists(writefile):
        with open(writefile,'r') as f:
            lines = f.readlines()
            if lines[len(lines)-1] == "Success":
                rename(writefile,backupfile)
                if verbose:
                    print "Backed up old output file."
            else:
                pass

    # Put the header in our writefile
    with open(writefile,'w') as f:
        header  = '{}\t{}\t{}\t{}\t{}\t{}\n'.format('Start Date',
                                                    'End Date',
                                                    'gratia_count',
                                                    'gracc_count',
                                                    'diff(gracc-gratia)',
                                                    'Percentage diff')
        f.write('Datestamp: {}\n\n{}'.format(datetime.datetime.now(),header))
        if verbose:
            print header
    return


def file_cleanup(writefile, backupfile, verbose=False):
    """Cleanup actions at the end of the script.  We flag the run as a success
    and remove the backup file"""
    with open(writefile,'a') as f:
        f.write("\nSuccess")
    
    if path.exists(backupfile):
        remove(backupfile)
        if verbose:
            print "Removed backup file"
    return
    

def analyze(gratia_connection, gracc_client, testdate, verbose = False):
    """Main analyzing function of our script that compares the record counts 
    for GRACC and GRATIA, and returns a tuple of the counts and the variances"""
    sdate = testdate 
    edate = sdate + datetime.timedelta(days=1)
    
    if verbose:
        print 'Start Date for date range loop is {}'.format(sdate)
    
    gratiacount = gratiasearch(gratia_connection, sdate, verbose)
    gracc_count = graccquery(gracc_client, sdate, verbose)
    
    diff = gracc_count - gratiacount
    diffquotient = float(diff)/float(gratiacount)

    return (sdate, edate, gratiacount, gracc_count, diff, diffquotient)
    
    
def main():    
    """Main execution function."""
    # Set up logging
    logging.basicConfig(filename='example.log',level=logging.DEBUG)
    logging.getLogger('elasticsearch.trace').addHandler(logging.StreamHandler())
    
    # Grab our arguments
    args_in = args_parser()
    
    # Specify our files
    writefile, backupfile = 'runresults.out', 'runresults_BAK.out'
    file_initialize(writefile, backupfile, args_in.verbose)

    # Ask for the password if we need to
    if args_in.password == None:
        passwd = getpass("Please enter the password for the GRATIA database: ")
    else:
        passwd = args_in.password
    
    # Parse the dates that user gave us
    date_range = (date_parse(args_in.start),date_parse(args_in.end))
    if args_in.verbose:
        print "Script's Date range is {} - {}".format(args_in.start,args_in.end)

    # Connection to GRATIA db
    conx = mysql.connector.connect(user = 'reader', 
                                   password = passwd, 
                                   host = 'gratiadb03.fnal.gov',
                                   database = 'gratia') 
    
    # Elasticsearch client for GRACC
    client = Elasticsearch(['https://gracc.opensciencegrid.org/e'],
                           use_ssl=True,
                           verify_certs=True,
                           ca_certs = certifi.where(),
                           client_cert='gracc_cert/gracc-reports-dev.crt',
                           client_key='gracc_cert/gracc-reports-dev.key',
                           timeout=60)   
   
    
    # Our main loop that analyzes GRACC and GRATIA
    datepointer = date_range[0] 
    while datepointer <= date_range[1]:
        resultstring = analyze(conx, client, datepointer, args_in.verbose)

        # Note:  The next line automatically converts the quotient diffquotient into a percentage 
        # which is why there's no extra multiply-by-100.
        outstr = '{}\t{}\t{}\t{}\t{}\t{:.4%}\n'.format(*resultstring)
        if args_in.verbose:
            print outstr
        
        with open(writefile,'a') as f:
            f.write(outstr)
    
        datepointer+=datetime.timedelta(days=1)
    
    conx.close()
    file_cleanup(writefile, backupfile, args_in.verbose)


if __name__ == '__main__':
    main()
