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
    cursor=conn.cursor()

    query = ("SELECT COUNT(*) as sqlcount\
            FROM JobUsageRecord J INNER JOIN JobUsageRecord_Meta M on M.dbid=J.dbid\
            WHERE J.ENDTIME >= %s \
                AND J.ENDTIME < %s\
            ;")

    endtime=starttime+datetime.timedelta(days=1)
    
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
def graccquery(client,starttime,endtime,verbose=False):
    
    start_date = str(starttime.isoformat())
    end_date = str(endtime.isoformat())
    indexpattern=indexpattern_generate(starttime,endtime)
   
    if verbose:
        print "Date range used for GRACC is {} - {}".format(start_date,end_date)
         
    s = Search(using=client,index=indexpattern)
    s = s.filter('range',EndTime={"gte":start_date,"lt":end_date})
    
    query = s.to_dict()

        


    response = s.execute()
    
    return response.hits.total


def args_parser():
    parser = argparse.ArgumentParser(description = 'Script to compare GRACC and GRATIA record counts by day')
    parser.add_argument('-s','--start',\
                      help = 'Start Date in format yyyy-mm-dd',\
                      action = 'store',\
                      required = True,\
                      default = (datetime.date.today()-datetime.timedelta(days=31)).isoformat())
    parser.add_argument('-e','--end',\
                      help = 'End Date in format yyyy-mm-dd',\
                      action = 'store',\
                      required = True,\
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
    return datetime.date(*[int(elt) for elt in date_string.split('-')])


def file_initialize(writefile,backupfile,verbose=False):
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
        header  = '{}\t{}\t{}\t{}\t{}\t{}\n'.format('Start Date','End Date','gratia_count','gracc_count','diff(gracc-gratia)','Percentage diff')
        f.write('Datestamp: {}\n\n{}'.format(datetime.datetime.now(),header))
        if verbose:
            print header
    
    return
    
    

def analyze():
    """Main analyzing function of our script that compares the record counts for GRACC and GRATIA, and returns those"""
    pass
    
    
def main():    
    """Where a lot of the setting up and passing variables back and forth goes"""
    # Set up logging
    logging.basicConfig(filename='example.log',level=logging.ERROR)
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
        print "Script's Date range is {}-{}".format(args_in.start,args_in.end)

    #Connection to GRATIA db
    conx = mysql.connector.connect(user = 'reader', 
                                   password = passwd, 
                                   host = 'gratiadb03.fnal.gov',
                                   database = 'gratia') 
    
    #Elasticsearch client for GRACC
    client = Elasticsearch(['https://gracc.opensciencegrid.org/e'],
                           use_ssl=True,
                           verify_certs=True,
                           ca_certs = certifi.where(),
                           client_cert='gracc_cert/gracc-reports-dev.crt',
                           client_key='gracc_cert/gracc-reports-dev.key',
                           timeout=60)   
   
    datepointer = date_range[0] 
    while datepointer <= date_range[1]:
        sdate = datepointer
        edate = sdate + datetime.timedelta(days=1)
        
        if args_in.verbose:
            print 'Start Date for date range loop is {}'.format(sdate)
            gratiacount = gratiasearch(conx,sdate,True)
            graccq_count = graccquery(client,sdate,edate,True)
        else:
            gratiacount = gratiasearch(conx,sdate)
            graccq_count = graccquery(client,sdate,edate)
        
        diff = graccq_count - gratiacount
        percdiff= float(diff)/float(gratiacount)

        # Note:  The next line automatically converts the quotient percdiff into a percentage 
        # which is why there's no extra multiply-by-100.
        outstr = '{}\t{}\t{}\t{}\t{}\t{:.4%}\n'.format(sdate,edate,gratiacount,graccq_count,diff,percdiff)
        
        if args_in.verbose:
            print outstr

        with open(writefile,'a') as f:
            f.write(outstr)
    
        datepointer+=datetime.timedelta(days=1)

    conx.close()
    
    with open(writefile,'a') as f:
        f.write("\nSuccess")
    
    if path.exists(backupfile):
        remove(backupfile)
        if args_in.verbose:
            print "Removed backup file"


if __name__ == '__main__':
    main()
