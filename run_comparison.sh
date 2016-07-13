#!/bin/sh

# Meant to be run from fermicloud049

WORD=`awk '{print $1}' readin.txt`
echo $WORD

cd /home/sbhat/CompareRecords && ./comparerecordcounts.py -p $WORD 
