#!/bin/sh

# Meant to be run from fermicloud049.  This script reads from a file readin.txt that doesn't exist in the repo for security reasons.

WORD=`awk '{print $1}' readin.txt`
cd /home/sbhat/CompareRecords && ./comparerecordcounts.py -p $WORD 
