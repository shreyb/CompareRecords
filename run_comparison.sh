#!/bin/sh

WORD=`awk '{print $1}' readin.txt`
echo $WORD

cd /home/sbhat/CompareRecords && ./comparerecordcounts.py -p $WORD
