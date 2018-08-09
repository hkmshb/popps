#!/bin/bash

NOW=`date '+%Y-%m-%d %H:%M:%S'`

# run script
echo 'Starting popps script at '$NOW
psql -d $DBNAME -U $DBUSER -h $DBHOST -X -f ./scripts/popps.sql

echo 'done at '$NOW
