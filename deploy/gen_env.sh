#!/bin/bash

set -e

DEFAULTFILE=".env"
echo $1
FILE=${1:-$DEFAULTFILE}     
if [ -f $FILE ]; then
   echo "File $FILE exists."
else
   echo "Creating $FILE."
   cp .env.example $FILE
   for passvar in OEAMDB_WEB_POSTGRES_ADMIN_PASSWORD OEAMDB_WEB_POSTGRES_PASSWORD;
   do
     sed -i -e "s/${passvar}=/${passvar}=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 20 ; echo '')/g" $FILE
   done
fi