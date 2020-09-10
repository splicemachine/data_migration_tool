#!/bin/bash

# Purpose: The purpose of this script is to extract the table definition and schemas to sql files to be manually ran against
#          the Splice Machine database.  This will allow the scripts to be passed around or reran needed

# Usage: ./sqlserver-export-schema.sh

source sqlserver-variables.sh
java -cp $DM_JAR:$MICROSOFT_JDBC_JAR com.splicemachine.cs.databasemigration.MigrateDatabase -configFile $CONFIG_FILE -connectionId "sqlserver" -configId default

