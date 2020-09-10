#!/bin/bash

# Purpose: The purpose of this script is to extract the table definition and schemas to sql files to be manually ran against
#          the Splice Machine database.  This will allow the scripts to be passed around or reran needed

# Usage: ./export-schema-from-db2.sh

source db2-variables.sh
java -cp $DM_JAR:$DB2_JDBC_JAR com.splicemachine.cs.databasemigration.MigrateDatabase -configFile $CONFIG_FILE -connectionId "db2" -configId default


