#!/bin/bash

# Purpose: The purpose of this script is to extract the table definition and schemas to sql files to be manually ran against
#          the Splice Machine database.  This will allow the scripts to be passed around or reran needed

# Usage: ./export-schema-from-oracle.sh

source oracle-variables.sh
java -cp $DM_JAR:$ORACLE_JDBC_JAR com.splicemachine.cs.databasemigration.MigrateDatabase -configFile $CONFIG_FILE -connectionId "oracle" -configId default


