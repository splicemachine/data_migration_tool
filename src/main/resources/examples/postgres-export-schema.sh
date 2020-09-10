#!/bin/bash

# Purpose: The purpose of this script is to extract the table definition and schemas to sql files to be manually ran against
#          the Splice Machine database.  This will allow the scripts to be passed around or reran needed

# Usage: ./export-schema-from-postgres.sh

source postgres-variables.sh
java -cp $DM_JAR:$POSTGRES_JDBC_JAR com.splicemachine.cs.databasemigration.MigrateDatabase -configFile $CONFIG_FILE -connectionId "postgres" -configId default


