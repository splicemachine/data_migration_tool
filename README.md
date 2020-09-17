# PURPOSE
Customer Solutions needed a tool to be able to migrate a database schema from a third party database vendor to Splice Machine.  This tool uses a JDBC connection to connect to the third party database (Oracle and SQL Server) and (optionally) Splice Machine.  

Additionally, this tool has two java files that can be used to generate CSV files for the results of running the sqoop / splice export / import process and the splice import process.

The migration tool is very flexible and therefore provides many options for doing the migration.  The migration can be done using intermediate files or it can be done with a direct database connection.  It is recommended to use intermediate scripts so that they can re-executed as needed.  


# FEATURES
- Creates Splice Machine compatible DDL scripts for schemas, tables, users, foreign keys, sequences, and indexes
- - Export all schema's objects or just specific schemas
- - Ability to include or exclude certain tables when processing a schema
- - Exports table column defaults
- - Exports Users
- - Ability to export check constraints
- - Specific Column Data Type Mapping - useful for Oracle DATE data types or NUMBER fields
- Creates import scripts compatible with Splice Machine's bulk loader
- Exports views, triggers, function, stored procedures and packages to files for easier analysis
- Creates SQOOP scripts for exporting / importing the data to Splice Machine
- Summary of Splice Imports from log files
- Summary of Sqoop / Splice Combination from log files

# FUTURE FEATURES
There are a couple of additional features that would be nice to have in the future
- Ability to map a source schema to a different target name.  The hooks for this are in place in the `my-config.xml` (see: schemaNameMapping).  And the Tables.java has been updated to have a source and target schema and table name.  The code has not been completely updated to alwasy refer to the target schema name when writing out the table, index, etc scripts.

# HOW TO COMPILE
This code is compiled using maven.  If you do not have maven installed please install maven 3.0 (see http://maven.apache.org/).  To compile the code type:

`mvn clean compile dependency:copy-dependencies package`

That will create a jar file called **database-migration-0.0.1-SNAPSHOT.jar** in the target directory.


# DEPLOYING IT TO A SEPARATE SERVER
If you want to deploy this process to a server other than where it was compiled.  You will need to copy the following files to the server:

* ./target/database-migration-0.0.1-SNAPSHOT.jar
* /src/main/resources/config/my-config.xml
* /src/main/resources/databaseVendors.xml
* Any JDBC jar files for source database (ie sqljdbc4.jar or ojdbc6.jar)


# HOW TO RUN
The basic command for running this tool is as follows:


`java -cp $DM_JAR:$ORACLE_JDBC_JAR com.splicemachine.cs.databasemigration.MigrateDatabase -configFile my-config.xml -connectionId oracle -configId default`

Where you would replace the `$DM_JAR` with the location of the jar file created when you compiled the source code.  And you would replace the `$ORACLE_JDBC_JAR` with the jar file for the third party database you are connecting with through JDBC. 

You must specify the three options:

* configFile - This is the file which contains your configuration settings.  See `src/main/resources/config/my-config.xml` as an example
* connectionId -  This is the id of the corresponding entry of a 'connection' element in the configFile with has the target and source database you are trying to connect to.  There are samples of Oracle, Sql Server and Splice Machine
* configId - This is the id of the corresponding entry of a 'config' element in the configFile with has the options for the run you are trying to execute. 


# CONFIGURATION RUN OPTIONS
There are a lot of configuration options that can be specified when running this tool.  See the file `src/main/resources/config/my-config.xml` as it is self 
documenting.  It has each option that is supported by the tool and a description of each. 


# UNDERSTANDING THE CODE
## com.splicemachine.cs.datamigration.MigrateDatabase
This is the class that gets called when you kick off the data migration process.  It uses the class MigrateDatabaseConfig to process all of the parameters / options for the run.  If you look at the main method that the flow
 
## com.splicemachine.cs.datamigration.MigrateDatabaseConfig
This class is used to process the run parameters / reading the options from the configuration file being used.

## com.splicemachine.cs.datamigration.MigrateDatabaseConstants
This class contains constants for the tool

## com.splicemachine.cs.datamigration.MigrateDatabaseUtils
This class has utility methods mainly for writing output to the console or display the database properties

## com.splicemachine.cs.datamigration.output
When you run this tool you have the option of either writing the output to a file or connecting directly to splice machine and executing the logic there.  The file FileOutpt.java contains the logic (mostly) for writing data out to files.  The file SpliceOutput.java has the logic for connecting directly to Splice Machine and executing the statements.

## com.splicemachine.cs.datamigration.schema
This directory contains an object Table that is used throught the process

## com.splicemachine.cs.datamigration.utils.ParseImportLogFile
Tool for parsing a directory or file of splice imports

## com.splicemachine.cs.datamigration.utils.ParseSqoopAndImportLogFile
Tool for parsing a directory or file of Sqoop/Splice extract/import lgos

## com.splicemachine.cs.datamigration.vendors
This package has database vendor specific sql.  For example, the sql to retrieve the users varies by database vendor. 



# EXAMPLES


