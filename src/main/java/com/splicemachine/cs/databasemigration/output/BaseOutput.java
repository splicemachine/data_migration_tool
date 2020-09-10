package com.splicemachine.cs.databasemigration.output;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConfig;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.schema.Table;

public abstract class BaseOutput {

    MigrateDatabaseConfig config = null;

    static final Logger LOG = Logger.getLogger(BaseOutput.class);

    public BaseOutput(MigrateDatabaseConfig config) {
        this.config = config;
    }

    public abstract void startCreateTable(String schema) throws Exception;

    public abstract void outputCreateTable(String schema, String sNameWithQuotes, String sName,
            String createStmt, String insertColumnList, boolean useOneLineRecords, String charSet) throws Exception;

    public abstract void endCreateTable();

    public abstract void startCreateSequence(String schema) throws Exception;

    public abstract void ouputCreateSequence(String schema, String sName,
            String createStmt) throws Exception;

    public abstract void endCreateSequence();

    public abstract void startCreateIndex(String schema) throws Exception;

    public abstract void ouputCreateIndex(String schema, String sName,
            String createStmt) throws Exception;

    public abstract void endCreateIndex();

    public abstract void startCreateUniqueIndex(String schema) throws Exception;

    public abstract void ouputCreateUniqueIndex(String schema, String sName,
            String createStmt) throws Exception;

    public abstract void endCreateUniqueIndex();

    public abstract void startCreateForeignKey(String schema) throws Exception;

    public abstract void ouputCreateForeignKey(String schema, String sName,
            String createStmt) throws Exception;

    public abstract void endCreateForeignKey();

    public abstract void startCreateConstraint(ArrayList<String> schemaList) throws Exception;

    public abstract void outputCreateConstraint(String schema, String sName, String createStmt) throws Exception;

    public abstract void endCreateConstraint();
    
    public abstract void createUsers(ArrayList<String> users);
    
    public abstract void startExportData(Table table, ResultSetMetaData resMeta)
            throws Exception;

    public abstract void ouputExportData(Table table,
            ResultSetMetaData resMeta, ResultSet res) throws Exception;

    public abstract void endExportData();

    public abstract void outputSqoopQueryFiles(String schema, String table, String query)
    		throws Exception;
    
    public abstract void endSqoopQueryFiles();
    
    public void writeMessage(String message) {
        MigrateDatabaseUtils.writeMessage(Level.INFO, message);
    }

    public void writeErrorMessage(String message) {
        MigrateDatabaseUtils.writeMessage(Level.ERROR, message);
    }

    /**
     * Writes out a message based on the current log level
     * 
     * @param message
     * @param level
     */
    public void writeMessage(Level level, String message) {
        MigrateDatabaseUtils.writeMessage(level, message);
    }

}
