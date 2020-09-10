package com.splicemachine.cs.databasemigration.output;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

import org.apache.log4j.Level;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConfig;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.output.BaseOutput;
import com.splicemachine.cs.databasemigration.schema.Table;

public class SpliceOutput extends BaseOutput {
    
    private static final int BATCH_SIZE = 10000;

    Statement stmt = null;
    PreparedStatement pStmt = null;
    
    private Timestamp TIMESTAMP_FUTURE;
    private Timestamp TIMESTAMP_PAST;

    public SpliceOutput(MigrateDatabaseConfig config) {
        super(config);
    
        Calendar future = Calendar.getInstance();
        future.set(Calendar.YEAR, 2199);
        TIMESTAMP_FUTURE = new Timestamp(future.getTimeInMillis());
        
        Calendar past = Calendar.getInstance();
        past.set(Calendar.YEAR, 1678);
        TIMESTAMP_PAST = new Timestamp(past.getTimeInMillis());
        

    
    }

    /**
     * Logic for the beginning process of creating tables for a schema
     */
    public void startCreateTable(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING TABLE SCRIPT FOR SCHEMA:"
                + schema);
        try {
            getStatement().executeUpdate("CREATE SCHEMA " + schema);
        } catch (SQLException e) {
            if (!e.getSQLState().equals("X0Y68")) {
                MigrateDatabaseUtils.logError(
                        "Exception creating schema for table", e);
                throw e;
            }
        }
    }

    /**
     * Logic for creating a table for a schema
     */
    public void outputCreateTable(String schema, String nameWithQuotes, String name, String createStmt, String insertColumnList, boolean useOneLineRecords, String charSet)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING TABLE SCRIPT FOR TABLE:"
                + name);
        MigrateDatabaseUtils.writeMessage(createStmt);

        stmt.executeUpdate(createStmt);
    }

    /**
     * Logic for the ending process of creating tables for a schema
     */
    public void endCreateTable() {
        closeStatement();
    }

    /**
     * Logic for the beginning process of creating sequences for a schema
     */
    public void startCreateSequence(String schema) throws Exception {
        MigrateDatabaseUtils
                .writeMessage("CREATING SEQUENCE SCRIPT FOR SCHEMA:" + schema);
    }

    /**
     * Logic for creating a sequence for a schema
     */
    public void ouputCreateSequence(String schema, String sName,
            String createStmt) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING SEQUENCE FOR SEQUENCE:"
                + sName);
        stmt.executeUpdate(createStmt);
    }

    /**
     * Logic for the ending process of creating sequences for a schema
     */
    public void endCreateSequence() {
        closeStatement();
    }

    /**
     * Logic for the beginning process of creating indexes for a schema
     */
    public void startCreateIndex(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING INDEX SCRIPT FOR SCHEMA:"
                + schema);
    }

    /**
     * Logic for creating a index for a schema
     */
    public void ouputCreateIndex(String schema, String sName, String createStmt)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING INDEX FOR INDEX:" + sName);
        stmt.executeUpdate(createStmt);
    }

    /**
     * Logic for the ending process of creating index for a schema
     */
    public void endCreateIndex() {
        closeStatement();
    }

    /**
     * Logic for the beginning process of creating indexes for a schema
     */
    public void startCreateUniqueIndex(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING UNIQUE INDEX SCRIPT FOR SCHEMA:"
                + schema);
    }

    /**
     * Logic for creating a index for a schema
     */
    public void ouputCreateUniqueIndex(String schema, String sName, String createStmt)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING UNIQUE INDEX FOR INDEX:" + sName);
        stmt.executeUpdate(createStmt);
    }

    /**
     * Logic for the ending process of creating index for a schema
     */
    public void endCreateUniqueIndex() {
        closeStatement();
    }
    
    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE TRIGGERS SCRIPTS                   */
    /*************************************************************/

    
    /**
     * Logic for the beginning process of creating indexes for a schema
     */
    public void startCreateTriggers(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING TRIGGER SCRIPT FOR SCHEMA:"
                + schema);
    }

    /**
     * Logic for creating a trigger for a schema
     */
    public void ouputCreateTriggers(String schema, String sName, String createStmt)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING TRIGGER FOR INDEX:" + sName);
        stmt.executeUpdate(createStmt);
    }

    /**
     * Logic for the ending process of creating trigger for a schema
     */
    public void endCreateTriggers() {
        closeStatement();
    }

    
    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE FOREIGN KEY SCRIPTS                */
    /*************************************************************/

    public void startCreateForeignKey(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING FOREIGN KEY - NOT IMPLEMENTED:" + schema);
    }

    public void ouputCreateForeignKey(String schema, String sName, String createStmt)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING FOREIGN KEY - NOT IMPLEMENTED:" + schema);
    }

    public void endCreateForeignKey() {
        MigrateDatabaseUtils.writeMessage("CREATING FOREIGN KEY - NOT IMPLEMENTED:");
    }

    public void startCreateConstraint(ArrayList<String> schemas) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING CONSTRAINT - NOT IMPLEMENTED");
    }

    public void outputCreateConstraint(String schema, String sName, String createStmt)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING CONSTRAINT - NOT IMPLEMENTED:" + schema);
    }

    public void endCreateConstraint() {
        MigrateDatabaseUtils.writeMessage("CREATING CONSTRAINT - NOT IMPLEMENTED:");
    }

    public void createUsers(ArrayList<String> users) {
        MigrateDatabaseUtils.writeMessage("CREATE USERS - NOT IMPLEMENTED");
    }

    /**
     * Logic for creating sqoop query files
     */
    public void outputSqoopQueryFiles(String schema, String table, String query)
            throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING SQOOP QUERY FILE FOR TABLE:"
                + table);
        MigrateDatabaseUtils.writeMessage(query);
    }

    /**
     * Logic for the ending process of creating sqoop query files
     */
    public void endSqoopQueryFiles() {
        closeStatement();
    }

    public void startExportData(Table table, ResultSetMetaData resMeta)
            throws Exception {
        writeMessage("Exporting via direct import: " + table.getSourceFullTableName());
        String tableName = table.getSourceFullTableName();
        tableName = tableName.replace("$", "");
        int columnCount = resMeta.getColumnCount();
        StringBuilder insert = new StringBuilder();
        insert.append("INSERT INTO ");
        insert.append(tableName);
        insert.append(" VALUES (");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0)
                insert.append(",");
            insert.append("?");
        }
        insert.append(")");
        pStmt = config.getTargetDatabaseConnection().prepareStatement(
                insert.toString());
    }

    public void ouputExportData(Table table, ResultSetMetaData resMeta,
            ResultSet res) throws Exception {
        try {
            long startTime = System.currentTimeMillis();
            long total = 0;
            int numObjectsPerFile = 0;
            int columnCount = resMeta.getColumnCount();
            String tableName = table.getSourceFullTableName();

            int[] columntypes = new int[columnCount];
            for (int i = 0; i < columnCount; i++) {
                columntypes[i] = resMeta.getColumnType(i + 1);
            }

            while (res.next()) {
                if (numObjectsPerFile != 0 && numObjectsPerFile % BATCH_SIZE == 0) {
                    pStmt.executeBatch();
                    numObjectsPerFile = 0;
                    writeMessage(Level.WARN, "total so far: [" + total
                            + "] duration=[" + (System.currentTimeMillis() - startTime) / BATCH_SIZE
                            + " seconds]");
                }
                numObjectsPerFile++;
                total++;


                for (int i = 1; i <= columnCount; i++) {
                    switch (columntypes[i - 1]) {
                    case java.sql.Types.BINARY:
                    case java.sql.Types.LONGVARBINARY:
                    case java.sql.Types.VARBINARY:
                        pStmt.setBytes(i, res.getBytes(i));
                        break;
                    case java.sql.Types.BLOB:
                        pStmt.setBlob(i, res.getBlob(i));
                        break;
                    case java.sql.Types.CLOB:
                        pStmt.setClob(i, res.getClob(i));
                        break;
                    case java.sql.Types.BIT:
                    case java.sql.Types.BOOLEAN:
                        pStmt.setBoolean(i, res.getBoolean(i));
                        break;
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                        pStmt.setShort(i, res.getShort(i));
                        break;
                    case java.sql.Types.INTEGER:
                        pStmt.setInt(i, res.getInt(i));
                        break;
                    case java.sql.Types.BIGINT:
                        pStmt.setLong(i, res.getLong(i));
                        break;
                    case java.sql.Types.REAL:
                    case java.sql.Types.FLOAT:
                        pStmt.setFloat(i, res.getFloat(i));
                        break;
                    case java.sql.Types.DOUBLE:
                        pStmt.setDouble(i, res.getDouble(i));
                        break;
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        pStmt.setBigDecimal(i, res.getBigDecimal(i));
                        break;
                    case java.sql.Types.DATE:
                        pStmt.setDate(i, res.getDate(i));
                        
                        break;
                    case java.sql.Types.TIME:
                        pStmt.setTime(i, res.getTime(i));
                        break;
                    case java.sql.Types.TIMESTAMP:
                        Timestamp temp = res.getTimestamp(i);
                        if(temp != null) {
                            int year = temp.getYear();
                            if(year > 2199) {
                                pStmt.setTimestamp(i, TIMESTAMP_FUTURE);
                            } else if (year < 1678) {
                                pStmt.setTimestamp(i, TIMESTAMP_PAST);
                            } else {
                                pStmt.setTimestamp(i, temp);
                            }
                        } else {
                            pStmt.setTimestamp(i, temp);
                        }
                        
                        break;
                    case java.sql.Types.CHAR:
                    case java.sql.Types.LONGVARCHAR:
                    case java.sql.Types.NVARCHAR:
                    case java.sql.Types.VARCHAR:
                        pStmt.setString(i, res.getString(i));
                        break;
                    default:
                        throw new Exception("Type not supported:"
                                + resMeta.getColumnTypeName(i));
                    }

                }
                pStmt.addBatch();
            }
            pStmt.executeBatch();
            long endTime = System.currentTimeMillis();
            writeMessage(Level.WARN, "total records imported: [" + total
                    + "] duration=[" + (endTime - startTime) / BATCH_SIZE
                    + " seconds]");
        } finally {
            try {
                res.close();
            } catch (Exception ignore) {

            }
        }
    }

    public void endExportData() {
        try {
            pStmt.close();
        } catch (SQLException e) {
            // Ignore
        }
    }

    public Statement getStatement() throws SQLException {
        if (stmt == null) {
            Connection conn = config.getTargetDatabaseConnection();
            stmt = conn.createStatement();
        }
        return stmt;
    }

    public void closeStatement() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                // Ignore
            }
        }
    }

}
