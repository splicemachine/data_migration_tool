package com.splicemachine.cs.databasemigration;

import com.splicemachine.cs.databasemigration.schema.Table;
import com.splicemachine.cs.databasemigration.vendors.BaseDatabaseVendor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import com.splicemachine.cs.databasemigration.schema.ColumnMetadata;

/**
 * Utilities for the Database Migration Tool
 * 
 * @author tedmonddong
 * 
 */
public class MigrateDatabaseUtils {

    private static final Logger LOG = Logger
            .getLogger(MigrateDatabaseUtils.class);

    /**
     * Prints a list of the tables
     * 
     * @param tables
     */
    public static void printTables(ArrayList<Table> tables) {
        writeMessage("TABLE,NUM RECORDS");
        for (Table table : tables) {
            writeMessage("   " + table.getSourceFullTableName() + ","
                    + table.getNumberOfRecords());
        }
    }

    /**
     * Prints the table data types
     * 
     * @param tables
     */
    public static void printSQLDataTypes() {
        writeMessage("JDBC DATATYPES: ");
        writeMessage("   Array: " + java.sql.Types.ARRAY);
        writeMessage("   Bigint: " + java.sql.Types.BIGINT);
        writeMessage("   Binary: " + java.sql.Types.BINARY);
        writeMessage("   Bit: " + java.sql.Types.BIT);
        writeMessage("   Blob: " + java.sql.Types.BLOB);
        writeMessage("   Boolean: " + java.sql.Types.BOOLEAN);
        writeMessage("   Char: " + java.sql.Types.CHAR);
        writeMessage("   Clob: " + java.sql.Types.CLOB);
        writeMessage("   Datalink: " + java.sql.Types.DATALINK);
        writeMessage("   Date: " + java.sql.Types.DATE);
        writeMessage("   Decimal: " + java.sql.Types.DECIMAL);
        writeMessage("   Distinct: " + java.sql.Types.DISTINCT);
        writeMessage("   Double: " + java.sql.Types.DOUBLE);
        writeMessage("   Float: " + java.sql.Types.FLOAT);
        writeMessage("   Integer: " + java.sql.Types.INTEGER);
        writeMessage("   Java Object: " + java.sql.Types.JAVA_OBJECT);
        writeMessage("   Longnvarchar: " + java.sql.Types.LONGNVARCHAR);
        writeMessage("   Longvarbinary: " + java.sql.Types.LONGVARBINARY);
        writeMessage("   Longvarchar: " + java.sql.Types.LONGVARCHAR);
        writeMessage("   Nchar: " + java.sql.Types.NCHAR);
        writeMessage("   Nclob: " + java.sql.Types.NCLOB);
        writeMessage("   Null: " + java.sql.Types.NULL);
        writeMessage("   Numeric: " + java.sql.Types.NUMERIC);
        writeMessage("   Nvarchar: " + java.sql.Types.NVARCHAR);
        writeMessage("   Other: " + java.sql.Types.OTHER);
        writeMessage("   Real: " + java.sql.Types.REAL);
        writeMessage("   Ref: " + java.sql.Types.REF);
        writeMessage("   Rowid: " + java.sql.Types.ROWID);
        writeMessage("   Smallint: " + java.sql.Types.SMALLINT);
        writeMessage("   Sqlxml: " + java.sql.Types.SQLXML);
        writeMessage("   Struct: " + java.sql.Types.STRUCT);
        writeMessage("   Time: " + java.sql.Types.TIME);
        writeMessage("   Timestamp: " + java.sql.Types.TIMESTAMP);
        writeMessage("   Tinyint: " + java.sql.Types.TINYINT);
        writeMessage("   Varbinary: " + java.sql.Types.VARBINARY);
        writeMessage("   Varchar: " + java.sql.Types.VARCHAR);
    }

    /**
     * This should be used to get summary information for the entire catalog /
     * schema. We want to know:
     * 
     * # tables # views # procedures # functions # indexes
     */
    public static void printSchemaStats(java.sql.Connection con, BaseDatabaseVendor vendor, final DatabaseMetaData md,
                                        final String catalog, final String schemaPattern) {

        ResultSet rs;

        // Get the list of catalogs
        try {
            writeMessage("   Getting catalogs");
            int counter = 0;
            rs = vendor.getCatalogs(con, md);
            writeMessage("   Catalog Name");
            while (rs.next()) {
                writeMessage("   " + rs.getString(1));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting catalogs", e);
        }

        // Get the list of catalogs
        try {
            writeMessage("   Getting schemas");
            int counter = 0;
            rs = vendor.getSchemas(con,md);
            writeMessage("    Schema, Catalog");
            while (rs.next()) {
                writeMessage("    " + rs.getString(1) + "," + rs.getString(2));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting schemas", e);
        }

        try {
            writeMessage("   Getting tables");
            int counter = 0;
            rs = vendor.getTables(con,md, catalog, schemaPattern);
            writeMessage("   Catalog, Schema, Table Name");
            while (rs.next()) {
                writeMessage("   " + rs.getString(1) + "," + rs.getString(2) + ","
                        + rs.getString(3));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting tables", e);
        }

        try {
            writeMessage("   Getting views");
            int counter = 0;
            rs = vendor.getViews(con,md, catalog, schemaPattern);
            writeMessage("   Catalog, Schema, Table Name");
            while (rs.next()) {
                writeMessage("   " + rs.getString(1) + "," + rs.getString(2) + ","
                        + rs.getString(3));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting views", e);
        }

        try {
            writeMessage("   Getting functions");
            int counter = 0;
            rs = vendor.getFunctions(con,md, catalog, schemaPattern);
            writeMessage("   Catalog, Schema, Function Name");
            while (rs.next()) {
                writeMessage("   " + rs.getString(1) + "," + rs.getString(2) + ","
                        + rs.getString(3));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting functions", e);
        }

        try {
            writeMessage("   Getting procedures");
            int counter = 0;
            rs = vendor.getProcedures(con,md, catalog, schemaPattern);
            writeMessage("   Catalog, Schema, Function Name");
            while (rs.next()) {
                writeMessage("   " + rs.getString(1) + "," + rs.getString(2) + ","
                        + rs.getString(3));
                counter++;
            }
            writeMessage("*** Total:" + counter);
            rs.close();
        } catch (Exception e) {
            logError("Exception getting procedures", e);
        }
    }

    /**
     * Writes out a message
     * 
     * @param message
     */
    public static void writeMessage(String message) {
        writeMessage(Level.INFO, message);
    }

    public static void writeErrorMessage(String message) {
        writeMessage(Level.ERROR, message);
    }

    public static void writeMessage(Level level, String message) {
        LOG.log(level, message);
    }

    public static void logError(String s) {
        LOG.info(s);
    }

    public static void logError(String s, Exception ex) {
        if (ex instanceof SQLException) {
            logError("SQLException: " + ex.getMessage());
            logError("SQLState: " + ((SQLException) ex).getSQLState());
            logError("VendorError: " + ((SQLException) ex).getErrorCode());
        }
        LOG.info(s, ex);
    }

    /**
     * When you pass in a table resultset it prints the details of the reduct
     * set.
     * 
     * @param table
     * @throws SQLException
     */
    public static void printIndexDetails(ResultSet table) throws SQLException {
/*        
        ResultSetMetaData rs_meta = table.getMetaData();
        for (int x = 1; x <= rs_meta.getColumnCount(); x++) {
            writeMessage("index attr: " + rs_meta.getColumnName(x));
        }
*/
        
        writeMessage("INDEX NAME: " + table.getString("INDEX_NAME"));
        writeMessage("   TABLE CAT: " + table.getString("TABLE_CAT"));
        writeMessage("   TABLE SCHEME: " + table.getString("TABLE_SCHEM"));
        writeMessage("   TABLE NAME: " + table.getString("TABLE_NAME"));
        writeMessage("   NON UNIQUE: " + table.getString("NON_UNIQUE"));
        writeMessage("   INDEX QUALIFIER: "
                + table.getString("INDEX_QUALIFIER"));
        writeMessage("   TYPE: " + table.getString("TYPE"));
        writeMessage("   ORDINAL POSITION: "
                + table.getString("ORDINAL_POSITION"));
        writeMessage("   COLUMN NAME: " + table.getString("COLUMN_NAME"));
        writeMessage("   ASC/DESC: " + table.getString("ASC_OR_DESC"));
        writeMessage("   CARDINALITY: " + table.getString("CARDINALITY"));
        writeMessage("   PAGES: " + table.getString("PAGES"));
        writeMessage("   FILTER CONDITION: "
                + table.getString("FILTER_CONDITION"));

    }

    /**
     * Prints out the columns details
     * 
     * @param rsmd
     * @param i
     * @param columns
     * @throws SQLException
     */
    public static void printColumnDetails(ColumnMetadata columns)
    {
        writeMessage("**** COLUMN:");
        writeMessage("         TABLE_CAT: " + columns.getTableCat());
        writeMessage("         TABLE_SCHEM: "
                + columns.getTableSchema());
        writeMessage("         TABLE_NAME: " + columns.getTableName());
        writeMessage("         COLUMN_NAME: "
                + columns.getColumnName());
        writeMessage("         DATA_TYPE: " + columns.getDataType());
        writeMessage("         TYPE_NAME: " + columns.getTypeName());
        writeMessage("         COLUMN_SIZE: " + columns.getColumnSize());
        writeMessage("         BUFFER_LENGTH: "
                + columns.getBufferLength());
        writeMessage("         DECIMAL_DIGITS: "
                + columns.getDecimalDigits());
        writeMessage("         NUM_PREC_RADIX: "
                + columns.getNumPrecRadix() );
        writeMessage("         NULLABLE: " + columns.getNullable());
        writeMessage("         REMARKS: " + columns.getRemarks());
        writeMessage("         COLUMN_DEF: " + columns.getColumnDefault());
        writeMessage("         SQL_DATA_TYPE: "
                + columns.getSqlDataType());
        writeMessage("         SQL_DATETIME_SUB: "
                + columns.getSqlDateTimeSub());
        writeMessage("         CHAR_OCTET_LENGTH: "
                + columns.getCharOctetLength());
        writeMessage("         ORDINAL_POSITION: "
                + columns.getOrdinalPosition());
        writeMessage("         IS_NULLABLE: "
                + columns.getIsNullable());
        /*
        writeMessage("         SCOPE_CATALOG: "
                + columns.getString("SCOPE_CATALOG"));
        writeMessage("         SCOPE_SCHEMA: "
                + columns.getString("SCOPE_SCHEMA"));
        writeMessage("         SCOPE_TABLE: "
                + columns.getString("SCOPE_TABLE"));
        writeMessage("         SOURCE_DATA_TYPE: "
                + columns.getString("SOURCE_DATA_TYPE"));
                */
        writeMessage("         IS_AUTOINCREMENT: "
                + columns.getIsAutoIncrement());
    }

    /**
     * Prints out the foreign key details
     * 
     * @param ik
     *            - Resultset with the foreign key
     * @throws SQLException
     */
    public static void printForeignKeyDetails(ResultSet ik) throws SQLException {
        /*
        ResultSetMetaData rs_meta = ik.getMetaData();
        for (int x = 1; x <= rs_meta.getColumnCount(); x++) {
            writeMessage("fkcol attr: " + rs_meta.getColumnName(x));
        }
        */
        writeMessage("FK_NAME=" + ik.getString("FK_NAME"));
        writeMessage("    PKTABLE_CAT=" + ik.getString("PKTABLE_CAT"));
        writeMessage("    PKTABLE_SCHEM=" + ik.getString("PKTABLE_SCHEM"));
        writeMessage("    PKTABLE_NAME=" + ik.getString("PKTABLE_NAME"));
        writeMessage("    PKCOLUMN_NAME=" + ik.getString("PKCOLUMN_NAME"));
        writeMessage("    FKTABLE_CAT=" + ik.getString("FKTABLE_CAT"));
        writeMessage("    FKTABLE_SCHEM=" + ik.getString("FKTABLE_SCHEM"));
        writeMessage("    FKTABLE_NAME=" + ik.getString("FKTABLE_NAME"));
        writeMessage("    FKCOLUMN_NAME=" + ik.getString("FKCOLUMN_NAME"));
        writeMessage("    KEY_SEQ=" + ik.getString("KEY_SEQ"));
        writeMessage("    UPDATE_RULE=" + ik.getString("UPDATE_RULE"));
        writeMessage("    DELETE_RULE=" + ik.getString("DELETE_RULE"));
        writeMessage("    PK_NAME=" + ik.getString("PK_NAME"));
        writeMessage("    DEFERRABILITY=" + ik.getString("DEFERRABILITY"));
    }

    /**
     * Prints out the primary key details
     * 
     * @param pk
     *            - Result set with the primary key details
     * @throws SQLException
     */
    public static void printPrimaryKeyDetails(ResultSet pk) throws SQLException {
/*
        ResultSetMetaData rs_meta = pk.getMetaData();

        for (int x = 1; x <= rs_meta.getColumnCount(); x++) {
            writeMessage("pk column attr: " + rs_meta.getColumnName(x));
        }
*/
        writeMessage("getPrimaryKeys(): TABLE_CAT=" + pk.getString("TABLE_CAT"));
        writeMessage("getPrimaryKeys(): TABLE_SCHEM=" + pk.getString("TABLE_SCHEM"));
        writeMessage("getPrimaryKeys(): TABLE_NAME=" + pk.getString("TABLE_NAME"));
        writeMessage("getPrimaryKeys(): COLUMN_NAME=" + pk.getString("COLUMN_NAME"));
        writeMessage("getPrimaryKeys(): KEY_SEQ=" + pk.getString("KEY_SEQ"));
        writeMessage("getPrimaryKeys(): PK_NAME=" + pk.getString("PK_NAME"));
    }

}
