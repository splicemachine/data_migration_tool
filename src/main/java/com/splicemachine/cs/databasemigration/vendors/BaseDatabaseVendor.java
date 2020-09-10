package com.splicemachine.cs.databasemigration.vendors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.output.BaseOutput;
import com.splicemachine.cs.databasemigration.schema.Table;

public class BaseDatabaseVendor {

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);



    public String jdbcPrefix = "";
    public String label = "";
    public String driverClass = "";
    public String urlFormat = "";
    public boolean useSchema = false;
    public SubnodeConfiguration vendorProperties = null;
    public String selectAlllimitRows = "select * from {TABLE} limit {LIMIT_NUM}";

    public BaseDatabaseVendor(SubnodeConfiguration config) {
        processVendorProperties(config);
    }

    public void processVendorProperties(SubnodeConfiguration config) {
        this.vendorProperties = config;

        String sClassName = vendorProperties.getString("migrationClass");

        this.useSchema = vendorProperties.getBoolean("/database/useSchema",
                false);

        this.jdbcPrefix = vendorProperties.getString("/jdbc/prefix");
        this.label = vendorProperties.getString("/jdbc/label");
        this.driverClass = vendorProperties.getString("/jdbc/driverClass");
        this.urlFormat = vendorProperties.getString("/jdbc/urlFormat");

        this.selectAlllimitRows = vendorProperties.getString(
                "/sqlSyntax/selectWithLimit", this.selectAlllimitRows);

        printParms();

    }

    /**
     * Print out the parameters for the database vendor
     * 
     */
    public void printParms() {
        LOG.debug("jdbcPrefix:" + jdbcPrefix);
        LOG.debug("label:" + label);
        LOG.debug("driverClass:" + driverClass);
        LOG.debug("urlFormat:" + urlFormat);

        LOG.debug("useSchema:" + useSchema);

        LOG.debug("selectAlllimitRows:" + selectAlllimitRows);
    }

    /**
     * Returns a connection based on the passed in Connection String, user id
     * and password
     * 
     * @param connectionString
     * @param user
     * @param password
     * @return
     * @throws SQLException
     */
    public Connection getConnection(String connectionString, String user,
            String password) throws SQLException {
        if (user != null && password != null) {
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            return DriverManager.getConnection(connectionString, props);
        }
        return DriverManager.getConnection(connectionString);
    }

    public ResultSet getCatalogs(Connection con, DatabaseMetaData md) {
        ResultSet rs = null;
        try {
            rs = md.getCatalogs();
        } catch (Exception e) {
            LOG.error("Exception getting catalogs", e);
        }
        return rs;
    }
    public ResultSet getSchemas(Connection con, DatabaseMetaData md) {
        ResultSet rs = null;
        try {
            rs = md.getSchemas();
        } catch (Exception e) {
            LOG.error("Exception getting schemas", e);
        }
        return rs;
    }
    public ResultSet getTables(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        ResultSet rs = null;
        try {

            if(con == null) {
                LOG.error("The database connection is null");
            }

            if(md == null) {
                LOG.error("The DatabaseMetaData is null");
            }

            LOG.info("getTables: catalog=[" + catalog + "] schemaPattern=[" + schemaPattern + "]");
            rs = md.getTables(catalog, schemaPattern, null,
                    new String[] { "TABLE" });
        } catch (Exception e) {
            LOG.error("Exception getting tables", e);
        }
        return rs;
    }
    public ResultSet getViews(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        ResultSet rs = null;
        try {
            rs = md.getTables(catalog, schemaPattern, null,
                    new String[] { "VIEW" });
        } catch (Exception e) {
            LOG.error("Exception getting views", e);
        }
        return rs;
    }
    public ResultSet getFunctions(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        ResultSet rs = null;
        try {
            rs = md.getFunctions(catalog, schemaPattern, null);
        } catch (Exception e) {
            LOG.error("Exception getting functions", e);
        }
        return rs;
    }
    public ResultSet getProcedures(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        ResultSet rs = null;
        try {
            rs = md.getProcedures(catalog, schemaPattern, null);
        } catch (Exception e) {
            LOG.error("Exception getting procedures", e);
        }
        return rs;
    }

    /**
     * SQL Syntax for retrieving all the records from a table using a select
     * statement with a limiting the number of records to return
     * 
     * @return
     */
    public String getAllColumnsLimitedCountSyntax() {
        return selectAlllimitRows;
    }

    public HashMap<String, String> getSequenceDefinition(Connection con,
            String schema, String seqName) {
        LOG.error("getSequenceDefinition - Not implemented for the database vendor");
        return null;
    }
    
    public String[] getListOfTriggers(Connection conn) {
        LOG.error("getListOfTriggers - Not implemented for the database vendor");
        return null;
    }
    
    public void exportTrigger(Connection con, String schema, String objectName,
            String outputPath) {
        
        LOG.error("exportTrigger - Not implemented for the database vendor");
    }
    
    public String[] getListOfPackage(Connection conn) {
        LOG.error("getListOfPackage - Not implemented for the database vendor");
        return null;
    }

    public void exportPackage(Connection con, String schema, String table,
            String outputPath) {
        LOG.error("exportPackage - Not implemented for the database vendor");
    }
    
    public String[] getListOfProcedures(Connection conn) {
        LOG.error("getListOfProcedures - Not implemented for the database vendor");
        return null;
    }

    public void exportProcedure(Connection con, String schema, String table,
            String outputPath) {
        
        LOG.error("exportProcedure - Not implemented for the database vendor");
    }
    
    public String[] getListOfFunctions(Connection conn) {
        LOG.error("getListOfFunctions - Not implemented for the database vendor");
        return null;
    }

    public void exportFunction(Connection con, String schema, String table,
            String outputPath) {
        LOG.error("exportFunction - Not implemented for the database vendor");
    }
    
    public String[] getListOfViews(Connection conn) {
        LOG.error("getListOfViews - Not implemented for the database vendor");
        return null;
    }

    public void exportView(Connection con, String schema, String table,
            String outputPath) {
        LOG.error("exportView - Not implemented for the database vendor");
    }

    
    public void exportCheckConstraints(Connection con, ArrayList<String> schema,
                                       BaseOutput output) {
        LOG.error("exportCheckConstraints - Not implemented for the database vendor");
    }
    
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        LOG.error("getUsers - Not implemented for the database vendor");
        return null;
    }
    
    public void exportRoles(Connection con, String outputPath) {
        LOG.error("exportRoles - Not implemented for the database vendor");
    }
    
    public boolean hasCustomMapping() {
        return false;
    }
    
    public String getConvertedColumnType(Connection conn,String tableName, String columnName,
            int type, String typeName, long precision, int scale, String nullable) {
        LOG.error("getConvertedColumnType - Not implemented for the database vendor");
        return null;
    }
    
    public String getDateDataType(Connection conn,String schema, String tableName, String columnName,
            int type, String typeName, long precision, int scale) {
        return "DATE";
    }
    
    public String getNumericDataType(Connection conn,String schema, String tableName, String columnName,
            int type, String typeName, long precision, int scale) {
        return "DOUBLE";
    }
    
    public HashMap<String,String> getTableColumnDefault(Connection con, Table table) {
        LOG.error("getTableColumnDefault - Not implemented for the database vendor");
        return new HashMap<String,String>();
    }
    
    /**
     * Retrieves the foreign keys are exports them
     */
    public ArrayList<String> exportForeignKeys(Connection conn, DatabaseMetaData meta, Table table, BaseOutput output)  throws Exception{
        ArrayList<String> foreignKeyNames = new ArrayList<String>();
        
        StringBuffer foreignKey = new StringBuffer();
        ResultSet fk = meta.getImportedKeys(null, table.getSourceSchema(), table.getSourceTableName());

        String fkName = null;
        String currentFK = null;
        String currentFKFullTableName = null;
        String currentPKFullTableName = null;
        ArrayList<String> columns = null;
        String prevPKTableName = "";

        try {
            while (fk.next()) {
                MigrateDatabaseUtils.printForeignKeyDetails(fk);
                fkName = fk.getString("FK_NAME");

                if(currentFK != null && !currentFK.equals(fkName)) {
                    String fkColumns = "";
                    String pkColumns = "";
                    java.util.Collections.sort(columns);
                    int numCols = columns.size();
                    for(int i=0;i<numCols;i++) {
                        String[] dtl = StringUtils.split(columns.get(i),",");
                        if(i>0) {
                            fkColumns += ",";
                            pkColumns += ",";
                        }
                        fkColumns += dtl[1];
                        pkColumns += dtl[2];
                    }
                    
                    foreignKey = new StringBuffer();
                    foreignKey.append("ALTER TABLE ");
                    foreignKey.append(currentFKFullTableName);
                    foreignKey.append(" ADD CONSTRAINT ");
                    foreignKey.append(currentFK);
                    foreignKey.append(" FOREIGN KEY (");
                    foreignKey.append(fkColumns);
                    foreignKey.append(") REFERENCES ");
                    foreignKey.append(currentPKFullTableName);
                    foreignKey.append("(");
                    foreignKey.append(pkColumns);
                    foreignKey.append(")");
                    
                    foreignKeyNames.add(currentFK);
                    output.ouputCreateForeignKey(table.getTargetSchema(), currentFK, foreignKey.toString());
                    
                    currentFK = null;
                }

                
                if(currentFK == null) {
                    currentFK = fk.getString("FK_NAME");
                    String schema = fk.getString("FKTABLE_SCHEM");
                    if(schema != null) {
                        currentFKFullTableName = schema + "." + fk.getString("FKTABLE_NAME");
                    } else {
                        currentFKFullTableName = fk.getString("FKTABLE_NAME");
                    }

                    schema = fk.getString("PKTABLE_SCHEM");
                    if(schema != null) {
                        currentPKFullTableName = schema + "." + fk.getString("PKTABLE_NAME");
                    } else {
                        currentPKFullTableName = fk.getString("PKTABLE_NAME");
                    }

                    columns = new ArrayList<String>();

                    columns.add(fk.getString("KEY_SEQ") + "," + fk.getString("FKCOLUMN_NAME") + "," + fk.getString("PKCOLUMN_NAME"));

                } else {
                    if (prevPKTableName.equals(fk.getString("PKTABLE_NAME"))) {
                        columns.add(fk.getString("KEY_SEQ") + "," + fk.getString("FKCOLUMN_NAME") + "," + fk.getString("PKCOLUMN_NAME"));
                    }
                }
                prevPKTableName = fk.getString("PKTABLE_NAME");
            }
        } finally {
            try {
                fk.close();
            } catch (Exception ignore) {
            }
        }
        
        if(currentFK != null) {
            String fkColumns = "";
            String pkColumns = "";
            java.util.Collections.sort(columns);
            int numCols = columns.size();
            for(int i=0;i<numCols;i++) {
                String[] dtl = StringUtils.split(columns.get(i),",");
                if(i>0) {
                    fkColumns += ",";
                    pkColumns += ",";
                }
                fkColumns += dtl[1];
                pkColumns += dtl[2];
            }
            
            foreignKey = new StringBuffer();
            foreignKey.append("ALTER TABLE ");
            foreignKey.append(currentFKFullTableName);
            foreignKey.append(" ADD CONSTRAINT ");
            foreignKey.append(currentFK);
            foreignKey.append(" FOREIGN KEY (");
            foreignKey.append(fkColumns);
            foreignKey.append(") REFERENCES ");
            foreignKey.append(currentPKFullTableName);
            foreignKey.append("(");
            foreignKey.append(pkColumns);
            foreignKey.append(")");
            foreignKeyNames.add(currentFK);
            output.ouputCreateForeignKey(table.getTargetSchema(), currentFK, foreignKey.toString());
        }
        
        return foreignKeyNames;
    }
    
    
    /**
     * Used to filter out system schemas that you don't want to process
     * 
     * @param schemaName
     * @return
     */
    public boolean isValidSchema(String schemaName) {
        return true;
    }

    /**
     * Indicates if the database supports schemas
     * 
     * @return
     */
    public boolean useSchema() {
        return useSchema;
    }

    public String getJdbcPrefix() {
        return jdbcPrefix;
    }

    public String getLabel() {
        return label;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getVarcharType(int type, String typeName,long precision, int scale) {
        LOG.debug("Varchar precision:" + precision);
        if( "text".equals(typeName.toLowerCase())) {
            if(precision > 2147483647) {
                return "TEXT (2147483647)";
            } else {
                return "TEXT (" + precision + ")";
            }
        } else if(precision < 1 ) {
            LOG.debug("It is less than 0");
            return "VARCHAR (30)";
        } else if (precision > 32672) {
            LOG.debug("It is grethan than 32672");
            return "VARCHAR (32672)";
        }
        return "VARCHAR (" + precision + ")";
    }

    public String getColumnType(int type, String typeName,long precision, int scale) {
        return "";
    }

    /**
     * Locates the database vendor class
     * 
     * @param prefix
     */
    public static BaseDatabaseVendor getDatabaseVendorClass(String prefix,
            String vendorFile) {
        BaseDatabaseVendor databaseVendorClass = null;
        if (prefix != null && prefix.length() > 0) {
            try {

                File xmlConfigFile = new File(vendorFile);
                if (!xmlConfigFile.exists()) {
                    LOG.info("Database vendor file not found:" + vendorFile);
                }

                XMLConfiguration config = new XMLConfiguration(xmlConfigFile);
                config.setExpressionEngine(new XPathExpressionEngine());
                SubnodeConfiguration vendorProps = config
                        .configurationAt("vendors/vendor[@id='" + prefix + "']");

                // migrationClass
                if (vendorProps != null) {
                    String sClassName = vendorProps
                            .getString("migrationClass",
                                    "com.splicemachine.cs.databasemigration.vendors.BaseDatabaseVendor");
                    try {
                        Class<BaseDatabaseVendor> c = (Class<BaseDatabaseVendor>) Class
                                .forName(sClassName);
                        Constructor<?> cons = c
                                .getConstructor(SubnodeConfiguration.class);
                        databaseVendorClass = (BaseDatabaseVendor) cons
                                .newInstance(vendorProps);
                    } catch (Exception e) {
                        MigrateDatabaseUtils
                                .logError(
                                        "Exception in the getDatabaseVendor method - problem instantiating database class name:"
                                                + sClassName, e);
                    }
                } else {
                    MigrateDatabaseUtils
                            .logError("Not able to find the database vendor for the prefix:"
                                    + prefix);
                }
            } catch (ConfigurationException e1) {
                MigrateDatabaseUtils
                        .logError(
                                "ConfigurationException when loading the /com/splicemachine/cs/databasemigration/vendors/databaseVendors.xml:",
                                e1);
                throw new RuntimeException(
                        "Configuration file /com/splicemachine/cs/databasemigration/vendors/databaseVendors.xml is missing");
            }
        }
        return databaseVendorClass;
    }
    
    public PrintWriter createFile(String directory, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
        
        File fDirectory = new File(directory);
        if(!fDirectory.exists()) {
            fDirectory.mkdirs();
        }
        
        return new PrintWriter(directory + fileName , "UTF-8");
    }

}
