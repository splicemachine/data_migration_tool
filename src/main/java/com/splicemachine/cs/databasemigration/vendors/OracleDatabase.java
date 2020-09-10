package com.splicemachine.cs.databasemigration.vendors;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConstants;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.output.BaseOutput;
import com.splicemachine.cs.databasemigration.schema.Table;

public class OracleDatabase extends BaseDatabaseVendor {
    
    public static ArrayList<String> systemSchemas = new ArrayList<String>();
    
    static {
        systemSchemas.add("APEX_030200");
        systemSchemas.add("APPQOSSYS");
        systemSchemas.add("ANONYMOUS");
        systemSchemas.add("APEX_040200");
        systemSchemas.add("APEX_PUBLIC_USER");
        systemSchemas.add("AUDSYS");
        systemSchemas.add("BI");
        systemSchemas.add("BTEVES");
        systemSchemas.add("DIP");
        systemSchemas.add("ITSENTE");
        systemSchemas.add("CTXSYS");
        systemSchemas.add("DBSNMP");
        systemSchemas.add("EXFSYS");
        systemSchemas.add("FLOWS_FILES");
        systemSchemas.add("HR");
        systemSchemas.add("IX");
        systemSchemas.add("MDSYS");
        systemSchemas.add("MDDATA");
        systemSchemas.add("OE");
        systemSchemas.add("OLAPSYS");
        systemSchemas.add("ORACLE_OCM");
        systemSchemas.add("ORDPLUGINS");
        systemSchemas.add("OWBSYS_AUDIT");
        systemSchemas.add("SI_INFORMTN_SCHEMA");
        systemSchemas.add("SQLTXPLAIN");
        systemSchemas.add("SYS");
        systemSchemas.add("SYSMAN");
        systemSchemas.add("SYSTEM");
        systemSchemas.add("WKSYS");
        systemSchemas.add("WK_TEST");
        systemSchemas.add("WMSYS");
        systemSchemas.add("XDB");
        systemSchemas.add("WKPROXY");
        systemSchemas.add("XS$NULL");
        systemSchemas.add("SPATIAL_CSW_ADMIN_USR");
        systemSchemas.add("SPATIAL_WFS_ADMIN_USR");
        systemSchemas.add("SQLTXADMIN");
        systemSchemas.add("ORDDATA");
        systemSchemas.add("ORDSYS");
        systemSchemas.add("OUTLN");
        systemSchemas.add("OWBSYS");
        systemSchemas.add("PM");
        systemSchemas.add("SCOTT");
        systemSchemas.add("SH");
        systemSchemas.add("SYSBACKUP");
        systemSchemas.add("SYSDG");
        systemSchemas.add("SYSKM");
    }

    public OracleDatabase(SubnodeConfiguration config) {
        super(config);
    }

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);
    
    public String getColumnType(int type, String typeName,long precision, int scale) {
        if(typeName != null && "LONG".equals(typeName)) {
            return "CLOB";
        }
        return "";
    }
    
    /**
     * Used to filter out system schemas that you don't want to process
     * 
     * @param schemaName
     * @return
     */
    public boolean isValidSchema(String schemaName) {
        return !systemSchemas.contains(schemaName);
    }

    public HashMap<String, String> getSequenceDefinition(Connection con,
            String schema, String seqName) {
        Statement stmt = null;
        ResultSet rs = null;
        HashMap<String, String> details = null;

        MigrateDatabaseUtils.writeMessage("Oracle CREATE SEQUENCE :" + seqName);

        try {
            stmt = con.createStatement();
            rs = stmt
                    .executeQuery("select * from SYS.dba_sequences where SEQUENCE_NAME='"
                            + seqName
                            + "' AND SEQUENCE_OWNER = '"
                            + schema
                            + "'");
            if (rs.next()) {
                details = new HashMap<String, String>();
                details.put("MinValue", rs.getObject("MIN_VALUE").toString());
                details.put("IncrementBy", rs.getObject("MIN_VALUE").toString());
                details.put("Cycle", rs.getObject("CYCLE_FLAG").toString());
                details.put("Last", rs.getObject("LAST_NUMBER").toString());
            }
        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception getting sequence", e);
        } finally {
            if (rs != null)
                try {
                    rs.close();
                } catch (Exception e) {
                }
            if (stmt != null)
                try {
                    stmt.close();
                } catch (Exception e) {
                }
        }
        return details;
    }



    /**
     * Exports a package body to a file
     */
    public void exportPackage(Connection con, String schema,
            String packageName, String outputPath) {
        
        exportGeneric(con, "PACKAGE BODY", schema, packageName, outputPath); 
    }

    /**
     * Exports a procedure to a file
     */
    public void exportProcedure(Connection con, String schema,
            String procedure, String outputPath) {
        exportGeneric(con, "PROCEDURE", schema, procedure, outputPath); 
    }

    /**
     * Exports a function to a file
     */
    public void exportFunction(Connection con, String schema, String function,
            String outputPath) {
        exportGeneric(con, "FUNCTION", schema, function, outputPath); 
    }

    /**
     * Exports a view to a file
     * TODO: This needs to be tested
     */
    public void exportView(Connection con, String schema, String view,
            String outputPath) {
        exportGeneric(con, "VIEW", schema, view, outputPath); 
    }
    
    /**
     * Generic function for exporting SQL Server objects such as Functions, Views,
     *  Triggers and procedures.
     * @param con
     * @param type
     * @param schema
     * @param objectName
     * @param outputPath
     */
    public void exportGeneric(Connection con, String type, String schema, String objectName,
            String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        PrintWriter file  = null;
        try {

            LOG.debug("Exporting " + type + " for schema=[" + schema
                    + "] and objectName=[" + objectName + "]");
            
            file = createFile(outputPath , "/create_"
                    + schema + "_" + type + "_" + objectName + ".sql");

            
            StringBuilder sb = new StringBuilder();
            sb.append("select TEXT from all_source where OWNER = '");
            sb.append(schema);
            sb.append("' and NAME = '");
            sb.append(objectName);
            sb.append("' AND TYPE='"); 
            sb.append(type);
            sb.append("' ORDER BY LINE");
            
            stmt = con.createStatement();
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                file.write(rs.getString(1));
            }
            

        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving " + type + 
                    ":" 
                    + objectName, e);
        } finally {
            if(file != null) {
                file.flush();
                file.close();
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }
            ;
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }
        }
    }
    
    

    /**
     * Exports a procedure to a file
     */
    public void exportCheckConstraints(Connection con,
            ArrayList<String> schemaList, String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            LOG.debug("Exporting Check Constraints for schemas=");

            PrintWriter packageFile = new PrintWriter(outputPath + "/create_"
                    + "check_constraints.sql", "UTF-8");
            stmt = con.createStatement();
            for (String schema : schemaList) {
                rs = stmt
                        .executeQuery("select TABLE_NAME, CONSTRAINT_NAME, SEARCH_CONDITION from ALL_CONSTRAINTS where OWNER = '"
                                + schema
                                + "' AND STATUS='ENABLED' AND CONSTRAINT_TYPE='C' and CONSTRAINT_NAME NOT LIKE 'SYS_%' ORDER BY TABLE_NAME, CONSTRAINT_NAME");
                while (rs.next()) {

                    packageFile.write("ALTER TABLE " + schema + "."
                            + rs.getString(1) + " ADD CONSTRAINT "
                            + rs.getString(2) + " CHECK (" + rs.getString(3)
                            + ") ");
                    packageFile.write(MigrateDatabaseConstants.NEW_LINE);
                }
            }
            packageFile.close();

        } catch (Exception e) {
            MigrateDatabaseUtils.logError(
                    "Exception retrieving check constraints:", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }
        }
    }
    
    public ArrayList<String> exportForeignKeys(Connection conn, DatabaseMetaData meta, Table table, BaseOutput output) throws Exception {
        ArrayList<String> foreignKeyNames = new ArrayList<String>();
        StringBuffer foreignKey = new StringBuffer();
        
        String oracleForeignKey = "select b.owner FKTABLE_SCHEM, b.table_name FKTABLE_NAME, b.column_name FKCOLUMN_NAME,b.position KEY_SEQ,c.owner PKTABLE_SCHEM, c.table_name PKTABLE_NAME, c.column_name PKCOLUMN_NAME,"
                + "a.constraint_name FK_NAME,a.delete_rule "
                + "from all_cons_columns b,all_cons_columns c,all_constraints a "
                + "where b.constraint_name = a.constraint_name and a.owner = b.owner "
                + "and b.position = c.position and c.constraint_name = a.r_constraint_name and c.owner = a.r_owner and a.constraint_type = 'R' "
                + "and b.table_name = '" + table.getSourceTableName() + "'"
                + "and c.owner = '" + table.getSourceSchema() + "'";

        
        String fkName = null;
        String currentFK = null;
        String currentFKFullTableName = null;
        String currentPKFullTableName = null;
        ArrayList<String> columns = null;
        ResultSet fk = null;

        try {
            fk = conn.createStatement().executeQuery(oracleForeignKey);
            while(fk.next()) {
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
                    foreignKey.append(");");
                    
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
                    columns.add(fk.getString("KEY_SEQ") + "," + fk.getString("FKCOLUMN_NAME") + "," + fk.getString("PKCOLUMN_NAME"));
                }
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
            foreignKey.append(");");
            foreignKeyNames.add(currentFK);
            output.ouputCreateForeignKey(table.getTargetSchema(), currentFK, foreignKey.toString());
        }
        
        return foreignKeyNames;
    }
    

    /**
     * Exports users
     */
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        ArrayList<String> userids = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            

            String USER_LIST = "select username from dba_users u where username not in"
                    + "('APEX_030200','APPQOSSYS','ANONYMOUS','APEX_PUBLIC_USER','BI','BTEVES','DIP','ITSENTE','CTXSYS','DBSNMP',"
                    + "'EXFSYS','FLOWS_FILES','HR','IX','MDSYS','MDDATA','OE','OLAPSYS','ORACLE_OCM','ORDPLUGINS','OWBSYS_AUDIT',"
                    + "'SI_INFORMTN_SCHEMA','SPATIAL_CSW_ADMIN_USR','SPATIAL_WFS_ADMIN_USR','SQLTXADMIN','ORDDATA',"
                    + "'ORDSYS','OUTLN','OWBSYS','PM','SCOTT','SH',"
                    + "'WKPROXY','XS$NULL',"
                    + "'SQLTXPLAIN','SYS','SYSMAN','SYSTEM','WKSYS','WK_TEST','WMSYS','XDB') "
                    + "and u.account_status = 'OPEN' order by username";

            LOG.debug("Exporting Users:" + USER_LIST);
            
            stmt = con.createStatement();
            rs = stmt.executeQuery(USER_LIST);
            while (rs.next()) {
                userids.add(rs.getString(1));
            }

        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving users:", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }
        }
        return userids;
    }

    /**
     * Exports roles
     */
    public void exportRoles(Connection con, String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            LOG.debug("Exporting Roles");

            String ROLE_LIST = "select role from dba_roles u where role not in"
                    + "('ADM_PARALLEL_EXECUTE_TASK','APEX_ADMINISTRATOR_ROLE','AQ_ADMINISTRATOR_ROLE','AQ_USER_ROLE','AUTHENTICATEDUSER',"
                    + "'CONNECT','CSW_USR_ROLE','CTXAPP','CWM_USER','DATAPUMP_EXP_FULL_DATABASE','DATAPUMP_IMP_FULL_DATABASE','DBA',"
                    + "'DBFS_ROLE','DELETE_CATALOG_ROLE','EJBCLIENT','EXECUTE_CATALOG_ROLE','EXP_FULL_DATABASE','GATHER_SYSTEM_STATISTICS',"
                    + "'GLOBAL_AQ_USER_ROLE','HS_ADMIN_EXECUTE_ROLE','HS_ADMIN_ROLE','HS_ADMIN_SELECT_ROLE','IMP_FULL_DATABASE',"
                    + "'JAVADEBUGPRIV','JAVAIDPRIV','JAVASYSPRIV','JAVAUSERPRIV','JAVA_ADMIN','JAVA_DEPLOY','JMXSERVER','LOGSTDBY_ADMINISTRATOR',"
                    + "'MGMT_USER','OEM_ADVISOR','OEM_MONITOR','OLAPI_TRACE_USER','OLAP_DBA','OLAP_USER','OLAP_XS_ADMIN','ORDADMIN','OWB$CLIENT',"
                    + "'OWB_DESIGNCENTER_VIEW','OWB_USER','RECOVERY_CATALOG_OWNER','RESOURCE','SCHEDULER_ADMIN','SELECT_CATALOG_ROLE','SPATIAL_CSW_ADMIN',"
                    + "'SPATIAL_WFS_ADMIN','SQLT_USER_ROLE','WFS_USR_ROLE','WKUSER','WM_ADMIN_ROLE',"
                    + "'XDBADMIN','XDB_SET_INVOKER','XDB_WEBSERVICES','XDB_WEBSERVICES_OVER_HTTP','XDB_WEBSERVICES_WITH_PUBLIC') order by role";

            PrintWriter packageFile = new PrintWriter(outputPath + "create_"
                    + "roles.sql", "UTF-8");
            stmt = con.createStatement();
            rs = stmt.executeQuery(ROLE_LIST);
            while (rs.next()) {
                packageFile.write("CREATE ROLE " + rs.getString(1) + ";");
                packageFile.write(MigrateDatabaseConstants.NEW_LINE);
            }

            packageFile.close();

        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving roles:", e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                }
            }
        }
    }
    
    public boolean hasCustomMapping() {
        return true;
    }
    
    public String getDateDataType(Connection conn,String schema, String tableName, String columnName,
            int type, String typeName, int precision, int scale) {
        
        String returnType = "TIMESTAMP";
        //We want to see if the date contains a time or not
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            long numRows = 0;
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select sum(1) from " + schema + "." + tableName + " WHERE TRUNC (" + columnName + ") != " + columnName);
            if (rs.next()) {
                numRows = rs.getLong(1);
                if(numRows > 0) {
                    System.out.println("************* TIMESTAMP IS TIMESTAMP COLUMN:" + schema + "." + tableName + " columnName:" + columnName);
                }
            }
            if(numRows == 0) {
                System.out.println("************* TIMESTAMP IS DATE COLUMN:" + schema + "." + tableName + " columnName:" + columnName);
                returnType = "DATE";
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            if(stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }

        
        return returnType;
    }
    
    public String getNumericDataType(Connection conn,String schema, String tableName, String columnName,
            int type, String typeName, int precision, int scale) {
        
        String returnType = "DOUBLE";
        //We want to see if the date contains a time or not
        Statement stmt = null;
        ResultSet rs = null;
        
        try {
            long wholeNumberLength = -1;
            long decimalNumberLength = -1;

            stmt = conn.createStatement();
            rs = stmt.executeQuery("select length(max(abs(trunc(" + columnName + ",0))))maxLeftOfDecimal, max(length(mod(" + columnName + ", 1))) -1 maxRightOfDecimal from " + schema + "." + tableName);
            if (rs.next()) {
                wholeNumberLength = rs.getLong(1);
                decimalNumberLength = rs.getLong(2);
            }
            
            if(wholeNumberLength == 0 && decimalNumberLength == 0) {
                returnType = "BIGINT";
            } else {
                 if (decimalNumberLength + wholeNumberLength > 31) {
                    returnType = "DOUBLE";
                } else if(decimalNumberLength == 0) {
                    if(wholeNumberLength < 5) {
                        //It should be a small init
                        returnType = "SMALLINT";
                    } else if (wholeNumberLength < 10) {
                        //It should be an integer
                        returnType = "INTEGER";
                    } else {
                        returnType = "BIGINT";
                    }
                } else {
                    return "NUMERIC(" + (wholeNumberLength + decimalNumberLength) + "," + decimalNumberLength + ")";
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(rs != null)
                try {
                    rs.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            if(stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }

        System.out.println("************* NUMBER COLUMN:" + schema + "." + tableName + " columnName:" + columnName + " returnType:" + returnType);
        
        return returnType;
    }
    
    public HashMap<String,String> getTableColumnDefault(Connection conn, Table table) {
        HashMap<String,String> tableColumnDefaults = new HashMap<String,String>();
        
        ResultSet def = null;
        Statement stmt = null;
        try {
            stmt = conn.createStatement(); 
            def = stmt.executeQuery("select COLUMN_NAME, DATA_DEFAULT from ALL_TAB_COLUMNS where OWNER = '" + table.getSourceSchema() + "' and DATA_DEFAULT IS NOT NULL and TABLE_NAME= '" + table.getSourceTableName() + "'");
            while(def.next()) {
                tableColumnDefaults.put(def.getString(1), def.getString(2));
            }
            } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            if(def != null)
                try {
                    def.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            if(stmt != null)
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
        }
        
        return tableColumnDefaults;
    }
    
    public String getConvertedColumnType(Connection conn,String tableName, String columnName,
            int type, String typeName, int precision, int scale, String nullable) {
        return null;
    }
}
