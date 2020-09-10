package com.splicemachine.cs.databasemigration.vendors;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.log4j.Logger;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConstants;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.schema.Table;

public class SQLServerDatabase extends BaseDatabaseVendor {
    
    public static ArrayList<String> systemSchemas = new ArrayList<String>();
    
    static {
        systemSchemas.add("db_accessadmin");
        systemSchemas.add("db_backupoperator");
        systemSchemas.add("db_datareader");
        systemSchemas.add("db_datawriter");
        systemSchemas.add("db_ddladmin");
        systemSchemas.add("db_denydatareader");
        systemSchemas.add("db_denydatawriter");
        systemSchemas.add("db_owner");
        systemSchemas.add("db_securityadmin");
        systemSchemas.add("guest");
        systemSchemas.add("INFORMATION_SCHEMA");
        systemSchemas.add("sys");
        
    }

    public SQLServerDatabase(SubnodeConfiguration config) {
        super(config);
    }

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);
    
    public String getColumnType(int type, String typeName,long precision, int scale) {
        if(typeName != null) {
            if("xml".equalsIgnoreCase(typeName)) {
                return "CLOB";
            } else if ("nchar".equalsIgnoreCase(typeName)) {
                return "CHAR(" + precision + ")";
            }
        }
        return "";
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
                        .executeQuery("select TABLE_NAME, COLUMN_NAME, CHECK_CLAUSE, c.CONSTRAINT_SCHEMA, cc.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME where cc.CONSTRAINT_SCHEMA = '" + schema + "' ORDER BY CONSTRAINT_SCHEMA, TABLE_NAME, COLUMN_NAME");
                 while (rs.next()) {

                    packageFile.write("ALTER TABLE " + schema + "."
                            + rs.getString(1) + " ADD CONSTRAINT "
                            + rs.getString(5) + " CHECK (" + rs.getString(3)
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
    
    public boolean isValidSchema(String schemaName) {
        return !systemSchemas.contains(schemaName);
    }
    
    /**
     * Exports users
     */
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        ArrayList<String> userids = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            

            String USER_LIST = "select distinct suser_sname(owner_sid) as 'Owner' from sys.databases";

            LOG.debug("Exporting Users:" + USER_LIST);
            
            stmt = con.createStatement();
            rs = stmt.executeQuery(USER_LIST);
            while (rs.next()) {
                String userId = rs.getString(1);
                //Its a windows user id
                if(userId.indexOf("\\") > -1) continue;
                userids.add(userId);
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
    
    public HashMap<String,String> getTableColumnDefault(Connection conn, Table table) {
        HashMap<String,String> tableColumnDefaults = new HashMap<String,String>();
        
        ResultSet def = null;
        Statement stmt = null;
        try {
            
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ST.[name] AS TableName, SC.[name] AS ColumnName, SD.definition AS DefaultValue ");
            sb.append("FROM sys.tables ST INNER JOIN sys.syscolumns SC ON ST.[object_id] = SC.[id]  ");
            sb.append("INNER JOIN sys.default_constraints SD ON ST.[object_id] = SD.[parent_object_id] AND SC.colid = SD.parent_column_id ");
            sb.append("INNER JOIN sys.schemas SH on SH.SCHEMA_ID = SD.schema_id  ");
            sb.append("where SH.NAME = '");
            sb.append(table.getSourceSchema());
            sb.append("' AND ST.NAME = '");
            sb.append(table.getSourceTableName());
            sb.append("' ");
            sb.append("ORDER BY ST.[name], SC.colid ");
            
            stmt = conn.createStatement(); 
            def = stmt.executeQuery(sb.toString());
            while(def.next()) {
                tableColumnDefaults.put(def.getString(2), def.getString(3));
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
    
    public String[] getListOfTriggers(Connection conn) {
        
        String [] views = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT sysobjects.name AS trigger_name,s.name AS table_schema,OBJECT_NAME(parent_obj) AS table_name ");
            sb.append("FROM sysobjects ");
            sb.append("INNER JOIN sys.tables t ON sysobjects.parent_obj = t.object_id ");
            sb.append("INNER JOIN sys.schemas s ON t.schema_id = s.schema_id ");
            sb.append(" WHERE sysobjects.type = 'TR' ");          
            
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                list.add(rs.getString(2) + "." + rs.getString(1));
            }
            views = new String[list.size()];
            views = list.toArray(views);
        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving list of triggers", e);
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
        return views;

    }
    
    public String[] getListOfViews(Connection conn) {
        String [] views = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT SCHEMA_NAME(schema_id) AS schema_name, ");
            sb.append("name AS view_name ");
            sb.append("FROM sys.views ");
            
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                list.add(rs.getString(1) + "." + rs.getString(2));
            }
            views = new String[list.size()];
            views = list.toArray(views);
        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving list of views", e);
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
        return views;
    }
    
    public String[] getListOfProcedures(Connection conn) {
        String [] procedures = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            
            StringBuilder sb = new StringBuilder();
            sb.append("select ROUTINE_SCHEMA, ROUTINE_NAME ");
            sb.append("from information_schema.routines ");
            sb.append("where routine_type = 'PROCEDURE' ");
            
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                list.add(rs.getString(1) + "." + rs.getString(2));
            }
            procedures = new String[list.size()];
            procedures = list.toArray(procedures);
        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving list of procedures", e);
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
        return procedures;
    }

    public String[] getListOfFunctions(Connection conn) {
        String [] functions = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            
            StringBuilder sb = new StringBuilder();
            sb.append("select ROUTINE_SCHEMA, ROUTINE_NAME ");
            sb.append("from information_schema.routines ");
            sb.append("where routine_type = 'FUNCTION' ");
            
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                list.add(rs.getString(1) + "." + rs.getString(2));
            }
            functions = new String[list.size()];
            functions = list.toArray(functions);
        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving list of functions", e);
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
        return functions;
    }

    /**
     * Writes a trigger definition to a file
     */
    public void exportTrigger(Connection con, String schema, String objectName,
            String outputPath) {
        exportGeneric(con, "Trigger", schema, objectName, outputPath); 
    }
    
    /**
     * Writes a procedure definition to a file
     */
    public void exportProcedure(Connection con, String schema, String objectName,
            String outputPath) {
        exportGeneric(con, "Procedure", schema, objectName, outputPath); 
    }
    
    /**
     * Writes a function definition to a file
     */
    public void exportFunction(Connection con, String schema, String objectName,
            String outputPath) {
        exportGeneric(con, "Function", schema, objectName, outputPath); 
    }
    
    /**
     * Writes a view definition to a file
     */
    public void exportView(Connection con, String schema, String objectName,
            String outputPath) {
         exportGeneric(con, "View", schema, objectName, outputPath); 
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
            sb.append("select definition ");
            sb.append("from sys.sql_modules  ");
            sb.append("where object_name(object_id) like '"); 
            sb.append(objectName);
            sb.append("'");
            
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
}
