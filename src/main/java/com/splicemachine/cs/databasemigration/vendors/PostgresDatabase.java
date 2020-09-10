package com.splicemachine.cs.databasemigration.vendors;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConstants;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.schema.Table;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class PostgresDatabase extends BaseDatabaseVendor {

    public static ArrayList<String> systemSchemas = new ArrayList<String>();

    static {
        systemSchemas.add("pg_catalog");
        systemSchemas.add("information_schema");
    }

    public PostgresDatabase(SubnodeConfiguration config) {
        super(config);
    }

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);


    //TODO
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
    
    public boolean isValidSchema(String schemaName) {
        return !systemSchemas.contains(schemaName);
    }

    public HashMap<String, String> getSequenceDefinition(Connection con,
                                                         String schema, String seqName) {
        Statement stmt = null;
        ResultSet rs = null;
        HashMap<String, String> details = null;

        MigrateDatabaseUtils.writeMessage("Postgres CREATE SEQUENCE :" + seqName);

        try {
            stmt = con.createStatement();
            rs = stmt
                    .executeQuery("select * from INFORMATION_SCHEMA.sequences where SEQUENCE_NAME='"
                            + seqName
                            + "' AND SEQUENCE_SCHEMA = '"
                            + schema
                            + "'");
            if (rs.next()) {
                details = new HashMap<String, String>();
                details.put("MinValue", rs.getObject("minimum_value").toString());
                details.put("IncrementBy", rs.getObject("increment").toString());
                details.put("Cycle", rs.getObject("cycle_option").toString());
            }

            try {
                rs = stmt.executeQuery("select last_value from " + seqName);
                if (rs.next()) {
                    details.put("Last", rs.getObject("last_value").toString());
                }
            } catch (Exception e) {
                //Sequence has not been used yet so set it to the same as the min value
                details.put("Last", details.get("MinValue"));
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

    public String[] getListOfTriggers(Connection conn) {

        String [] views = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            StringBuilder sb = new StringBuilder();
            sb.append("SELECT n.nspname, t.tgname FROM pg_trigger t, pg_proc p INNER JOIN pg_catalog.pg_namespace n ON (p.pronamespace = n.oid)\n" +
                    "WHERE t.tgfoid=p.oid and t.tgisinternal = false");
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                list.add(rs.getString(1) + "." + rs.getString(2));
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
    
    /**
     * Exports users
     */
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        ArrayList<String> userids = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            

            String USER_LIST = "SELECT u.usename AS \"User name\" FROM pg_catalog.pg_user u";

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
            LOG.debug("Getting defaults for:" + table.getSourceSchema() + "." + table.getSourceTableName());
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT a.attname, d.adsrc AS default_value\n" +
                    "FROM   pg_catalog.pg_attribute a\n" +
                    "LEFT   JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum)\n" +
                    "                                     = (d.adrelid,  d.adnum)\n" +
                    "WHERE  NOT a.attisdropped   -- no dropped (dead) columns\n" +
                    "AND    a.attnum > 0         -- no system columns\n" +
                    "AND    a.attname IS NOT NULL \n" +
                    "AND    a.attrelid = '" + table.getSourceSchema() + "." + table.getSourceTableName() + "'::regclass\n");
            stmt = conn.createStatement();
            try {
                def = stmt.executeQuery(sb.toString());
                while(def.next()) {
                    String columnName = def.getString(1);
                    String columnDefault = def.getString(2);
                    LOG.debug("DEFAULTS COLUMN_NAME:" + columnName + " DEFAULT_VALUE:" + columnDefault);
                    if(columnDefault == null) {
                        continue;
                    }

                    int defaultSep = columnDefault.indexOf("::");
                    if(defaultSep > -1) {
                        columnDefault = columnDefault.substring(0,defaultSep);
                    }
                    tableColumnDefaults.put(columnName, columnDefault);
                }
            } catch (Exception e) {
                //e.printStackTrace();
            }
        } catch (SQLException e) {
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

    
    public String[] getListOfViews(Connection conn) {
        String [] views = null;
        ArrayList<String> list = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();

            StringBuilder sb = new StringBuilder();
            sb.append("select table_schema AS schema_name, ");
            sb.append("table_name AS view_name ");
            sb.append("from INFORMATION_SCHEMA.views WHERE table_schema not in ('pg_catalog','information_schema') ");
            
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
            sb.append("where routine_type = 'PROCEDURE' and ROUTINE_SCHEMA not in ('pg_catalog','information_schema')");
            
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
            sb.append("where routine_type = 'FUNCTION' and ROUTINE_SCHEMA not in ('pg_catalog','information_schema')");
            
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
     * Writes a view definition to a file
     */
    public void exportView(Connection con, String schema, String objectName,
            String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        PrintWriter file  = null;
        try {

            LOG.debug("Exporting view for schema=[" + schema
                    + "] and objectName=[" + objectName + "]");

            file = createFile(outputPath , "/create_"
                    + schema + "_view_" + objectName + ".sql");

            StringBuilder sb = new StringBuilder();
            sb.append("select definition ");
            sb.append("from pg_views  ");
            sb.append("where viewname = '" + objectName + "' and schemaname = '" );
            sb.append(schema);
            sb.append("'");

            stmt = con.createStatement();

            file.write("CREATE VIEW " + schema + "." + objectName + " AS ");
            rs = stmt
                    .executeQuery(sb.toString());

            while (rs.next()) {
                file.write(rs.getString(1));
            }


        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving view " +
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
     * Writes a trigger definition to a file
     */
    public void exportTrigger(Connection con, String schema, String objectName,
                              String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        PrintWriter file  = null;
        try {

            LOG.debug("Exporting TRIGGER for schema=[" + schema
                    + "] and objectName=[" + objectName + "]");

            file = createFile(outputPath , "/create_"
                    + schema + "_TRIGGER_" + objectName + ".sql");

            StringBuilder sb = new StringBuilder();
            sb.append("SELECT pg_get_triggerdef(trg.oid) FROM pg_trigger trg JOIN pg_proc proc ON proc.oid = trg.tgfoid JOIN pg_catalog.pg_namespace n ON n.oid = proc.pronamespace where n.nspname = '");
            sb.append(schema);
            sb.append("' and tgname = '");
            sb.append(objectName);
            sb.append("'");


            stmt = con.createStatement();
            rs = stmt
                    .executeQuery(sb.toString());
            while (rs.next()) {
                file.write(rs.getString(1));
            }


        } catch (Exception e) {
            MigrateDatabaseUtils.logError("Exception retrieving TRIGGER" +
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
     * Exports roles
     */
    public void exportRoles(Connection con, String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            LOG.debug("Exporting Roles");

            String ROLE_LIST = "select rolname from pg_roles";

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
            sb.append("SELECT pg_get_functiondef(f.oid) FROM pg_catalog.pg_proc f  INNER JOIN pg_catalog.pg_namespace n ON (f.pronamespace = n.oid) where n.nspname = '");
            sb.append(schema);
            sb.append("' and f.proname = '");
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

    /**
     * Exports a procedure to a file
     */
    public void exportCheckConstraints(Connection con,
                                       ArrayList<String> schemaList, String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            PrintWriter packageFile = new PrintWriter(outputPath + "/create_"
                    + "check_constraints.sql", "UTF-8");
            stmt = con.createStatement();
            for (String schema : schemaList) {
                LOG.debug("Exporting Check Constraints for schemas:" + schema);
                rs = stmt
                        .executeQuery("select TABLE_NAME, COLUMN_NAME, CHECK_CLAUSE, c.CONSTRAINT_SCHEMA, cc.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME where cc.CONSTRAINT_SCHEMA = '" + schema + "' ORDER BY CONSTRAINT_SCHEMA, TABLE_NAME, COLUMN_NAME");
                while (rs.next()) {

                    String tableName = rs.getString(1);
                    String constraintName =  rs.getString(5);
                    String constraintCondition = rs.getString(3);
                    constraintCondition = constraintCondition.replace("::date","");
                    constraintCondition = constraintCondition.replace("::numeric","");
                    constraintCondition = constraintCondition.replace("::string","");

                    packageFile.write("ALTER TABLE " + schema + "."
                            + tableName + " ADD CONSTRAINT "
                            + constraintName + " CHECK (" + constraintCondition
                            + ") ");
                    packageFile.write(MigrateDatabaseConstants.NEW_LINE);
                }
            }
            packageFile.close();

        } catch (Exception e) {
            e.printStackTrace();
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

    /**
     * Postgresql does not have packages
     * @param conn
     * @return
     */
    public String[] getListOfPackage(Connection conn) {
        return null;
    }

    /**
     * Postgres does not have packages - nothing to do
     */
    public void exportPackage(Connection con, String schema,
                              String packageName, String outputPath) {

        return;
    }
}
