package com.splicemachine.cs.databasemigration.vendors;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConstants;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.schema.Table;
import com.splicemachine.cs.databasemigration.output.BaseOutput;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.log4j.Logger;


import java.io.PrintWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Ibmdb2Database extends BaseDatabaseVendor {


    public static ArrayList<String> systemSchemas = new ArrayList<String>();

    static {
        systemSchemas.add("SYSIBM");
        systemSchemas.add("SYSTOOLS");
        systemSchemas.add("NULLID");
        systemSchemas.add("SQLJ");
        systemSchemas.add("SYSIBM");
        systemSchemas.add("SYSIBMADM");
    }

    public Ibmdb2Database(SubnodeConfiguration config) {
        super(config);
    }

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);

    /**
     * Used to filter out system schemas that you don't want to process
     *
     * @param schemaName
     * @return
     */
    public boolean isValidSchema(String schemaName) {
        return !systemSchemas.contains(schemaName);
    }


    public ResultSet getTables(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            if(schemaPattern == null) {
                rs = stmt
                        .executeQuery("select NULL AS TABLE_CAT, CREATOR AS TABLE_SCHEM, NAME AS TABLE_NAME from sysibm.systables where TYPE = 'T'");

            } else {
                rs = stmt
                        .executeQuery("select NULL AS TABLE_CAT, CREATOR AS TABLE_SCHEM, NAME AS TABLE_NAME from sysibm.systables where TYPE = 'T' and creator='" + schemaPattern.toUpperCase() + "'");
            }
        } catch (Exception e) {
            LOG.error("Exception getting catalogs", e);
        }
        return rs;
    }

    public ResultSet getViews(Connection con, DatabaseMetaData md, String catalog, String schemaPattern) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            rs = stmt
                    .executeQuery("select NULL, CREATOR, NAME from sysibm.systables where TYPE = 'V' and creator='" + schemaPattern.toUpperCase() + "'");

        } catch (Exception e) {
            LOG.error("Exception getting catalogs", e);
        }
        return rs;
    }

    public HashMap<String, String> getSequenceDefinition(Connection con,
                                                         String schema, String seqName) {
        Statement stmt = null;
        ResultSet rs = null;
        HashMap<String, String> details = null;

        MigrateDatabaseUtils.writeMessage("Ibm CREATE SEQUENCE :" + seqName);

        try {
            stmt = con.createStatement();
            rs = stmt
                    .executeQuery("select SCHEMANAME, SEQUENCENAME,SEQUENCEDATATYPE,CURRENTVALUE,STARTVALUE,MINIMUMVALUE,MAXIMUMVALUE,INCREMENT,CYCLEOPTION from SYS.SYSSCHEMAS s, SYS.SYSSEQUENCES q where s.SCHEMAID = q.SCHEMAID and s.SCHEMANAME = '" + schema + "' and q.SEQUENCENAME = '" + seqName + "'");
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
     * Exports a procedure to a file
     */
    public void exportCheckConstraints(Connection con,
                                       ArrayList<String> schemaList, BaseOutput output) {
        Statement stmt = null;
        ResultSet rs = null;
        StringBuffer constraint = new StringBuffer();

        try {
            LOG.debug("Exporting Check Constraints for schemas");

            stmt = con.createStatement();
            for (String schema : schemaList) {
                rs = stmt
                        .executeQuery("select TABNAME, CONSTNAME, TEXT from SYSCAT.CHECKS where TABSCHEMA = '" + schema + "'");
                while (rs.next()) {
                    constraint = new StringBuffer();
                    constraint.append("ALTER TABLE ");
                    constraint.append(schema);
                    constraint.append(".");
                    constraint.append(rs.getString(1));
                    constraint.append(" ADD CONSTRAINT ");
                    constraint.append(rs.getString(2));
                    constraint.append(" CHECK (");
                    constraint.append(rs.getString(3));
                    constraint.append(")");
                    output.outputCreateConstraint(schema, rs.getString(2), constraint.toString());
                }
            }

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

    /**
     * Exports users
     */
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        ArrayList<String> userids = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {

            String USER_LIST = "select GRANTEE from SYSCAT.DBAUTH";

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

            String ROLE_LIST = "select ROLENAME from SYSCAT.ROLES";

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

    public HashMap<String,String> getTableColumnDefault(Connection conn, Table table) {
        HashMap<String,String> tableColumnDefaults = new HashMap<String,String>();

        ResultSet def = null;
        Statement stmt = null;
        String sqlText = "select c.NAME, c.\"DEFAULT\" from SYSIBM.SYSCOLUMNS c where c.TBCREATOR = '" + table.getSourceSchema()  + "' and c.TBNAME = '" + table.getSourceTableName() + "' and c.\"DEFAULT\" IS NOT NULL";
        try {
            stmt = conn.createStatement();
            
            def = stmt.executeQuery(sqlText);
            while(def.next()) {
                tableColumnDefaults.put(def.getString(1), def.getString(2));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
        	LOG.error("SQL:["+sqlText+"]", e);
            
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
