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

public class SpliceDatabase extends BaseDatabaseVendor {

    public SpliceDatabase(SubnodeConfiguration config) {
        super(config);
    }

    private static final Logger LOG = Logger
            .getLogger(BaseDatabaseVendor.class);
    
    public HashMap<String, String> getSequenceDefinition(Connection con,
            String schema, String seqName) {
        Statement stmt = null;
        ResultSet rs = null;
        HashMap<String, String> details = null;

        MigrateDatabaseUtils.writeMessage("Splice Machine CREATE SEQUENCE :" + seqName);

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
            ArrayList<String> schemaList, String outputPath) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            LOG.debug("Exporting Check Constraints for schemas=");
            
            PrintWriter packageFile = new PrintWriter(outputPath + "create_"
                    + "check_constraints.sql", "UTF-8");
            stmt = con.createStatement();
            for (String schema : schemaList) {
                rs = stmt
                        .executeQuery("select t.TABLENAME, c.CONSTRAINTNAME, d.CHECKDEFINITION from SYS.SYSSCHEMAS s, SYS.SYSTABLES t, SYS.SYSCONSTRAINTS c, SYS.SYSCHECKS d where s.SCHEMAID = t.SCHEMAID and t.TABLEID = c.TABLEID and d.CONSTRAINTID = c.CONSTRAINTID and s.SCHEMANAME = '" + schema + "'");
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

    /**
     * Exports users
     */
    public ArrayList<String> getUsers(Connection con, String outputPath) {
        ArrayList<String> userids = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            
            String USER_LIST = "select USERNAME from SYS.SYSUSERS";

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

            String ROLE_LIST = "select ROLEID from SYS.SYSROLES";

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
        try {
            stmt = conn.createStatement(); 
            def = stmt.executeQuery("select COLUMNNAME, COLUMNDEFAULT from SYS.SYSCOLUMNS c, SYS.SYSTABLES t, SYS.SYSSCHEMAS s where s.SCHEMANAME = '" + table.getSourceSchema()  + "' and s.SCHEMAID = t.SCHEMAID and t.TABLENAME = '" + table.getSourceTableName() + "' and t.TABLEID = c.REFERENCEID and COLUMNDEFAULT IS NOT NULL");
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
