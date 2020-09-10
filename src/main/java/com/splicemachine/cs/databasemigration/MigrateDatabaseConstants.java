package com.splicemachine.cs.databasemigration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Constants for the Database Migration Tool
 * 
 * @author tedmonddong
 *
 */
public class MigrateDatabaseConstants {

	public static final String TAB = "\t";
	public static final String SPACE = " ";
	public static final String NEW_LINE = System.getProperty("line.separator");
	
	public static final String DELIM_TAB = "\t";
	public static final String DELIM_COMMA = ",";
	
	public static final String EXPORT_FILE = "FS";
	public static final String EXPORT_HDFS = "HDFS";
	
	public static ArrayList<String> RESERVED_WORDS = null;

	public enum LogLevel {
		DEBUG("DEBUG", 0),
		VERBOSE("VERBOSE", 1);
		
		String code;
		int level = -1;
		
		private static Map<String, LogLevel> codeToLevelMapping;
		
	    private LogLevel(String code, int level) {
	        this.code = code;
	        this.level = level;
	    }

	    public static LogLevel getLogLevel(String i) {
	        if (codeToLevelMapping == null) {
	            initMapping();
	        }
	        return codeToLevelMapping.get(i);
	    }
	 
	    private static void initMapping() {
	    	codeToLevelMapping = new HashMap<String, LogLevel>();
	        for (LogLevel s : values()) {
	        	codeToLevelMapping.put(s.code, s);
	        }
	    }

	}
	
	public static ArrayList<String> getReservedWords() {
	    if(RESERVED_WORDS == null) {
	        RESERVED_WORDS = new ArrayList<String>();
	        RESERVED_WORDS.add("ADD");
	        RESERVED_WORDS.add("ALL");
	        RESERVED_WORDS.add("ALLOCATE");
	        RESERVED_WORDS.add("ALTER");
	        RESERVED_WORDS.add("AND");
	        RESERVED_WORDS.add("ANY");
	        RESERVED_WORDS.add("ARE");
	        RESERVED_WORDS.add("AS");
	        RESERVED_WORDS.add("ASC");
	        RESERVED_WORDS.add("ASSERTION");
	        RESERVED_WORDS.add("AT");
	        RESERVED_WORDS.add("AUTHORIZATION");
	        RESERVED_WORDS.add("AVG");
	        RESERVED_WORDS.add("BEGIN");
	        RESERVED_WORDS.add("BETWEEN");
	        RESERVED_WORDS.add("BIGINT");
	        RESERVED_WORDS.add("BIT");
	        RESERVED_WORDS.add("BOOLEAN");
	        RESERVED_WORDS.add("BOTH");
	        RESERVED_WORDS.add("BY");
	        RESERVED_WORDS.add("CALL");
	        RESERVED_WORDS.add("CASCADE");
	        RESERVED_WORDS.add("CASCADED");
	        RESERVED_WORDS.add("CASE");
	        RESERVED_WORDS.add("CAST");
	        RESERVED_WORDS.add("CHAR");
	        RESERVED_WORDS.add("CHARACTER");
	        RESERVED_WORDS.add("CHECK");
	        RESERVED_WORDS.add("CLOSE");
	        RESERVED_WORDS.add("COALESCE");
	        RESERVED_WORDS.add("COLLATE");
	        RESERVED_WORDS.add("COLLATION");
	        RESERVED_WORDS.add("COLUMN");
	        RESERVED_WORDS.add("COMMIT");
	        RESERVED_WORDS.add("CONNECT");
	        RESERVED_WORDS.add("CONNECTION");
	        RESERVED_WORDS.add("CONSTRAINT");
	        RESERVED_WORDS.add("CONSTRAINTS");
	        RESERVED_WORDS.add("CONTINUE");
	        RESERVED_WORDS.add("CONVERT");
	        RESERVED_WORDS.add("CORRESPONDING");
	        RESERVED_WORDS.add("CREATE");
	        RESERVED_WORDS.add("CROSS");
	        RESERVED_WORDS.add("CURRENT");
	        RESERVED_WORDS.add("CURRENT_DATE");
	        RESERVED_WORDS.add("CURRENT_ROLE");
	        RESERVED_WORDS.add("CURRENT_TIME");
	        RESERVED_WORDS.add("CURRENT_TIMESTAMP");
	        RESERVED_WORDS.add("CURRENT_USER");
	        RESERVED_WORDS.add("CURSOR");
	        RESERVED_WORDS.add("DEALLOCATE");
	        RESERVED_WORDS.add("DEC");
	        RESERVED_WORDS.add("DECIMAL");
	        RESERVED_WORDS.add("DECLARE");
	        RESERVED_WORDS.add("DEFAULT");
	        RESERVED_WORDS.add("DEFERRABLE");
	        RESERVED_WORDS.add("DEFERRED");
	        RESERVED_WORDS.add("DELETE");
	        RESERVED_WORDS.add("DESC");
	        RESERVED_WORDS.add("DESCRIBE");
	        RESERVED_WORDS.add("DIAGNOSTICS");
	        RESERVED_WORDS.add("DISCONNECT");
	        RESERVED_WORDS.add("DISTINCT");
	        RESERVED_WORDS.add("DOUBLE");
	        RESERVED_WORDS.add("DROP");
	        RESERVED_WORDS.add("ELSE");
	        RESERVED_WORDS.add("END");
	        RESERVED_WORDS.add("END-EXEC");
	        RESERVED_WORDS.add("ESCAPE");
	        RESERVED_WORDS.add("EXCEPT");
	        RESERVED_WORDS.add("EXCEPTION");
	        RESERVED_WORDS.add("EXEC");
	        RESERVED_WORDS.add("EXECUTE");
	        RESERVED_WORDS.add("EXISTS");
	        RESERVED_WORDS.add("EXPLAIN");
	        RESERVED_WORDS.add("EXTERNAL");
	        RESERVED_WORDS.add("FALSE");
	        RESERVED_WORDS.add("FETCH");
	        RESERVED_WORDS.add("FIRST");
	        RESERVED_WORDS.add("FLOAT");
	        RESERVED_WORDS.add("FOR");
	        RESERVED_WORDS.add("FOREIGN");
	        RESERVED_WORDS.add("FOUND");
	        RESERVED_WORDS.add("FROM");
	        RESERVED_WORDS.add("FULL");
	        RESERVED_WORDS.add("FUNCTION");
	        RESERVED_WORDS.add("GET");
	        RESERVED_WORDS.add("GETCURRENTCONNECTION");
	        RESERVED_WORDS.add("GLOBAL");
	        RESERVED_WORDS.add("GO");
	        RESERVED_WORDS.add("GOTO");
	        RESERVED_WORDS.add("GRANT");
	        RESERVED_WORDS.add("GROUP");
	        RESERVED_WORDS.add("HAVING");
	        RESERVED_WORDS.add("HOUR");
	        RESERVED_WORDS.add("IDENTITY");
	        RESERVED_WORDS.add("IMMEDIATE");
	        RESERVED_WORDS.add("IN");
	        RESERVED_WORDS.add("INDICATOR");
	        RESERVED_WORDS.add("INITIALLY");
	        RESERVED_WORDS.add("INNER");
	        RESERVED_WORDS.add("INOUT");
	        RESERVED_WORDS.add("INPUT");
	        RESERVED_WORDS.add("INSENSITIVE");
	        RESERVED_WORDS.add("INSERT");
	        RESERVED_WORDS.add("INT");
	        RESERVED_WORDS.add("INTEGER");
	        RESERVED_WORDS.add("INTERSECT");
	        RESERVED_WORDS.add("INTO");
	        RESERVED_WORDS.add("IS");
	        RESERVED_WORDS.add("ISOLATION");
	        RESERVED_WORDS.add("JOIN");
	        RESERVED_WORDS.add("KEY");
	        RESERVED_WORDS.add("LAST");
	        RESERVED_WORDS.add("LEFT");
	        RESERVED_WORDS.add("LIKE");
	        RESERVED_WORDS.add("LOWER");
	        RESERVED_WORDS.add("LTRIM");
	        RESERVED_WORDS.add("MATCH");
	        RESERVED_WORDS.add("MAX");
	        RESERVED_WORDS.add("MIN");
	        RESERVED_WORDS.add("MINUTE");
	        RESERVED_WORDS.add("NATIONAL");
	        RESERVED_WORDS.add("NATURAL");
	        RESERVED_WORDS.add("NCHAR");
	        RESERVED_WORDS.add("NVARCHAR");
	        RESERVED_WORDS.add("NEXT");
	        RESERVED_WORDS.add("NO");
	        RESERVED_WORDS.add("NONE");
	        RESERVED_WORDS.add("NOT");
	        RESERVED_WORDS.add("NULL");
	        RESERVED_WORDS.add("NULLIF");
	        RESERVED_WORDS.add("NUMERIC");
	        RESERVED_WORDS.add("OF");
	        RESERVED_WORDS.add("ON");
	        RESERVED_WORDS.add("ONLY");
	        RESERVED_WORDS.add("OPEN");
	        RESERVED_WORDS.add("OPTION");
	        RESERVED_WORDS.add("OR");
	        RESERVED_WORDS.add("ORDER");
	        RESERVED_WORDS.add("OUTER");
	        RESERVED_WORDS.add("OUTPUT");
	        RESERVED_WORDS.add("OVER");
	        RESERVED_WORDS.add("OVERLAPS");
	        RESERVED_WORDS.add("PAD");
	        RESERVED_WORDS.add("PARTIAL");
	        RESERVED_WORDS.add("PREPARE");
	        RESERVED_WORDS.add("PRESERVE");
	        RESERVED_WORDS.add("PRIMARY");
	        RESERVED_WORDS.add("PRIOR");
	        RESERVED_WORDS.add("PRIVILEGES");
	        RESERVED_WORDS.add("PROCEDURE");
	        RESERVED_WORDS.add("PUBLIC");
	        RESERVED_WORDS.add("READ");
	        RESERVED_WORDS.add("REAL");
	        RESERVED_WORDS.add("REFERENCES");
	        RESERVED_WORDS.add("RELATIVE");
	        RESERVED_WORDS.add("RESTRICT");
	        RESERVED_WORDS.add("REVOKE");
	        RESERVED_WORDS.add("RIGHT");
	        RESERVED_WORDS.add("ROLLBACK");
	        RESERVED_WORDS.add("ROWS");
	        RESERVED_WORDS.add("ROW_NUMBER");
	        RESERVED_WORDS.add("RTRIM");
	        RESERVED_WORDS.add("SCHEMA");
	        RESERVED_WORDS.add("SCROLL");
	        RESERVED_WORDS.add("SECOND");
	        RESERVED_WORDS.add("SELECT");
	        RESERVED_WORDS.add("SESSION_USER");
	        RESERVED_WORDS.add("SET");
	        RESERVED_WORDS.add("SMALLINT");
	        RESERVED_WORDS.add("SOME");
	        RESERVED_WORDS.add("SPACE");
	        RESERVED_WORDS.add("SQL");
	        RESERVED_WORDS.add("SQLCODE");
	        RESERVED_WORDS.add("SQLERROR");
	        RESERVED_WORDS.add("SQLSTATE");
	        RESERVED_WORDS.add("SUBSTR");
	        RESERVED_WORDS.add("SUBSTRING");
	        RESERVED_WORDS.add("SUM");
	        RESERVED_WORDS.add("TABLE");
	        RESERVED_WORDS.add("TEMPORARY");
	        RESERVED_WORDS.add("TIMEZONE_HOUR");
	        RESERVED_WORDS.add("TIMEZONE_MINUTE");
	        RESERVED_WORDS.add("TO");
	        RESERVED_WORDS.add("TRANSACTION");
	        RESERVED_WORDS.add("TRANSLATE");
	        RESERVED_WORDS.add("TRANSLATION");
	        RESERVED_WORDS.add("TRIM");
	        RESERVED_WORDS.add("TRUE");
	        RESERVED_WORDS.add("TEXT");
	        RESERVED_WORDS.add("UNION");
	        RESERVED_WORDS.add("UNIQUE");
	        RESERVED_WORDS.add("UNKNOWN");
	        RESERVED_WORDS.add("UPDATE");
	        RESERVED_WORDS.add("UPPER");
	        RESERVED_WORDS.add("USER");
	        RESERVED_WORDS.add("USING");
	        RESERVED_WORDS.add("VALUES");
	        RESERVED_WORDS.add("VARCHAR");
	        RESERVED_WORDS.add("VARYING");
	        RESERVED_WORDS.add("VIEW");
	        RESERVED_WORDS.add("WHENEVER");
	        RESERVED_WORDS.add("WHERE");
	        RESERVED_WORDS.add("WITH");
	        RESERVED_WORDS.add("WORK");
	        RESERVED_WORDS.add("WRITE");
	        RESERVED_WORDS.add("XML");
	        RESERVED_WORDS.add("XMLEXISTS");
	        RESERVED_WORDS.add("XMLPARSE");
	        RESERVED_WORDS.add("XMLQUERY");
	        RESERVED_WORDS.add("XMLSERIALIZE");
	        RESERVED_WORDS.add("YEAR");
	    }
	    return RESERVED_WORDS;
	}
	
	public static boolean isReservedKeyword(String word) {
	    return getReservedWords().contains(word);
	}
}
