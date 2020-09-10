package com.splicemachine.cs.databasemigration.schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class Table implements Comparable<Table> {
    String sourceSchemaName = null;
    String sourceTableName = null;
    
    String targetSchemaName = null;
    String targetTableName = null;
    
    String catalogName = null;
    long numRecords = 0;
    int distance = 0;
    int columnCount = 0;
    ArrayList<String> foreignKeys = new ArrayList<String>();

    public Table(String table) {
        this.sourceTableName = table;
    }

    public Table(String schema, String table) {
        this.sourceTableName = table;
        this.sourceSchemaName = schema;
        
        this.targetTableName = table;
        this.targetSchemaName = schema;
        
    }

    public Table(String schema, String table, HashMap<String,String> schemaMapping) {
        this.sourceTableName = table;
        this.sourceSchemaName = schema;
        
        this.targetTableName = table;
        this.targetSchemaName = schema;
        
        if(schemaMapping != null) {
            String mapToName = schemaMapping.get(schema);
            if(mapToName!= null) {
                this.targetSchemaName = mapToName;
            }
        }
     }
    
    public Table(String cat, String schema, String table) {
        this.catalogName = cat;
        this.sourceTableName = table;
        this.sourceSchemaName = schema;
    }

    public void setNumRecords(long numberOfRecords) {
        this.numRecords = numberOfRecords;
    }

    public void setColumnCount(int numColumns) {
        this.columnCount = numColumns;
    }

    public String getSourceFullTableName() {
        return getSourceFullTableName(false);
    }

    public String getSourceFullTableName(boolean addQuotes) {
        if (addQuotes) {
            if (sourceSchemaName != null && sourceSchemaName.length() > 0) {
                return "\"" + sourceSchemaName + "\".\"" + sourceTableName + "\"";
            } else {
                return "\"" + sourceTableName + "\"";
            }
        } else {
            if (sourceSchemaName != null && sourceTableName.length() > 0) {
                return sourceSchemaName + "." + sourceTableName;
            } else {
                return sourceTableName;
            }
        }
    }

    public String getTargetFullTableName() {
    	return getTargetFullTableName(false);
    }
    
    public String getTargetFullTableName(boolean addQuotes) {
        if (addQuotes) {
        	if (targetSchemaName != null && targetSchemaName.length() > 0) {
        		return "\"" + targetSchemaName + "\".\"" + targetTableName + "\"";
        	} else {
        		return "\"" + targetTableName + "\"";
        	}
        } else {
        	if (targetSchemaName != null && targetSchemaName.length() > 0) {
                return targetSchemaName + "." + targetTableName;
            } else {
            	return targetTableName;
            }
        }
    }
    
    public String getSourceSchema() {
        return sourceSchemaName;
    }

    public String getTargetSchema() {
        return targetSchemaName;
    }
    
    public long getNumberOfRecords() {
        return this.numRecords;
    }

    public String getSourceTableName() {
        return this.sourceTableName;
    }

    public String getTargetTableName() {
        return this.targetTableName;
    }
    
    public int getDistance() {
        return this.distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public void addForeignKey(String fk) {
        foreignKeys.add(fk);
    }

    public int compareTo(Table n) {
        return this.distance - n.distance;
    }

    public static MyComparator getComparator() {
        return new MyComparator();
    }

    public ArrayList<String> getForeignKeys() {
        return foreignKeys;
    }

}

class MyComparator implements Comparator<Table> {
    public int compare(Table o1, Table o2) {
        if (o1.distance > o2.distance) {
            return -1;
        } else if (o1.distance < o2.distance) {
            return 1;
        }
        return 0;
    }
}
