package com.splicemachine.cs.databasemigration.output;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;

import com.splicemachine.cs.databasemigration.MigrateDatabaseConfig;
import com.splicemachine.cs.databasemigration.MigrateDatabaseConstants;
import com.splicemachine.cs.databasemigration.MigrateDatabaseUtils;
import com.splicemachine.cs.databasemigration.schema.Table;

public class FileOutput extends BaseOutput {
    
    String IMPORT_CMD = "CALL SYSCS_UTIL.IMPORT_DATA ('{SCHEMA}','{TABLE}',{INSERT_COLUMN_LIST},'{IMPORT_PATH}/{SCHEMA}/{TABLE}',{COLUMN_DELIM},{CHAR_DELIM},{TIMESTAMP_FORMAT},{DATE_FORMAT},{TIME_FORMAT},{BAD_RECORD_CNT},'{BAD_PATH}/{SCHEMA}/{TABLE}',{ONE_LINE_RECORDS},{CHARSET});";
    
    PrintWriter createFile = null;
    PrintWriter dropFile = null;
    PrintWriter loadFile = null;
    PrintWriter sqoopTableList = null;

    PrintWriter fkFile = null;
    PrintWriter fkDropFile = null;
    PrintWriter ccFile = null;
    
    int createFileCount = 1;
    int numObjectsPerFile = 0;

    PrintWriter createSequenceFile = null;
    PrintWriter dropSequenceFile = null;

    public FileOutput(MigrateDatabaseConfig config) {
        super(config);
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE TABLE SCRIPTS */
    /*************************************************************/

    public void startCreateTable(String schema) throws Exception {
        MigrateDatabaseUtils
                .writeMessage("CREATING CREATE TABLE SCRIPT FOR SCHEMA:"
                        + schema);
        try {
            createFileCount = 1;
            numObjectsPerFile = 0;
        } catch (Exception e) {
            MigrateDatabaseUtils.logError(
                    "Exception creating schema for table", e);
            throw e;
        }

    }

    public void outputCreateTable(String schema, String sTableNameWithQuotes, String sFullTableName,
            String createTableStmt, String insertColumnList, boolean useOneLineRecords, String charSet) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING TABLE FOR TABLE:"
                + sFullTableName);

        if(numObjectsPerFile == 0) {
            //This is the first time that a record is being written to this
            //file, so we want to create the files
            
            this.closeFiles();
            
            String createFileName = replace(this.config.getCreateTableFileFormat(), "{SCHEMA}", schema);
            createFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateTableSubDirectory(), createFileName.toLowerCase());
            
            if(config.createDropTableScript) {
            	String dropFileName = replace(this.config.getDropTableFileFormat(), "{SCHEMA}", schema);
            	
                dropFile = createFile(this.config.getScriptOutputPath(), this.config.getDropTableSubDirectory(), dropFileName.toLowerCase());
            }
            
            if(config.createImportForEachSchema) {
            	String importSchemaFileName = replace(this.config.getImportForEachSchemaFileFormat(), "{SCHEMA}", schema);
                loadFile = createFile(this.config.getScriptOutputPath(), this.config.getImportScriptSubDirectory(), importSchemaFileName);
            }
                       
            if (this.config.bUseSchema) {
                //We only want to do this if the schema is not a user
                if(!config.isSchemaAUser(schema)) {
                    createFile.write("CREATE SCHEMA ");
                    createFile.write(schema);
                    createFile.write(";");
                    createFile.write(MigrateDatabaseConstants.NEW_LINE);
                }
            }
        }
        
        numObjectsPerFile++;
        if(config.createDropTableScript) {
            dropFile.write("DROP TABLE ");
            dropFile.write(sTableNameWithQuotes);
            dropFile.write(";");
            dropFile.write(MigrateDatabaseConstants.NEW_LINE);
        }
        
        
        createFile.write(createTableStmt);
        createFile.write(";");
        createFile.write(MigrateDatabaseConstants.NEW_LINE);


        String tablename = sFullTableName;
        int lastPeriod = tablename.lastIndexOf(".");
        if(lastPeriod > -1) {
            tablename = sFullTableName.substring(lastPeriod+1);
        }
        String truncateCmd = buildTruncateString(schema, tablename);
        String vacuumCmd = "CALL SYSCS_UTIL.VACUUM();";
        String loadCmd = buildImportString(schema, tablename, insertColumnList, useOneLineRecords, charSet);
        String majorCompactCmd = buildMajorCompactString(schema, tablename);
        if(config.isCreateImportForEachSchema()) {
        	if (config.isAddTruncate()) {
        		loadFile.write(truncateCmd);
        		loadFile.write(MigrateDatabaseConstants.NEW_LINE);
        	}
        	if (config.isAddVacuum()) {
        		loadFile.write(vacuumCmd);
        		loadFile.write(MigrateDatabaseConstants.NEW_LINE);
        	}
        	loadFile.write(loadCmd);
            loadFile.write(MigrateDatabaseConstants.NEW_LINE);
            if (config.isAddMajorCompact()) {
            	loadFile.write(majorCompactCmd);
            	loadFile.write(MigrateDatabaseConstants.NEW_LINE);
            }
        } else if (config.isCreateImportForEachTable()) {
        	String importTableFileName = replace(this.config.getImportForEachTableFileFormat(), "{SCHEMA}", schema);
        	importTableFileName = replace(importTableFileName, "{TABLE}", tablename);
            PrintWriter outFile = createFile(this.config.getScriptOutputPath(), this.config.getImportScriptSubDirectory(), importTableFileName);
        	if (config.isAddTruncate()) {
        		outFile.write(truncateCmd);
        		outFile.write(MigrateDatabaseConstants.NEW_LINE);
        	}
        	if (config.isAddVacuum()) {
        		outFile.write(vacuumCmd);
        		outFile.write(MigrateDatabaseConstants.NEW_LINE);
        	}
        	outFile.write(loadCmd);
            outFile.write(MigrateDatabaseConstants.NEW_LINE);
            if (config.isAddMajorCompact()) {
            	outFile.write(majorCompactCmd);
            	outFile.write(MigrateDatabaseConstants.NEW_LINE);
        	}
            outFile.close();
        }
    }
    
    public String buildTruncateString(String schema, String tablename) {
    	return "TRUNCATE TABLE \"" + schema.toUpperCase() + "\".\"" + tablename.toUpperCase() + "\";";
    }
    
    public String buildMajorCompactString(String schema, String tablename) {
    	return "CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE('" + schema.toUpperCase() + "','" + tablename.toUpperCase() + "');";
    }
    
    public String buildImportString(
    		String schema, 
    		String tablename, 
    		String insertColumnList, 
    		boolean useOneLineRecords, 
    		String charSet
    		) {
        String temp = IMPORT_CMD;
        temp = StringUtils.replace(temp, "{SCHEMA}", schema);
        temp = StringUtils.replace(temp, "{TABLE}", tablename);
        temp = StringUtils.replace(temp, "{IMPORT_PATH}", getValue(config.getImportPathOnHDFS(), "/data",false));
        temp = StringUtils.replace(temp, "{INSERT_COLUMN_LIST}", (insertColumnList==null) ? "null" : "'" + insertColumnList + "'");
        temp = StringUtils.replace(temp, "{ONE_LINE_RECORDS}", useOneLineRecords?"true":"false");
        temp = StringUtils.replace(temp, "{CHARSET}", (charSet==null) ? "null" : "'" + charSet + "'"); 
                
        
        String columnDelimiter = config.getDelimiter();
        temp = StringUtils.replace(temp, "{COLUMN_DELIM}", getValue(columnDelimiter, "null",true));
        

        String charDelimiter = config.getCellDelimiter();
        temp = StringUtils.replace(temp, "{CHAR_DELIM}", getValue(charDelimiter, "null",true));

        temp = StringUtils.replace(temp, "{TIMESTAMP_FORMAT}", getValue(config.getTimestampFormat(), "null",true));
        temp = StringUtils.replace(temp, "{DATE_FORMAT}", getValue(config.getDateFormat(), "null",true));
        temp = StringUtils.replace(temp, "{TIME_FORMAT}", getValue(config.getTimeFormat(), "null",true));
        temp = StringUtils.replace(temp, "{BAD_RECORD_CNT}", getValue(Integer.toString(config.getFailBadRecordCount()), "1",false));
        temp = StringUtils.replace(temp, "{BAD_PATH}", getValue(config.getBadPathOnHDFS(), "/bad",false));
        return temp;
    }
    
    public String buildSqoopScriptContents(String schema) {
        return schema;
    }
    
    public String getValue(String val, String defaultVal, boolean addQuotes) {
        if(val == null || val.length() == 0 || val.equalsIgnoreCase("null")) {
            return defaultVal;
        } else {
            if(addQuotes) {
                return "'" + val + "'";
            } else {
                return val;
            }
        }
    }

    public void endCreateTable() {
        closeFiles();
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE SEQUENCE SCRIPTS */
    /*************************************************************/

    public void startCreateSequence(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING SEQUENCE SCRIPT:" + schema);
        createFileCount = 1;
        numObjectsPerFile = 0;

    }

    public void ouputCreateSequence(String schema, String sFullTableName,
            String createTableStmt) throws Exception {

        writeMessage("CREATING SEQUENCE NAME:" + sFullTableName);
        
        if(numObjectsPerFile == 0) {
        	String createSequencesFileName = replace(this.config.getCreateSequenceFileFormat(), "{SCHEMA}", schema);
            createFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateSequenceSubDirectory(), createSequencesFileName.toLowerCase());
            String dropSequencesFileName = replace(this.config.getDropSequenceFileFormat(), "{SCHEMA}", schema);
            dropFile = createFile(this.config.getScriptOutputPath(), this.config.getDropSequenceSubDirectory(), dropSequencesFileName.toLowerCase());
            if (config.bUseSchema) {
                dropFile.write("SET SCHEMA ");
                dropFile.write(schema);
                dropFile.write(";");
                dropFile.write(MigrateDatabaseConstants.NEW_LINE);

                createFile.write("SET SCHEMA ");
                createFile.write(schema);
                createFile.write(";");
                createFile.write(MigrateDatabaseConstants.NEW_LINE);
            }
        }

        // Some schemas may have a large number of tables. In that case
        // we want to break up the create table statements for that schema
        // into smaller files.
        numObjectsPerFile++;

        dropFile.write("DROP SEQUENCE ");
        dropFile.write(sFullTableName);
        dropFile.write(" RESTRICT;");
        dropFile.write(MigrateDatabaseConstants.NEW_LINE);

        createFile.write(createTableStmt);
        createFile.write(";");
        createFile.write(MigrateDatabaseConstants.NEW_LINE);

    }

    public void endCreateSequence() {
        closeFiles();
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE INDEX SCRIPTS */
    /*************************************************************/

    public void startCreateIndex(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING INDEX SCRIPT:" + schema);
        createFileCount = 1;
        numObjectsPerFile = 0;

    }

    public void ouputCreateIndex(String schema, String sName, String createStmt)
            throws Exception {

        writeMessage("CREATING INDEX NAME:" + sName);
        
        if(numObjectsPerFile == 0) {
            
        	String createIndexesFileName = replace(this.config.getCreateIndexFileFormat(), "{SCHEMA}", schema);
            createFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateIndexSubDirectory(), createIndexesFileName.toLowerCase());

            if(config.createDropIndexScript) {
            	String dropIndexesFileName = replace(this.config.getDropIndexFileFormat(), "{SCHEMA}", schema);
                dropFile = createFile(this.config.getScriptOutputPath(), this.config.getDropIndexSubDirectory(), dropIndexesFileName.toLowerCase());
            }
            
            if (config.bUseSchema) {
                if(config.createDropIndexScript) {
                    dropFile.write("SET SCHEMA ");
                    dropFile.write(schema);
                    dropFile.write(";");
                    dropFile.write(MigrateDatabaseConstants.NEW_LINE);
                }

                createFile.write("SET SCHEMA ");
                createFile.write(schema);
                createFile.write(";");
                createFile.write(MigrateDatabaseConstants.NEW_LINE);
            }
        }
        
        numObjectsPerFile++;

        if(config.createDropIndexScript) {
            dropFile.write("DROP INDEX ");
            dropFile.write(sName);
            dropFile.write(";");
            dropFile.write(MigrateDatabaseConstants.NEW_LINE);
        }

        createFile.write(createStmt);
        createFile.write(";");
        createFile.write(MigrateDatabaseConstants.NEW_LINE);

    }

    public void endCreateIndex() {
        closeFiles();
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE UNIQUE INDEX SCRIPTS */
    /*************************************************************/

    public void startCreateUniqueIndex(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING UNIQUE INDEX SCRIPT:" + schema);
        createFileCount = 1;
        numObjectsPerFile = 0;

    }

    public void ouputCreateUniqueIndex(String schema, String sName, String createStmt)
            throws Exception {

        writeMessage("CREATING UNIQUE INDEX NAME:" + sName);
        
        if(numObjectsPerFile == 0) {
            
        	String createUniqueIndexesFileName = replace(this.config.getCreateUniqueIndexFileFormat(), "{SCHEMA}", schema);
            createFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateIndexSubDirectory(), createUniqueIndexesFileName.toLowerCase());

            if(config.createDropIndexScript) {
            	String dropUniqueIndexesFileName = replace(this.config.getDropUniqueIndexFileFormat(), "{SCHEMA}", schema);
                dropFile = createFile(this.config.getScriptOutputPath(), this.config.getDropIndexSubDirectory(), dropUniqueIndexesFileName.toLowerCase());
            }
            
            if (config.bUseSchema) {
                if(config.createDropIndexScript) {
                    dropFile.write("SET SCHEMA ");
                    dropFile.write(schema);
                    dropFile.write(";");
                    dropFile.write(MigrateDatabaseConstants.NEW_LINE);
                }

                createFile.write("SET SCHEMA ");
                createFile.write(schema);
                createFile.write(";");
                createFile.write(MigrateDatabaseConstants.NEW_LINE);
            }
        }
        
        numObjectsPerFile++;

        if(config.createDropIndexScript) {
            dropFile.write("DROP INDEX ");
            dropFile.write(sName);
            dropFile.write(";");
            dropFile.write(MigrateDatabaseConstants.NEW_LINE);
        }

        createFile.write(createStmt);
        createFile.write(";");
        createFile.write(MigrateDatabaseConstants.NEW_LINE);

    }

    public void endCreateUniqueIndex() {
        closeFiles();
    }
    
    
    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE FOREIGN KEY SCRIPTS                */
    /*************************************************************/

    public void startCreateForeignKey(String schema) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING FOREIGN KEY:" + schema);
        if(fkFile != null) {
            fkFile.close();
            fkFile = null;
        }
        if(fkDropFile != null) {
            fkDropFile.flush();
            fkDropFile.close();
            fkDropFile = null;
        }
        

    }

    public void ouputCreateForeignKey(String schema, String sName, String createStmt)
            throws Exception {

        writeMessage("CREATING FOREIGN KEYNAME:" + sName);
        
        if(fkFile == null) {
        	String createFKeyFileName = replace(this.config.getCreateFKeyFileFormat(), "{SCHEMA}", schema);
            fkFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateForeignKeysSubDirectory(), createFKeyFileName.toLowerCase());

            if(config.isCreateDropForeignKeys()) {
            	String dropFKeyFileName = replace(this.config.getDropFKeyFileFormat(), "{SCHEMA}", schema);
                fkDropFile = createFile(this.config.getScriptOutputPath(), this.config.getDropForeignKeysSubDirectory(), dropFKeyFileName.toLowerCase());
            }
            

        }
        

        fkFile.write(createStmt);
        fkFile.write(";");
        fkFile.write(MigrateDatabaseConstants.NEW_LINE);
        
        if(config.isCreateDropForeignKeys()) {
            fkDropFile.write("DROP CONSTRAINT ");
            fkDropFile.write(sName);
            fkDropFile.write(";");
            fkDropFile.write(MigrateDatabaseConstants.NEW_LINE);            
        }
    }

    public void endCreateForeignKey() {
        if (fkFile != null) {
            fkFile.flush();
            fkFile.close();
        }
        if(fkDropFile != null) {
            fkDropFile.flush();
            fkDropFile.close();
        }
        fkFile = null;
        fkDropFile = null;
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATE CHECK CONSTRAINTS SCRIPTS                */
    /*************************************************************/

    public void startCreateConstraint(ArrayList<String> schemas) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING CHECK CONSTRAINTS");
        if(ccFile != null) {
            ccFile.close();
            ccFile = null;
        }
    }

    public void outputCreateConstraint(String schema, String sName, String createStmt)
            throws Exception {

        writeMessage("CREATING CHECK CONTRAINT:" + sName);

        if(ccFile == null) {
            String createCCFileName = replace(this.config.getCreateConstraintFileFormat(), "{SCHEMA}", schema);
            ccFile = createFile(this.config.getScriptOutputPath(), this.config.getCreateConstraintSubDirectory(), createCCFileName.toLowerCase());
        }

        ccFile.write(createStmt);
        ccFile.write(";");
        ccFile.write(MigrateDatabaseConstants.NEW_LINE);
    }

    public void endCreateConstraint() {
        if (ccFile != null) {
            ccFile.flush();
            ccFile.close();
        }
        ccFile = null;
    }

    /*************************************************************/
    /* BEGIN LOGIC FOR CREATING USERS                            */
    /*************************************************************/
    
    public void createUsers(ArrayList<String> users)  {
        MigrateDatabaseUtils.writeMessage("CREATING USERS FILE:");
        
        try {
            int numUsers = users == null ? 0 : users.size();
            if(numUsers > 0) {
                PrintWriter out = createFile(this.config.getScriptOutputPath(), this.config.getCreateUserSubDirectory(), "create-users.sql");
                
                for(int i=0; i<numUsers; i++) {
                    out.println("call syscs_util.syscs_create_user('" + users.get(i) + "', 'password');");
                }
                out.close();
            } else {
                writeMessage(Level.INFO, "There are no users to export");
            }
        } catch (Exception e) {
            writeMessage(Level.ERROR, "Exception creating create_users.sql");
            e.printStackTrace();
        }
    }
    
    /*************************************************************/
    /* BEGIN LOGIC FOR EXPORT DATA */
    /*************************************************************/

    /**
     * Creates the initial file for the exported data and also if specified
     * prints the column names in the output
     */
    public void startExportData(Table table, ResultSetMetaData resMeta)
            throws Exception {
        writeMessage("Exporting to CSV: " + table.getSourceFullTableName());

        createFileCount = 1;
        numObjectsPerFile = 0;

        String path = this.config.getScriptOutputPath() + config.getImportPathOnHDFS() + "/" + table.getTargetSchema() + "/" + table.getTargetTableName() + "/";
        createFiles(path, table.getSourceFullTableName(), "data", createFileCount, false,
                true, false, ".csv");

        // Includes the column headers in the output
        if (config.exportColumnNames) {
            int columnCount = resMeta.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                if (i > 1)
                    createFile.write(config.getDelimiter());
                createFile.write(resMeta.getColumnName(i));
            }
            createFile.write(MigrateDatabaseConstants.NEW_LINE);
        }
    }

    public void ouputExportData(Table table, ResultSetMetaData resMeta,
            ResultSet res) throws Exception {
        try {
            long startTime = System.currentTimeMillis();
            long total = 0;
            int columnCount = resMeta.getColumnCount();
            String tableName = table.getSourceFullTableName();
            
            String cellDelimiter = config.getCellDelimiter();          
            String delimiter = config.getDelimiter();
            
            if(delimiter == null || delimiter.length() == 0) delimiter = ",";
            
            boolean useCellDelimiter = cellDelimiter != null;
            while (res.next()) {

                if (createFileCount != 1 && numObjectsPerFile % 5000000 == 0) {
                    createFileCount++;
                    String path = this.config.getScriptOutputPath() + config.getImportPathOnHDFS() + "/" + table.getTargetSchema() + "/" + table.getTargetTableName() + "/";
                    createFiles(path, tableName, "data", createFileCount, true, true,
                            false, ".csv");
                    numObjectsPerFile = 0;
                }
                numObjectsPerFile++;
                total++;

                for (int i = 1; i <= columnCount; i++) {
                    if (res.getObject(i) != null) {
                        if (useCellDelimiter) {
                            createFile.write(cellDelimiter);
                        }
                        createFile.write(res.getObject(i).toString());
                        if (useCellDelimiter) {
                            createFile.write(cellDelimiter);
                        }
                    }
                    if (i != columnCount) {
                        createFile.write(delimiter);
                    }
                }
                createFile.write(MigrateDatabaseConstants.NEW_LINE);
            }
            long endTime = System.currentTimeMillis();
            writeMessage(Level.WARN, "total records imported: [" + total
                    + "] duration=[" + (endTime - startTime) / 1000
                    + " seconds]");

        } finally {
            try {
                res.close();
            } catch (Exception ignore) {
            }
        }

    }

    public void endExportData() {
        closeFiles();
    }

    public void outputSqoopQueryFiles(String schema, String table, String sqoopQuery) throws Exception {
        MigrateDatabaseUtils.writeMessage("CREATING SQOOP QUERY FILE FOR TABLE:"
                + table);
        String queryFileName = replace(this.config.getSqoopQueryFileNameFormat(), "{SCHEMA}", schema);
        queryFileName = replace(queryFileName, "{TABLE}", table);
        PrintWriter outFile = createFile(this.config.getScriptOutputPath(), this.config.getSqoopQuerySubDirectory(), queryFileName);
        outFile.write(sqoopQuery);
        outFile.close();
    }
    
    public void endSqoopQueryFiles() {
    	//do nothiing
    }
    
    /**
     * Creates a file from a root directory, then a folder under that directory and finally the 
     * filename.  It is assumed that the directories do not end with a slash - the trailing 
     * slashers should have been removed when they were read in by the configurator.  Sub directories
     * will have the leading slash but not the trailing slash - this should be set by the process
     * of reading in the database configuration settings.
     * 
     * @param scriptOutputPath
     * @param subDirectory
     * @param filename
     * @return
     * @throws UnsupportedEncodingException 
     * @throws FileNotFoundException 
     */
    public PrintWriter createFile(String scriptOutputPath, String subDirectory, String filename) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        
        String fullFolderName = scriptOutputPath;
        if(subDirectory != null && subDirectory.length() > 0) {
            fullFolderName += subDirectory;
        }
        
        File myFolderPath = new File(fullFolderName);
        if(!myFolderPath.exists()) {
            myFolderPath.mkdirs();
        }

        File myFile = new File(myFolderPath, filename);
        PrintWriter outFile = null;
        if(myFile.exists() && !myFile.isDirectory()) { 
        	FileWriter fw = new FileWriter(myFile, true);
        	outFile = new PrintWriter(fw);
        } else {
        	outFile = new PrintWriter(myFile, "UTF-8");
        }
        
        return outFile;
    }

    public void createFiles(String schema, String type, int count, boolean close)
            throws FileNotFoundException, UnsupportedEncodingException {

        createFiles(schema.toLowerCase(), type.toLowerCase(), count, close, false, count == 1, ".sql");
    }

    public void createFiles(String schema, String type, int count,
            boolean close, boolean padCount, boolean createDrop,
            String extension) throws FileNotFoundException,
            UnsupportedEncodingException {
        MigrateDatabaseUtils.writeMessage("Creating Files");

        if (close) {
            createFile.close();
        }

        String sCount = "" + count;
        if (padCount) {
            sCount = padFileName(count);
        }

        createFile = new PrintWriter(this.config.getScriptOutputPath()
                + "create-" + schema + "-" + type + extension,
                "UTF-8");

        if (createDrop) {
            dropFile = new PrintWriter(this.config.getScriptOutputPath()
                    + "drop-" + schema + "-" + type + extension, "UTF-8");
        }
    }

    public void createFiles(String path, String schema, String type, int count,
                            boolean close, boolean padCount, boolean createDrop,
                            String extension) throws FileNotFoundException,
            UnsupportedEncodingException {
        MigrateDatabaseUtils.writeMessage("Creating Files");

        if (close) {
            createFile.close();
        }

        String sCount = "" + count;
        if (padCount) {
            sCount = padFileName(count);
        }

        File file = new File(path);
        if(!file.exists()) {
            file.mkdirs();
        }

        createFile = new PrintWriter(path
                + "create-" + schema + "-" + type + extension,
                "UTF-8");

        if (createDrop) {
            dropFile = new PrintWriter(this.config.getScriptOutputPath()
                    + "drop-" + schema + "-" + type + extension, "UTF-8");
        }


    }

    public void closeFiles() {
        MigrateDatabaseUtils.writeMessage("Closing Files");
        if (createFile != null) {
            createFile.flush();
            createFile.close();
        }
        if (dropFile != null) {
            dropFile.flush();
            dropFile.close();
        }
        
        if(loadFile != null) {
            loadFile.flush();
            loadFile.close();
        }
        
        if(sqoopTableList != null) {
            sqoopTableList.flush();
            sqoopTableList.close();
        }

        loadFile = null;
        createFile = null;
        dropFile = null;
        sqoopTableList = null;
    }

    public String padFileName(long number) {
        return StringUtils.leftPad(Long.toString(number),
                config.getFileNumberPadding(), "0");
    }
    
    public String replace(String str, String search, String replace) {
    	return StringUtils.replace(str, search, replace);
    }
}
