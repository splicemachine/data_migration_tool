package com.splicemachine.cs.databasemigration;

import java.io.File;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;

import com.splicemachine.cs.databasemigration.output.BaseOutput;
import com.splicemachine.cs.databasemigration.schema.DataTypeConversion;
import com.splicemachine.cs.databasemigration.schema.Table;
import com.splicemachine.cs.databasemigration.vendors.BaseDatabaseVendor;

/**
 * This class contains the configuration settings for the current running
 * instance
 * 
 * @author erindriggers
 * 
 */
public class MigrateDatabaseConfig {

	public String configFileLocation = null;
	public String connectionId = null;
	public String configId = null;

	public String databaseVendorFile = null;

	// Variables for the source database
	public String sourceJDBCUrl = null;
	public String sourceJDBCDriver = null;
	public String sourceUser = null;
	public String sourcePassword = null;
	public DatabaseMetaData sourceDatabaseMeta = null;
	public BaseDatabaseVendor sourceDatabaseVendor = null;
	public Connection sourceDatabaseConnection = null;

	// Variables for the target database
	public String targetJDBCUrl = null;
	public String targetJDBCDriver = null;
	public String targetUser = null;
	public String targetPassword = null;
	public BaseDatabaseVendor targetDatabaseVendor = null;
	public Connection targetDatabaseConnection = null;

	// Sqoop run extract script variables
	public String sqoopConfigFile = null;
	public String sqoopTableListPath = null;
	public String sqoopImportPath = null;
	public String sqoopQueryPath = null;
	public String sqoopLogPath = null;

	public String hadoopBin = null;
	public String spliceBin = null;

	public String sourceCatalog = null;
	public String sourceSchema = null;

	public boolean directConnection = false;

	// Variables for the script creation
	public String outputPath = null;
	public String createTableSubDirectory = null;
	public String createConstraintSubDirectory = null;
	public String createForeignKeysSubDirectory = null;
	public String createIndexSubDirectory = null;
	public String createUserSubDirectory = null;
	public String createSequenceSubDirectory = null;
	public String createRoleSubDirectory = null;
	public String createGrantSubDirectory = null;
	public String dropTableSubDirectory = null;
	public String dropForeignKeysSubDirectory = null;
	public String dropIndexSubDirectory = null;
	public String dropTriggerSubDirectory = null;
	public String dropSequenceSubDirectory = null;
	public String rolesToGrantRead = null;
	public String rolesToGrantWrite = null;
	public String rolesToGrantExecute = null;
	public boolean createTableScript = false;
	public boolean createDropTableScript = false;
	public boolean createSequenceScript = false;
	public boolean createDropSequenceScript = false;
	public boolean createIndexScript = false;
	public boolean createDropIndexScript = false;
	public boolean createDropTriggerScript = false;
	public boolean createForeignKeys = false;
	public boolean createDropForeignKeys = false;
	public boolean createCheckConstraints = false;
	public boolean createRoleScript = false;
	public boolean createGrantReadScript = false;
	public boolean createGrantWriteScript = false;
	public boolean createGrantExecuteScript = false;
	public boolean padVarchar = false;
	public boolean padChar = false;
	public int padVarcharValue = 0;
	public int padCharValue = 0;

	public boolean useUniqueIndexForMissingPrimary = false;
	public String primaryKeyUniqueIndexPrefix = "";
	public boolean addColumnDefault = true;

	// variables for file formats
	public String createTableFileFormat = null;
	public String dropTableFileFormat = null;
	public String createSequenceFileFormat = null;
	public String dropSequenceFileFormat = null;
	public String createIndexFileFormat = null;
	public String dropIndexFileFormat = null;
	public String createUniqueIndexFileFormat = null;
	public String dropUniqueIndexFileFormat = null;
	public String createFKeyFileFormat = null;
	public String dropFKeyFileFormat = null;
	public String createConstraintFileFormat = null;
	public String importForEachSchemaFileFormat = null;
	public String importForEachTableFileFormat = null;

	ArrayList<String> usersToSkip = new ArrayList<String>();

	// Packages and procedures
	public String exportProcedureDirectory = null;
	public String procedureList = null;
	public boolean exportProcedure = false;
	public boolean exportAllProcedures = false;

	public String exportPackageDirectory = null;
	public String packageList = null;
	public boolean exportPackage = false;
	public boolean exportAllPackages = false;

	public String exportFunctionDirectory = null;
	public boolean exportFunction = false;
	public boolean exportAllFunctions = false;
	public String functionList = null;

	public String exportViewsDirectory = null;
	public boolean exportViews = false;
	public boolean exportAllViews = false;
	public String viewList = null;

	public String exportTriggersDirectory = null;
	public boolean exportTriggers = false;
	public boolean exportAllTriggers = false;
	public String triggerList = null;

	public boolean createUsers = false;
	public boolean exportRoles = false;

	// Variables for data export
	public boolean exportData = false;
	public boolean exportColumnNames = false;
	public String exportOutputType = MigrateDatabaseConstants.EXPORT_FILE;
	public String exportOutputPath = null;
	public long maxRecordsPerTable = -1;
	public long maxRecordsPerFile = 5000000;
	public boolean compress = false;
	public String delimiter = MigrateDatabaseConstants.DELIM_TAB;
	public String cellDelimiter = null;

	// Splice Import Variables
	public boolean createImportForEachTable = false;
	public boolean createImportForEachSchema = false;
	public String importScriptSubDirectory = "/import";

	public String importPathOnHDFS = "/data/sqoop/";
	public String timestampFormat = "yyyy-MM-dd HH:mm:ss";
	public String dateFormat = "yyyy-MM-dd HH:mm:ss";
	public String timeFormat = "yyyy-MM-dd HH:mm:ss";
	public int failBadRecordCount = 1;
	public boolean addTruncate = false;
	public boolean addVacuum = false;
	public boolean addMajorCompact = false;

	public ArrayList<String> userList = new ArrayList<String>();

//    public HashMap<String,String> dateDataTypeMapping = new HashMap<String,String>();
//    public HashMap<String,String> decimalDataTypeMapping = new HashMap<String,String>();
//    public HashMap<String,String> numericDataTypeMapping = new HashMap<String,String>();
//    public HashMap<String,String> timestampDataTypeMapping = new HashMap<String,String>();
	public HashMap<String, HashMap<String, DataTypeConversion>> dataTypeMapping = new HashMap<String, HashMap<String, DataTypeConversion>>(); // maps
																																				// all
																																				// other
																																				// data
																																				// type
																																				// coversions
																																				// in
																																				// the
																																				// config
																																				// file

	// Schema Name Mapping
	public HashMap<String, String> schemaNameMapping = new HashMap<String, String>();
	public HashMap<String, String> columnNameReplacements = new HashMap<String, String>();
	public boolean doubleQuoteColumnNames = false;

	/**
	 * Varialbe for sqoop
	 */
	public boolean createSqoopScripts = false;
	public String sqoopDirectory = "/home/splice/sqoop";
	public String sqoopFilesSubDirectory = "sqoop";
	public String badPathOnHDFS = "/bad";
	public String sqoopConfig = "/tmp/sqoop/config.txt";
	public String sqoopExtractScriptFileNameFormat = "extract-{SCHEMA}.txt";
	public String sqoopTableListFileNameFormat = "tables-{SCHEMA}.txt";
	public String sqoopImportDir = "/tmp/";
	public boolean createSqoopQueryFiles = false;
	public String sqoopQuerySubDirectory = "/query/";
	public String sqoopQueryFileNameFormat = "query-{SCHEMA}{TABLE}.sql";

	/*
	 * For the data export file the number of characters to use to pad the file name
	 */
	public int fileNumberPadding = 11;

	public boolean printListOfTables = false;
	public boolean printListOfTablesWithRecordCount = false;
	public boolean printSchemaStats = false;
	public boolean verbose = false;
	public boolean debug = false;

	/*
	 * Settings for processing the schemas and tables
	 */
	boolean processAllSchemas = false;
	ArrayList<String> processSchemaList = new ArrayList<String>();
	ArrayList<String> excludeSchemaList = new ArrayList<String>();
	HashMap<String, SchemaIncludeExclude> schemaInclusionExclusion = new HashMap<String, SchemaIncludeExclude>();

	public ArrayList<Table> tableList = new ArrayList<Table>();
	public ArrayList<String> schemaList = new ArrayList<String>();
	public HashMap<String, ArrayList<Table>> schemaTables = new HashMap<String, ArrayList<Table>>();

	public ArrayList<String> rolesToCreate = new ArrayList<String>();

	public boolean bUseSchema = true;

	BaseOutput outputClass = null;

	/**
	 * Processes the input parameters
	 * 
	 * @param args
	 * @return
	 * @throws ParseException
	 */
	public boolean processParameters(String[] args) throws ParseException {
		CommandLineParser parser = new PosixParser();
		Options options = new Options();

		options.addOption("configFile", true,
				"[required] Config xml file. Settings for the run will be read from this file.");

		options.addOption("connectionId", true,
				"[required] The database connection id from the config xml file that contains the source and target database details.");

		options.addOption("configId", true,
				"[required] The config id from the config xml file that contains the details for options for the run.");

		CommandLine argsLine = parser.parse(options, args);
		if (!argsLine.hasOption("configFile") || !argsLine.hasOption("connectionId")
				|| !argsLine.hasOption("configId")) {
			MigrateDatabaseUtils.writeMessage("Missing one of the required parameters");
			this.printUsage(options);
			return false;
		}

		configFileLocation = argsLine.getOptionValue("configFile");
		connectionId = argsLine.getOptionValue("connectionId");
		configId = argsLine.getOptionValue("configId");

		loadConfigFile(configFileLocation, connectionId, configId);

		if (debug) {
			this.printParameters();
		}

		return true;
	}

	/**
	 * This function reads in the configuration xml file and sets all the options
	 * for the runs.
	 * 
	 * @param configFileLocation
	 * @param connectionId
	 * @param configId
	 */
	public void loadConfigFile(String configFileLocation, String connectionId, String configId) {

		// loadConfigFile
		File xmlConfigFile = new File(configFileLocation);
		if (!xmlConfigFile.exists()) {
			MigrateDatabaseUtils.logError("Database vendor file not found:" + configFileLocation);
		}

		XMLConfiguration initialConfig;
		try {
			initialConfig = new XMLConfiguration(xmlConfigFile);
			initialConfig.setExpressionEngine(new XPathExpressionEngine());

			// Get the specified connection id
			SubnodeConfiguration connectionDef = initialConfig
					.configurationAt("connections/connection[@id='" + connectionId + "']");
			// config.getList("failoverServers.server.ipAddress");

			this.setDatabaseVendorFile(connectionDef.getString("databaseVendorFile"));
			this.setSourceJDBCURL(connectionDef.getString("sourceJdbcUrl"));
			this.setSourceUser(connectionDef.getString("sourceUser"));
			this.setSourcePassword(connectionDef.getString("sourcePassword"));
			this.setTargetJDBCURL(connectionDef.getString("targetJdbcUrl"));
			this.setTargetUser(connectionDef.getString("targetUser"));
			this.setTargetPassword(connectionDef.getString("targetPassword"));

			// Get the specified configuration id
			SubnodeConfiguration configDef = initialConfig.configurationAt("configs/config[@id='" + configId + "']");

			// Get the scripts output directory
			this.setScriptOutputPath(configDef.getString("scriptOutputPath"));

			// Process the debugOptions
			this.setLog(configDef.getString("debugOptions/log"));
			printListOfTables = configDef.getBoolean("debugOptions/printListOfTables", false);
			printListOfTablesWithRecordCount = configDef.getBoolean("debugOptions/printListOfTablesRecordCount", false);
			printSchemaStats = configDef.getBoolean("debugOptions/printDatabaseStats", false);

			// Process the createDDLOptions

			this.setCreateTableScript(configDef.getBoolean("createDDLOptions/createTable", false));
			if (this.isCreateTableScript()) {
				this.setCreateTableSubDirectory(
						configDef.getString("createDDLOptions/createTableSubDirectory", "/ddl/create/tables"));
				this.setUseUniqueIndexForMissingPrimary(
						configDef.getBoolean("createDDLOptions/useUniqueIndexForMissingPrimary", false));
				this.setPrimaryKeyUniqueIndexPrefix(
						configDef.getString("createDDLOptions/primaryKeyUniqueIndexPrefix", ""));
				this.setAddColumnDefault(configDef.getBoolean("createDDLOptions/addColumnDefaults", true));
			}

			this.setCreateCheckConstraints(configDef.getBoolean("createDDLOptions/createCheckConstraints", false));
			if (this.isCreateCheckConstraints()) {
				this.setCreateConstraintSubDirectory(
						configDef.getString("createDDLOptions/createConstraintSubDirectory", "/ddl/create"));
			}

			this.setCreateForeignKeys(configDef.getBoolean("createDDLOptions/createForeignKeys", false));
			if (this.isCreateForeignKeys()) {
				this.setCreateForeignKeysSubDirectory(
						configDef.getString("createDDLOptions/createForeignKeysSubDirectory", "/ddl/create/fkeys"));
			}

			this.setCreateIndexScript(configDef.getBoolean("createDDLOptions/createIndexes", false));
			if (this.isCreateIndexScript()) {
				this.setCreateIndexSubDirectory(
						configDef.getString("createDDLOptions/createIndexSubDirectory", "/ddl/create/indexes"));
			}

			this.setCreateUsers(configDef.getBoolean("createDDLOptions/createUsers", false));
			if (this.isCreateUsers()) {
				this.setCreateUserSubDirectory(
						configDef.getString("createDDLOptions/createUserSubDirectory", "/ddl/create"));
				this.setUsersToSkip(configDef.getString("createDDLOptions/skipUsers"));
			}
			this.setCreateSequenceScript(configDef.getBoolean("createDDLOptions/createSequence", false));
			if (this.isCreateSequenceScript()) {
				this.setCreateSequenceSubDirectory(
						configDef.getString("createDDLOptions/createSequenceSubDirectory", "/ddl/create/sequence"));
			}
			this.setDropTableScript(configDef.getBoolean("createDDLOptions/dropTables", false));
			if (this.isDropTableScript()) {
				this.setDropTableSubDirectory(
						configDef.getString("createDDLOptions/dropTableSubDirectory", "/ddl/drop/tables"));
			}

			this.setDropForeignKeysScript(configDef.getBoolean("createDDLOptions/dropForeignKeys", false));
			if (this.isCreateDropForeignKeys()) {
				this.setDropForeignKeysSubDirectory(
						configDef.getString("createDDLOptions/dropForeignKeysSubDirectory", "/ddl/drop/fkeys"));
			}

			this.setCreateDropIndexScript(configDef.getBoolean("createDDLOptions/dropIndexes", false));
			if (this.isCreateDropIndexScript()) {
				this.setDropIndexSubDirectory(
						configDef.getString("createDDLOptions/dropIndexSubDirectory", "/ddl/drop/indexes"));
			}

			this.setCreateDropTriggerScript(configDef.getBoolean("createDDLOptions/dropTriggers", false));
			if (this.isCreateDropTriggerScript()) {
				this.setDropTriggerSubDirectory(
						configDef.getString("createDDLOptions/dropTriggerSubDirectory", "/ddl/drop/triggers"));
			}

			this.setCreateDropSequenceScript(configDef.getBoolean("createDDLOptions/dropSequence", false));
			if (this.isCreateDropSequenceScript()) {
				this.setDropSequenceSubDirectory(
						configDef.getString("createDDLOptions/dropSequenceSubDirectory", "/ddl/drop/indexes"));
			}

			this.setCreateRoleScript(configDef.getBoolean("createDDLOptions/createRoles", false));
			if (this.isCreateRoleScript()) {
				this.setCreateRoleSubDirectory(
						configDef.getString("createDDLOptions/createRoleSubDirectory", "/ddl/create/roles"));
				List<Object> roles = configDef.getList("createDDLOptions/rolesToCreate/role");
				for (Object role : roles) {
					this.rolesToCreate.add(role.toString());
				}
			}

			this.setCreateGrantReadScript(configDef.getBoolean("createDDLOptions/createGrantRead", false));
			this.setCreateGrantWriteScript(configDef.getBoolean("createDDLOptions/createGrantWrite", false));
			this.setCreateGrantExecuteScript(configDef.getBoolean("createDDLOptions/createGrantExecute", false));
			if (this.isCreateGrantReadScript() || this.isCreateGrantWriteScript()
					|| this.isCreateGrantExecuteScript()) {
				this.setCreateGrantSubDirectory(configDef.getString("createDDLOptions/createGrantSubDirectory"));
				if (this.isCreateGrantReadScript()) {
					this.setRolesToGrantRead(configDef.getString("createDDLOptions/rolesToGrantRead"));
				}
				if (this.isCreateGrantWriteScript()) {
					this.setRolesToGrantWrite(configDef.getString("createDDLOptions/rolesToGrantWrite"));
				}
				if (this.isCreateGrantExecuteScript()) {
					this.setRolesToGrantExecute(configDef.getString("createDDLOptions/rolesToGrantExecute"));
				}
			}

			// pad varchar/char
			this.setPadVarchar(configDef.getBoolean("createDDLOptions/padVarcharColumns", false));
			if (this.isPadVarchar()) {
				this.setPadVarcharValue(configDef.getInteger("createDDLOptions/padVarcharColumnValue", 0));
			}
			this.setPadChar(configDef.getBoolean("createDDLOptions/padCharColumns", false));
			if (this.isPadChar()) {
				this.setPadCharValue(configDef.getInteger("createDDLOptions/padCharColumnValue", 0));
			}

			// Set File Formats
			this.setCreateTableFileFormat(
					configDef.getString("createDDLOptions/createTableFileFormat", "create-{SCHEMA}-tables.sql"));
			this.setDropTableFileFormat(
					configDef.getString("createDDLOptions/dropTableFileFormat", "drop-{SCHEMA}-tables.sql"));
			this.setCreateSequenceFileFormat(
					configDef.getString("createDDLOptions/createSequenceFileFormat", "create-{SCHEMA}-sequences.sql"));
			this.setDropSequenceFileFormat(
					configDef.getString("createDDLOptions/dropSequenceFileFormat", "drop-{SCHEMA}-sequences.sql"));
			this.setCreateIndexFileFormat(
					configDef.getString("createDDLOptions/createIndexFileFormat", "create-{SCHEMA}-indexes.sql"));
			this.setDropIndexFileFormat(
					configDef.getString("createDDLOptions/dropIndexFileFormat", "drop-{SCHEMA}-indexes.sql"));
			this.setCreateUniqueIndexFileFormat(configDef.getString("createDDLOptions/createUniqueIndexFileFormat",
					"create-{SCHEMA}-unique-indexes.sql"));
			this.setDropUniqueIndexFileFormat(configDef.getString("createDDLOptions/dropUniqueIndexFileFormat",
					"drop-{SCHEMA}-unique-indexes.sql"));
			this.setCreateFKeyFileFormat(
					configDef.getString("createDDLOptions/createFKeyFileFormat", "create-{SCHEMA}-fkeys.sql"));
			this.setDropFKeyFileFormat(
					configDef.getString("createDDLOptions/dropFKeyFileFormat", "drop-{SCHEMA}-fkeys.sql"));
			this.setCreateConstraintFileFormat(
					configDef.getString("createDDLOptions/createConstraintFileFormat", "create-{SCHEMA}-constraints.sql"));
			this.setImportForEachSchemaFileFormat(
					configDef.getString("spliceImport/importForEachSchemaFileFormat", "{SCHEMA}-import-tables.sql"));
			this.setImportForEachTableFileFormat(
					configDef.getString("spliceImport/importForEachTableFileFormat", "import-{SCHEMA}-{TABLE}.sql"));

			// import/export delimiter config
			this.setDelimiter(configDef.getString("importExportDataOptions/delimiter", ","));
			this.setCellDelimiter(configDef.getString("importExportDataOptions/cellDelimiter", "\""));

			// Set Export Data Options
			this.setExportData(configDef.getBoolean("exportDataOptions/exportData", false));
			this.setCompress(configDef.getBoolean("exportDataOptions/compress", false));
			this.setDataOutputType(configDef.getString("exportDataOptions/dataOutputType", "FS"));
			this.setDataOutputPath(configDef.getString("exportDataOptions/dataOutputPath", "/tmp"));
			this.setExportColumnNames(configDef.getBoolean("exportDataOptions/exportColumnNames", false));
			this.setMaxRecordsPerFile(configDef.getLong("exportDataOptions/maxRecordsPerFile", 10000000));
			this.setMaxRecordsPerTable(configDef.getLong("exportDataOptions/limitRecords", -1));

			// Set Export Objects Options
			this.setExportFunction(configDef.getBoolean("exportObjectOptions/exportFunction", false));
			if (this.isExportFunction()) {
				this.setExportAllFunctions(configDef.getBoolean("exportObjectOptions/exportAllFunctions", false));
				this.setFunctionList(configDef.getString("exportObjectOptions/functionList"));
				this.setExportFunctionDirectory(configDef.getString("exportObjectOptions/exportFunctionDirectory"));
			}
			this.setExportPackage(configDef.getBoolean("exportObjectOptions/exportPackage", false));
			if (this.isExportPackage()) {
				this.setExportAllPackages(configDef.getBoolean("exportObjectOptions/exportAllPackages", false));
				this.setPackageList(configDef.getString("exportObjectOptions/packageList"));
				this.setExportPackageDirectory(configDef.getString("exportObjectOptions/exportPackageDirectory"));
			}
			this.setExportProcedure(configDef.getBoolean("exportObjectOptions/exportProcedure", false));
			if (this.isExportProcedure()) {
				this.setExportAllProcedures(configDef.getBoolean("exportObjectOptions/exportAllProcedures", false));
				this.setProcedureList(configDef.getString("exportObjectOptions/procedureList"));
				this.setExportProcedureDirectory(configDef.getString("exportObjectOptions/exportProcedureDirectory"));
			}
			this.setExportViews(configDef.getBoolean("exportObjectOptions/exportViews", false));
			if (this.isExportViews()) {
				this.setExportAllViews(configDef.getBoolean("exportObjectOptions/exportAllViews", false));
				this.setViewList(configDef.getString("exportObjectOptions/viewList"));
				this.setExportViewsDirectory(configDef.getString("exportObjectOptions/exportViewsDirectory"));
			}
			this.setExportTriggers(configDef.getBoolean("exportObjectOptions/exportTriggers", false));
			if (this.isExportTriggers()) {
				this.setExportAllTriggers(configDef.getBoolean("exportObjectOptions/exportAllTriggers", false));
				this.setTriggerList(configDef.getString("exportObjectOptions/triggerList"));
				this.setExportTriggersDirectory(configDef.getString("exportObjectOptions/exportTriggersDirectory"));
			}
			this.setExportRoles(configDef.getBoolean("exportObjectOptions/exportRoles", false));

			// Set Splice Bulk Import ScriptOptions
			this.setCreateImportForEachSchema(configDef.getBoolean("spliceImport/createForEachSchema", false));
			this.setCreateImportForEachTable(configDef.getBoolean("spliceImport/createForEachTable", false));
			this.setImportScriptSubDirectory(configDef.getString("spliceImport/importScriptSubDirectory"));
			this.setImportPathOnHDFS(configDef.getString("spliceImport/importPathOnHDFS"));
			this.setTimestampFormat(configDef.getString("spliceImport/timestampFormat"));
			this.setDateFormat(configDef.getString("spliceImport/dateFormat"));
			this.setTimeFormat(configDef.getString("spliceImport/timeFormat"));
			this.setFailBadRecordCount(configDef.getString("spliceImport/failBadRecordCount"));
			this.setBadPathOnHDFS(configDef.getString("spliceImport/badPathOnHDFS", "/bad/"));
			this.setAddTruncate(configDef.getBoolean("spliceImport/addTruncate", false));
			this.setAddVacuum(configDef.getBoolean("spliceImport/addVacuum", false));
			this.setAddMajorCompact(configDef.getBoolean("spliceImport/addMajorCompact", false));

			// Set the Sqoop Script Options

			this.setCreateSqoopScripts(configDef.getBoolean("sqoopOptions/sqoopScripts", false));

			// Set Sqoop extract variables
			this.setSqoopConfigFile(configDef.getString("sqoopOptions/sqoopConfigFile"));
			this.setSqoopTableListPath(configDef.getString("sqoopOptions/sqoopTableListPath"));
			this.setSqoopImportPath(configDef.getString("sqoopOptions/sqoopImportPath"));
			this.setSqoopQueryPath(configDef.getString("sqoopOptions/sqoopQuerySubDirectory"));
			this.setSqoopLogPath(configDef.getString("sqoopOptions/sqoopLogPath"));

			this.setSqoopHadoopBin(configDef.getString("sqoopOptions/hadoopBin"));
			this.setSqoopSpliceBin(configDef.getString("sqoopOptions/spliceBin"));

			if (this.isCreateSqoopScripts()) {
				this.setSqoopDirectory(configDef.getString("sqoopOptions/sqoopDirectory"));
				this.setSqoopFilesSubDirectory(configDef.getString("sqoopOptions/sqoopFilesSubDirectory"));
				this.setSqoopConfig(configDef.getString("sqoopOptions/sqoopExportScript/configFile"));
				this.setSqoopExtractScriptFileNameFormat(
						configDef.getString("sqoopOptions/sqoopExportScript/extractScriptFileNameFormat"));
				this.setSqoopTableListFileNameFormat(
						configDef.getString("sqoopOptions/sqoopExportScript/tableListFileNameFormat"));
				this.setSqoopImportDir(configDef.getString("sqoopOptions/sqoopExportScript/importDir"));
				this.setCreateSqoopQueryFiles(configDef.getBoolean("sqoopOptions/sqoopCreateQueryFiles"));
				this.setSqoopQuerySubDirectory(configDef.getString("sqoopOptions/sqoopQuerySubDirectory"));
				if (this.isCreateSqoopQueryFiles()) {
					this.setSqoopQueryFileNameFormat(configDef.getString("sqoopOptions/sqoopQueryFileNameFormat"));
				}
			}

			// Process Schema / Table Options
			this.setProcessAllSchemas(configDef.getBoolean("schemas/processAllSchemas", false));
			if (!this.isProcessAllSchemas()) {
				this.setSchemaList(configDef.getList("schemas/processSchemas/includeSchemas/schemaName"));
			} else {
				this.setExcludeSchemaList(configDef.getList("schemas/processSchemas/excludeSchemas/schemaName"));
			}

			List<HierarchicalConfiguration> schemaMapping = configDef
					.configurationsAt("schemas/schemaNameMapping/schema");
			for (HierarchicalConfiguration schemaDef : schemaMapping) {
				schemaNameMapping.put(schemaDef.getString("@source"), schemaDef.getString("@target"));
			}

			List<HierarchicalConfiguration> columnNameMapping = configDef
					.configurationsAt("schemas/columnNameReplacements/replace");
			if (columnNameMapping.size() == 0) {
				// if list is empty, let's add a $ case for backward compatibility
				columnNameReplacements.put("$", "_");

			} else {
				for (HierarchicalConfiguration replacementDef : columnNameMapping) {
					columnNameReplacements.put(replacementDef.getString("@source"),
							replacementDef.getString("@target"));
				}
			}

			this.setDoubleQuoteColumnNames(configDef.getBoolean("schemas/doubleQuoteColumnNames", false));

			List<HierarchicalConfiguration> schemas = configDef.configurationsAt("schemas/inclusionsExclusions/schema");
			for (HierarchicalConfiguration schema : schemas) {
				String schemaName = schema.getString("@name");
				List<Object> include = schema.getList("tablesToInclude/table");
				List<Object> exclude = schema.getList("tablesToExclude/table");

				SchemaIncludeExclude entry = schemaInclusionExclusion.get(schemaName);
				if (entry == null) {
					entry = new SchemaIncludeExclude(schemaName, include, exclude);
				} else {
					entry.updateIncludeExclude(include, exclude);
				}
				schemaInclusionExclusion.put(schemaName, entry);
			}

			List<HierarchicalConfiguration> dataTypes = configDef.configurationsAt("dataTypeMapping/dataType");
			for (HierarchicalConfiguration datatype : dataTypes) {

				String dataTypeName = datatype.getString("@name");
				List<HierarchicalConfiguration> columns = datatype.configurationsAt("column");
				processDataTypeList(dataTypeName, columns, dataTypeMapping);
			}

		} catch (ConfigurationException e) {
			e.printStackTrace();
			MigrateDatabaseUtils.logError("Exception: ", e);
		}
	}

	public void processDataTypeList(String sourceTypeName, List<HierarchicalConfiguration> columns,
			HashMap<String, HashMap<String, DataTypeConversion>> dataTypeMapping) {
		if (columns == null || columns.size() == 0) {
			return;
		}
		HashMap<String, DataTypeConversion> columnConversions;
		if (dataTypeMapping.containsKey(sourceTypeName)) {
			columnConversions = dataTypeMapping.get(sourceTypeName);
		} else {
			columnConversions = new HashMap<String, DataTypeConversion>();
			dataTypeMapping.put(sourceTypeName.toUpperCase(), columnConversions);
		}

		for (HierarchicalConfiguration c : columns) {
			ConfigurationNode row = c.getRootNode();

			String schemaName = row.getAttributes("schema").get(0).getValue().toString();
			String tableName = row.getAttributes("table").get(0).getValue().toString();
			String columnName = row.getAttributes("column").get(0).getValue().toString();

			DataTypeConversion conversion = new DataTypeConversion();
			conversion.setTargetDataType(row.getAttributes("dataType").get(0).getValue().toString());
			if (row.getAttributes("convert").size() > 0) {
				conversion.setConversionExpression(row.getAttributes("convert").get(0).getValue().toString());

			}

			columnConversions.put(schemaName + "." + tableName + "." + columnName, conversion);
		}
	}

	public DataTypeConversion getDataTypeConversion(String dataTypeName, String schemaName, String tableName,
			String columnName) {
		DataTypeConversion rtnVal = null;
		if (dataTypeName != null) {
			HashMap<String, DataTypeConversion> dataMap = dataTypeMapping.get(dataTypeName.toUpperCase());

			if (dataMap != null) {
				rtnVal = dataMap.get(schemaName + "." + tableName + "." + columnName);
				if (rtnVal == null) {
					rtnVal = dataMap.get(schemaName + ".*." + columnName);
					if (rtnVal == null) {
						rtnVal = dataMap.get("*.*." + columnName);
					}
					if (rtnVal == null) {
						rtnVal = dataMap.get("*.*.*");
					}
				}
			}

		}
		return rtnVal;
	}

	private void setDropForeignKeysScript(boolean boolean1) {
		// TODO Auto-generated method stub

	}

	public void setLog(String log) {
		if (log != null && log.length() > 0) {
			if (log.equals("DEBUG")) {
				setDebug(true);
			} else if (log.equals("VERBOSE")) {
				setDebug(true);
				setVerbose(true);
			}
			return;
		}

	}

	/**
	 * Sets the database driver class
	 * 
	 * @param driverClass
	 */
	public void setDriverClass(BaseDatabaseVendor dbVendor, String driverClass) {

		String driverClassName = driverClass;
		try {
			if (dbVendor != null) {
				driverClassName = dbVendor.getDriverClass();
			}
			Class.forName(driverClassName).newInstance();
		} catch (Exception ex) {
			MigrateDatabaseUtils.logError("Error loading driver class" + driverClassName, ex);
			System.exit(-1);
		}
	}

	/*
	 * BEGIN GETTERS / SETTERS
	 */

	public void setDatabaseVendorFile(String s) {
		if (s != null) {
			this.databaseVendorFile = s;
		}
	}

	public Connection getSourceDatabaseConnection() throws SQLException {
		if (sourceDatabaseConnection == null) {
			sourceDatabaseConnection = getConnection(sourceDatabaseVendor, sourceJDBCUrl, sourceUser, sourcePassword);
		}
		return sourceDatabaseConnection;
	}

	public void closeSourceDatabaseConnection() throws SQLException {
		if (sourceDatabaseConnection != null) {
			sourceDatabaseConnection.close();
		}
	}

	public DatabaseMetaData getSourceMetaData() throws SQLException {
		return getSourceDatabaseConnection().getMetaData();
	}

	public void setSourceJDBCURL(String url) {
		if (url != null) {
			this.sourceJDBCUrl = url;
			if (this.sourceJDBCUrl != null && this.sourceJDBCUrl.length() > 5) {
				String prefix = this.sourceJDBCUrl.substring(5);
				int inx = prefix.indexOf(":");
				if (inx > -1) {
					prefix = prefix.substring(0, inx);
					sourceDatabaseVendor = BaseDatabaseVendor.getDatabaseVendorClass(prefix, databaseVendorFile);
					setSourceJDBCDriver(sourceDatabaseVendor.driverClass);
				}
			}
		}
	}

	public void setDoubleQuoteColumnNames(boolean useDoubleQuotedColumnNames) {
		this.doubleQuoteColumnNames = useDoubleQuotedColumnNames;

	}

	public String getSourceJDBCURL() {
		return this.sourceJDBCUrl;
	}

	public String getSourceJDBCDriver() {
		return this.sourceJDBCDriver;
	}

	public void setSourceJDBCDriver(String s) {
		if (s != null)
			this.sourceJDBCDriver = s;
	}

	public String getSourceUser() {
		return this.sourceUser;
	}

	public void setSourceUser(String s) {
		if (s != null)
			this.sourceUser = s;
	}

	public String getSourcePassword() {
		return this.sourcePassword;
	}

	public void setSourcePassword(String s) {
		if (s != null)
			this.sourcePassword = s;
	}

	public void setTargetJDBCURL(String url) {
		if (url == null)
			return;
		this.targetJDBCUrl = url;
		if (this.targetJDBCUrl != null && this.targetJDBCUrl.length() > 5) {
			String prefix = this.targetJDBCUrl.substring(5);
			int inx = prefix.indexOf(":");
			if (inx > -1) {
				prefix = prefix.substring(0, inx);
				targetDatabaseVendor = BaseDatabaseVendor.getDatabaseVendorClass(prefix, databaseVendorFile);
				targetJDBCDriver = targetDatabaseVendor.driverClass;
			}
		}
	}

	public String getTargetJDBCURL() {
		return this.targetJDBCUrl;
	}

	public String getTargetJDBCDriver() {
		return this.targetJDBCDriver;
	}

	public void setTargetJDBCDriver(String s) {
		if (s != null)
			this.targetJDBCDriver = s;
	}

	public String getTargetUser() {
		return this.targetUser;
	}

	public void setTargetUser(String s) {
		if (s != null)
			this.targetUser = s;
	}

	public String getTargetPassword() {
		return this.targetPassword;
	}

	public void setTargetPassword(String s) {
		if (s != null)
			this.targetPassword = s;
	}

	public String getScriptOutputPath() {
		return this.outputPath;
	}

	public void setScriptOutputPath(String s) {
		if (s == null)
			return;
		if (!s.endsWith("/")) {
			s = s + "/";
		}
		File temp = new File(s);
		if (!temp.exists()) {
			temp.mkdirs();
		}
		this.outputPath = s;
	}

	public String getSqoopConfigFile() {
		return this.sqoopConfigFile;
	}

	public void setSqoopConfigFile(String sqoopConfigFile) {
		this.sqoopConfigFile = sqoopConfigFile;
	}

	public String getSqoopTableListPath() {
		return this.sqoopTableListPath;
	}

	public void setSqoopTableListPath(String sqoopTableListPath) {
		this.sqoopTableListPath = sqoopTableListPath;
	}

	public String getSqoopImportPath() {
		return this.sqoopImportPath;
	}

	public void setSqoopImportPath(String sqoopImportPath) {
		this.sqoopImportPath = sqoopImportPath;
	}

	public String getSqoopQueryPath() {
		return this.sqoopQueryPath;
	}

	public void setSqoopQueryPath(String sqoopQueryPath) {
		this.sqoopQueryPath = sqoopQueryPath;
	}

	public String getSqoopLogPath() {
		return this.sqoopLogPath;
	}

	public void setSqoopLogPath(String sqoopLogPath) {
		this.sqoopLogPath = sqoopLogPath;
	}

	public boolean isCreateTableScript() {
		return this.createTableScript;
	}

	public void setCreateTableScript(boolean b) {
		this.createTableScript = b;
	}

	public boolean isDropTableScript() {
		return this.createDropTableScript;
	}

	public void setDropTableScript(boolean b) {
		this.createDropTableScript = b;
	}

	public boolean isUseUniqueIndexForMissingPrimary() {
		return useUniqueIndexForMissingPrimary;
	}

	public void setUseUniqueIndexForMissingPrimary(boolean useUniqueIndexForMissingPrimary) {
		this.useUniqueIndexForMissingPrimary = useUniqueIndexForMissingPrimary;
	}

	public String getPrimaryKeyUniqueIndexPrefix() {
		return primaryKeyUniqueIndexPrefix;
	}

	public void setPrimaryKeyUniqueIndexPrefix(String primaryKeyUniqueIndexPrefix) {
		this.primaryKeyUniqueIndexPrefix = primaryKeyUniqueIndexPrefix;
	}

	public boolean isAddColumnDefault() {
		return addColumnDefault;
	}

	public void setAddColumnDefault(boolean b) {
		this.addColumnDefault = b;
	}

	public boolean isCreateIndexScript() {
		return this.createIndexScript;
	}

	public void setCreateIndexScript(boolean b) {
		this.createIndexScript = b;
	}

	public boolean isCreateSequenceScript() {
		return this.createSequenceScript;
	}

	public void setCreateSequenceScript(boolean b) {
		this.createSequenceScript = b;
	}

	public boolean isCreateRoleScript() {
		return this.createRoleScript;
	}

	public void setCreateRoleScript(boolean b) {
		this.createRoleScript = b;
	}

	public ArrayList getRolesToCreate() {
		return this.rolesToCreate;
	}

	public boolean isCreateGrantReadScript() {
		return this.createGrantReadScript;
	}

	public void setCreateGrantReadScript(boolean b) {
		this.createGrantReadScript = b;
	}

	public boolean isCreateGrantWriteScript() {
		return this.createGrantWriteScript;
	}

	public void setCreateGrantWriteScript(boolean b) {
		this.createGrantWriteScript = b;
	}

	public boolean isCreateGrantExecuteScript() {
		return this.createGrantExecuteScript;
	}

	public void setCreateGrantExecuteScript(boolean b) {
		this.createGrantExecuteScript = b;
	}

	public boolean isPadVarchar() {
		return this.padVarchar;
	}

	public void setPadVarchar(boolean b) {
		this.padVarchar = b;
	}

	public int getPadVarcharValue() {
		return this.padVarcharValue;
	}

	public void setPadVarcharValue(int i) {
		this.padVarcharValue = i;
	}

	public boolean isPadChar() {
		return this.padChar;
	}

	public void setPadChar(boolean b) {
		this.padChar = b;
	}

	public int getPadCharValue() {
		return this.padCharValue;
	}

	public void setPadCharValue(int i) {
		this.padCharValue = i;
	}

	public boolean getExportData() {
		return this.exportData;
	}

	public void setExportData(boolean b) {
		this.exportData = b;
	}

	public boolean getExportColumnNames() {
		return this.exportColumnNames;
	}

	public void setExportColumnNames(boolean b) {
		this.exportColumnNames = b;
	}

	public boolean getCompress() {
		return this.compress;
	}

	public void setCompress(boolean b) {
		this.compress = b;
	}

	public String getDataOutputType() {
		return this.exportOutputType;
	}

	public void setDataOutputType(String s) {
		this.exportOutputType = s;
	}

	public String getDataOutputSubdirectory() {
		return this.exportOutputPath;
	}

	public void setDataOutputPath(String s) {
		if (s == null)
			return;
		if (!s.endsWith("/")) {
			s = s + "/";
		}
		File temp = new File(s);
		if (!temp.exists()) {
			temp.mkdirs();
		}

		this.exportOutputPath = s;
	}

	public String getDelimiter() {
		return this.delimiter;
	}

	public void setDelimiter(String s) {
		this.delimiter = addressUnicode(s);
	}

	private String addressUnicode(String s) {
		String result = s;

		if (s != null && s.startsWith("u")) {
			char unicodeChar = (char) Integer.parseInt(s.substring(1));
			result = Character.toString(unicodeChar);
		}
		return result;
	}

	public String getCellDelimiter() {
		return this.cellDelimiter;
	}

	public void setCellDelimiter(String s) {
		if (s == null)
			return;
		this.cellDelimiter = addressUnicode(s);
	}

	public long getMaxRecordsPerTable() {
		return this.maxRecordsPerTable;
	}

	public void setMaxRecordsPerTable(long i) {
		this.maxRecordsPerTable = i + 1;
	}

	public long getMaxRecordsPerFile() {
		return this.maxRecordsPerFile;
	}

	public void setMaxRecordsPerFile(long i) {
		this.maxRecordsPerFile = i;
	}

	public void setSchemaList(List<Object> s) {
		int numRcds = s == null ? 0 : s.size();
		for (int i = 0; i < numRcds; i++) {
			schemaList.add(s.get(i).toString());
		}
	}

	public void setExcludeSchemaList(List<Object> s) {
		int numRcds = s == null ? 0 : s.size();
		for (int i = 0; i < numRcds; i++) {
			excludeSchemaList.add(s.get(i).toString());
		}
	}

	public void setTableList(String s) {
		if (s != null) {
			String[] tables = StringUtils.split(s, ",");
			int numTables = tables.length;
			for (int i = 0; i < numTables; i++) {
				String[] tableDetails = StringUtils.split(tables[i], ".");
				if (tableDetails.length > 1) {
					Table tab = new Table(tableDetails[0], tableDetails[1]);
					tableList.add(tab);
					if (!schemaList.contains(tableDetails[0])) {
						schemaList.add(tableDetails[0]);
					}

					ArrayList<Table> temp = schemaTables.get(tableDetails[0]);
					if (temp == null) {
						temp = new ArrayList<Table>();
					}
					temp.add(tab);
					schemaTables.put(tableDetails[0], temp);

				} else {
					tableList.add(new Table(tables[i]));
				}
			}
		}
	}

	public boolean getVerbose() {
		return verbose;
	}

	public void setVerbose(boolean b) {
		verbose = b;
	}

	public boolean getDebug() {
		return debug;
	}

	public void setDebug(boolean b) {
		debug = b;
	}

	public int getFileNumberPadding() {
		return this.fileNumberPadding;
	}

	/**
	 * Returns a database connection
	 * 
	 * @param dbVendor
	 * @param jdbcConnectionString
	 * @param user
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(BaseDatabaseVendor dbVendor, String jdbcConnectionString, String user,
			String password) throws SQLException {
		if (dbVendor != null) {
			return dbVendor.getConnection(jdbcConnectionString, user, password);
		} else if (user == null || password == null) {
			return DriverManager.getConnection(jdbcConnectionString);
		} else {
			return DriverManager.getConnection(jdbcConnectionString, user, password);
		}
	}

	public Connection getTargetDatabaseConnection() throws SQLException {
		if (targetDatabaseConnection == null) {
			targetDatabaseConnection = getConnection(targetDatabaseVendor, targetJDBCUrl, targetUser, targetPassword);
		}
		return targetDatabaseConnection;
	}

	public void closeTargetDatabaseConnection() throws SQLException {
		if (targetDatabaseConnection != null) {
			targetDatabaseConnection.close();
		}
	}

	public BaseOutput getTargetOutput() {
		if (outputClass == null) {
			String sClassName = "com.splicemachine.cs.databasemigration.output.FileOutput";
			if (directConnection) {
				sClassName = "com.splicemachine.cs.databasemigration.output.SpliceOutput";
			}
			try {
				Class<BaseOutput> c = (Class<BaseOutput>) Class.forName(sClassName);
				Constructor<?> cons = c.getConstructor(MigrateDatabaseConfig.class);
				outputClass = (BaseOutput) cons.newInstance(this);
			} catch (Exception e) {
				MigrateDatabaseUtils.logError(
						"Exception in the getDatabaseVendor method - problem instantiating database class name:"
								+ sClassName,
						e);
			}
		}
		return outputClass;
	}

	public boolean isCreateSqoopScripts() {
		return createSqoopScripts;
	}

	public void setCreateSqoopScripts(boolean createSqoopScripts) {
		this.createSqoopScripts = createSqoopScripts;
	}

	public boolean isCreateSqoopQueryFiles() {
		return createSqoopQueryFiles;
	}

	public void setCreateSqoopQueryFiles(boolean createSqoopQueryFiles) {
		this.createSqoopQueryFiles = createSqoopQueryFiles;
	}

	public String getSqoopQuerySubDirectory() {
		return sqoopQuerySubDirectory;
	}

	public void setSqoopQuerySubDirectory(String sqoopQuerySubDirectory) {
		this.sqoopQuerySubDirectory = sqoopQuerySubDirectory;
	}

	public String getCreateTableSubDirectory() {
		return createTableSubDirectory;
	}

	public void setCreateTableSubDirectory(String createTableSubDirectory) {
		this.createTableSubDirectory = formatSubDirectory(createTableSubDirectory);
	}

	public String getCreateForeignKeysSubDirectory() {
		return createForeignKeysSubDirectory;
	}

	public void setCreateForeignKeysSubDirectory(String createForeignKeysSubDirectory) {
		this.createForeignKeysSubDirectory = formatSubDirectory(createForeignKeysSubDirectory);
	}

	public String getCreateIndexSubDirectory() {
		return createIndexSubDirectory;
	}

	public void setCreateIndexSubDirectory(String createIndexSubDirectory) {
		this.createIndexSubDirectory = formatSubDirectory(createIndexSubDirectory);
	}

	public String getCreateSequenceSubDirectory() {
		return createSequenceSubDirectory;
	}

	public void setCreateSequenceSubDirectory(String createSequenceSubDirectory) {
		this.createSequenceSubDirectory = formatSubDirectory(createSequenceSubDirectory);
	}

	public String getDropTableSubDirectory() {
		return dropTableSubDirectory;
	}

	public void setDropTableSubDirectory(String dropTableSubDirectory) {
		this.dropTableSubDirectory = formatSubDirectory(dropTableSubDirectory);
	}

	public String getDropForeignKeysSubDirectory() {
		return dropForeignKeysSubDirectory;
	}

	public void setDropForeignKeysSubDirectory(String dropForeignKeysSubDirectory) {
		this.dropForeignKeysSubDirectory = formatSubDirectory(dropForeignKeysSubDirectory);
	}

	public String getDropIndexSubDirectory() {
		return dropIndexSubDirectory;
	}

	public void setDropIndexSubDirectory(String dropIndexSubDirectory) {
		this.dropIndexSubDirectory = formatSubDirectory(dropIndexSubDirectory);
	}

	public String getDropSequenceSubDirectory() {
		return dropSequenceSubDirectory;
	}

	public void setDropSequenceSubDirectory(String dropSequenceSubDirectory) {
		this.dropSequenceSubDirectory = formatSubDirectory(dropSequenceSubDirectory);
	}

	public String getCreateRoleSubDirectory() {
		return createRoleSubDirectory;
	}

	public void setCreateRoleSubDirectory(String createRoleSubDirectory) {
		this.createRoleSubDirectory = formatSubDirectory(createRoleSubDirectory);
	}

	public String getCreateGrantSubDirectory() {
		return createGrantSubDirectory;
	}

	public void setCreateGrantSubDirectory(String createGrantSubDirectory) {
		this.createGrantSubDirectory = formatSubDirectory(createGrantSubDirectory);
	}

	public String getRolesToGrantRead() {
		return rolesToGrantRead;
	}

	public void setRolesToGrantRead(String rolesToGrantRead) {
		this.rolesToGrantRead = rolesToGrantRead;
	}

	public String getRolesToGrantWrite() {
		return rolesToGrantWrite;
	}

	public void setRolesToGrantWrite(String rolesToGrantWrite) {
		this.rolesToGrantWrite = rolesToGrantWrite;
	}

	public String getRolesToGrantExecute() {
		return rolesToGrantExecute;
	}

	public void setRolesToGrantExecute(String rolesToGrantExecute) {
		this.rolesToGrantExecute = rolesToGrantExecute;
	}

	public boolean isCreateDropTableScript() {
		return createDropTableScript;
	}

	public void setCreateDropTableScript(boolean createDropTableScript) {
		this.createDropTableScript = createDropTableScript;
	}

	public boolean isCreateDropSequenceScript() {
		return createDropSequenceScript;
	}

	public void setCreateDropSequenceScript(boolean createDropSequenceScript) {
		this.createDropSequenceScript = createDropSequenceScript;
	}

	public boolean isCreateDropIndexScript() {
		return createDropIndexScript;
	}

	public void setCreateDropIndexScript(boolean createDropIndexScript) {
		this.createDropIndexScript = createDropIndexScript;
	}

	public boolean isCreateDropForeignKeys() {
		return createDropForeignKeys;
	}

	public void setCreateDropForeignKeys(boolean createDropForeignKeys) {
		this.createDropForeignKeys = createDropForeignKeys;
	}

	public boolean isCreateCheckConstraints() {
		return createCheckConstraints;
	}

	public void setCreateCheckConstraints(boolean exportCheckConstraints) {
		this.createCheckConstraints = exportCheckConstraints;
	}

	public String getExportOutputType() {
		return exportOutputType;
	}

	public void setExportOutputType(String exportOutputType) {
		this.exportOutputType = exportOutputType;
	}

	public String getExportOutputPath() {
		return exportOutputPath;
	}

	public void setExportOutputPath(String exportOutputPath) {
		this.exportOutputPath = exportOutputPath;
	}

	public boolean isCreateUsers() {
		return createUsers;
	}

	public void setCreateUsers(boolean createUsers) {
		this.createUsers = createUsers;
	}

	public String getCreateUserSubDirectory() {
		return createUserSubDirectory;
	}

	public void setCreateUserSubDirectory(String createUserSubDirectory) {
		this.createUserSubDirectory = formatSubDirectory(createUserSubDirectory);
	}

	public String getCreateConstraintSubDirectory() {
		return createConstraintSubDirectory;
	}

	public void setCreateConstraintSubDirectory(String createConstraintSubDirectory) {
		this.createConstraintSubDirectory = createConstraintSubDirectory;
	}

	public ArrayList<String> getUsersToSkip() {
		return usersToSkip;
	}

	public void setUsersToSkip(ArrayList<String> usersToSkip) {
		this.usersToSkip = usersToSkip;
	}

	public void setUsersToSkip(String usersToSkip) {
		if (usersToSkip != null && usersToSkip.length() > 0) {
			this.usersToSkip = new ArrayList<String>(Arrays.asList(usersToSkip.split(",")));
		}
	}

	public String getProcedureList() {
		return procedureList;
	}

	public void setProcedureList(String procedureList) {
		this.procedureList = procedureList;
	}

	public String getPackageList() {
		return packageList;
	}

	public void setPackageList(String packageList) {
		this.packageList = packageList;
	}

	public boolean isExportProcedure() {
		return exportProcedure;
	}

	public void setExportProcedure(boolean exportProcedure) {
		this.exportProcedure = exportProcedure;
	}

	public boolean isExportPackage() {
		return exportPackage;
	}

	public void setExportPackage(boolean exportPackage) {
		this.exportPackage = exportPackage;
	}

	public boolean isExportFunction() {
		return exportFunction;
	}

	public void setExportFunction(boolean exportFunction) {
		this.exportFunction = exportFunction;
	}

	public String getViewList() {
		return viewList;
	}

	public void setViewList(String viewList) {
		this.viewList = viewList;
	}

	public String getFunctionList() {
		return functionList;
	}

	public void setFunctionList(String functionList) {
		this.functionList = functionList;
	}

	public boolean isCreateImportForEachTable() {
		return createImportForEachTable;
	}

	public void setCreateImportForEachTable(boolean createImportForEachTable) {
		this.createImportForEachTable = createImportForEachTable;
	}

	public String getImportScriptSubDirectory() {
		return importScriptSubDirectory;
	}

	public void setImportScriptSubDirectory(String importScriptSubDirectory) {
		this.importScriptSubDirectory = formatSubDirectory(importScriptSubDirectory);
	}

	public String getImportPathOnHDFS() {
		return importPathOnHDFS;
	}

	public void setImportPathOnHDFS(String importPathOnHDFS) {
		this.importPathOnHDFS = importPathOnHDFS;
	}

	public String getTimestampFormat() {
		return timestampFormat;
	}

	public void setTimestampFormat(String timestampFormat) {
		this.timestampFormat = timestampFormat;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getTimeFormat() {
		return timeFormat;
	}

	public void setTimeFormat(String timeFormat) {
		this.timeFormat = timeFormat;
	}

	public int getFailBadRecordCount() {
		return failBadRecordCount;
	}

	public void setFailBadRecordCount(String failBadRecordCount) {
		if (failBadRecordCount != null && failBadRecordCount.length() > 0) {
			this.failBadRecordCount = Integer.parseInt(failBadRecordCount);
		}
	}

	public boolean isExportRoles() {
		return exportRoles;
	}

	public void setExportRoles(boolean exportRoles) {
		this.exportRoles = exportRoles;
	}

	public String getSqoopDirectory() {
		return sqoopDirectory;
	}

	public void setSqoopDirectory(String sqoopDirectory) {
		this.sqoopDirectory = formatSubDirectory(sqoopDirectory);
	}

	public String getSqoopFilesSubDirectory() {
		return sqoopFilesSubDirectory;
	}

	public void setSqoopFilesSubDirectory(String sqoopFilesSubDirectory) {
		this.sqoopFilesSubDirectory = formatSubDirectory(sqoopFilesSubDirectory);
	}

	public String getBadPathOnHDFS() {
		return badPathOnHDFS;
	}

	public void setBadPathOnHDFS(String badPathOnHDFS) {
		this.badPathOnHDFS = badPathOnHDFS;
	}

	public boolean isAddTruncate() {
		return addTruncate;
	}

	public void setAddTruncate(boolean addTruncate) {
		this.addTruncate = addTruncate;
	}

	public boolean isAddVacuum() {
		return addVacuum;
	}

	public void setAddVacuum(boolean addVacuum) {
		this.addVacuum = addVacuum;
	}

	public boolean isAddMajorCompact() {
		return addMajorCompact;
	}

	public void setAddMajorCompact(boolean addMajorCompact) {
		this.addMajorCompact = addMajorCompact;
	}

	public String getSqoopConfig() {
		return sqoopConfig;
	}

	public void setSqoopConfig(String sqoopConfig) {
		this.sqoopConfig = sqoopConfig;
	}

	public String getSqoopExtractScriptFileNameFormat() {
		return sqoopExtractScriptFileNameFormat;
	}

	public void setSqoopExtractScriptFileNameFormat(String sqoopExtractScriptFileNameFormat) {
		this.sqoopExtractScriptFileNameFormat = sqoopExtractScriptFileNameFormat;
	}

	public String getSqoopTableListFileNameFormat() {
		return sqoopTableListFileNameFormat;
	}

	public void setSqoopTableListFileNameFormat(String sqoopTableListFileNameFormat) {
		this.sqoopTableListFileNameFormat = sqoopTableListFileNameFormat;
	}

	public String getSqoopImportDir() {
		return sqoopImportDir;
	}

	public void setSqoopImportDir(String sqoopImportDir) {
		this.sqoopImportDir = sqoopImportDir;
	}

	public String getSqoopQueryFileNameFormat() {
		return sqoopQueryFileNameFormat;
	}

	public void setSqoopQueryFileNameFormat(String sqoopQueryFileNameFormat) {
		this.sqoopQueryFileNameFormat = sqoopQueryFileNameFormat;
	}

	public boolean isProcessAllSchemas() {
		return processAllSchemas;
	}

	public void setProcessAllSchemas(boolean processAllSchemas) {
		this.processAllSchemas = processAllSchemas;
	}

	public ArrayList<String> getProcessSchemaList() {
		return processSchemaList;
	}

	public void setProcessSchemaList(ArrayList<String> processSchemaList) {
		this.processSchemaList = processSchemaList;
	}

	public HashMap<String, SchemaIncludeExclude> getSchemaInclusionExclusion() {
		return schemaInclusionExclusion;
	}

	public void setSchemaInclusionExclusion(HashMap<String, SchemaIncludeExclude> schemaInclusionExclusion) {
		this.schemaInclusionExclusion = schemaInclusionExclusion;
	}

	public HashMap<String, SchemaIncludeExclude> getSchemaIncludeExclude() {
		return this.schemaInclusionExclusion;
	}

	public ArrayList<String> getSchemaInclusions(String schema) {
		SchemaIncludeExclude entity = schemaInclusionExclusion.get(schema);
		if (entity == null) {
			return null;
		}
		return entity.getInclude();
	}

	public ArrayList<String> getSchemaExclusions(String schema) {
		SchemaIncludeExclude entity = schemaInclusionExclusion.get(schema);
		if (entity == null) {
			return null;
		}
		return entity.getExclude();
	}

	public boolean isCreateImportForEachSchema() {
		return createImportForEachSchema;
	}

	public void setCreateImportForEachSchema(boolean createImportForEachSchema) {
		this.createImportForEachSchema = createImportForEachSchema;
	}

	public boolean isCreateDropTriggerScript() {
		return createDropTriggerScript;
	}

	public void setCreateDropTriggerScript(boolean createDropTriggerScript) {
		this.createDropTriggerScript = createDropTriggerScript;
	}

	public String getDropTriggerSubDirectory() {
		return dropTriggerSubDirectory;
	}

	public void setDropTriggerSubDirectory(String dropTriggerSubDirectory) {
		this.dropTriggerSubDirectory = dropTriggerSubDirectory;
	}

	public String getExportProcedureDirectory() {
		return exportProcedureDirectory;
	}

	public void setExportProcedureDirectory(String exportProcedureDirectory) {
		this.exportProcedureDirectory = exportProcedureDirectory;
	}

	public boolean isExportAllProcedures() {
		return exportAllProcedures;
	}

	public void setExportAllProcedures(boolean exportAllProcedures) {
		this.exportAllProcedures = exportAllProcedures;
	}

	public String getExportPackageDirectory() {
		return exportPackageDirectory;
	}

	public void setExportPackageDirectory(String exportPackageDirectory) {
		this.exportPackageDirectory = exportPackageDirectory;
	}

	public boolean isExportAllPackages() {
		return exportAllPackages;
	}

	public void setExportAllPackages(boolean exportAllPackages) {
		this.exportAllPackages = exportAllPackages;
	}

	public String getExportFunctionDirectory() {
		return exportFunctionDirectory;
	}

	public void setExportFunctionDirectory(String exportFunctionDirectory) {
		this.exportFunctionDirectory = exportFunctionDirectory;
	}

	public boolean isExportAllFunctions() {
		return exportAllFunctions;
	}

	public void setExportAllFunctions(boolean exportAllFunctions) {
		this.exportAllFunctions = exportAllFunctions;
	}

	public String getExportTriggersDirectory() {
		return exportTriggersDirectory;
	}

	public void setExportTriggersDirectory(String exportTriggersDirectory) {
		this.exportTriggersDirectory = exportTriggersDirectory;
	}

	public boolean isExportTriggers() {
		return exportTriggers;
	}

	public void setExportTriggers(boolean exportTriggers) {
		this.exportTriggers = exportTriggers;
	}

	public boolean isExportAllTriggers() {
		return exportAllTriggers;
	}

	public void setExportAllTriggers(boolean exportAllTriggers) {
		this.exportAllTriggers = exportAllTriggers;
	}

	public String getTriggerList() {
		return triggerList;
	}

	public void setTriggerList(String triggerList) {
		this.triggerList = triggerList;
	}

	public String getExportViewsDirectory() {
		return exportViewsDirectory;
	}

	public void setExportViewsDirectory(String exportViewsDirectory) {
		this.exportViewsDirectory = exportViewsDirectory;
	}

	public boolean isExportViews() {
		return exportViews;
	}

	public void setExportViews(boolean exportViews) {
		this.exportViews = exportViews;
	}

	public boolean isExportAllViews() {
		return exportAllViews;
	}

	public void setExportAllViews(boolean exportAllViews) {
		this.exportAllViews = exportAllViews;
	}

	public ArrayList<String> getUserList() {
		return userList;
	}

	public void setUserList(ArrayList<String> userList) {
		this.userList = userList;
	}

	public void setFailBadRecordCount(int failBadRecordCount) {
		this.failBadRecordCount = failBadRecordCount;
	}

	public boolean isCreateForeignKeys() {
		return createForeignKeys;
	}

	public void setCreateForeignKeys(boolean createForeignKeys) {
		this.createForeignKeys = createForeignKeys;
	}

	/*
	 * END GETTERS / SETTERS
	 */

	public String formatSubDirectory(String directoryName) {

		if (directoryName != null && directoryName.length() > 0) {
			if (!directoryName.startsWith("/")) {
				directoryName = "/" + directoryName;
			}
			if (directoryName.endsWith("/")) {
				directoryName = directoryName.substring(0, directoryName.length() - 1);
			}
		}
		return directoryName;
	}

	public boolean isSchemaAUser(String schema) {

		if (schema != null) {
			if (this.getUserList() != null) {
				return this.getUserList().contains(schema);
			}
		}

		return false;
	}

	public String getSqoopHadoopBin() {
		return hadoopBin;
	}

	public void setSqoopHadoopBin(String hadoopBin) {
		this.hadoopBin = hadoopBin;
	}

	public String getSqoopSpliceBin() {
		return spliceBin;
	}

	public void setSqoopSpliceBin(String spliceBin) {
		this.spliceBin = spliceBin;
	}

	public String getCreateTableFileFormat() {
		return createTableFileFormat;
	}

	public void setCreateTableFileFormat(String createTableFileFormat) {
		this.createTableFileFormat = createTableFileFormat;
	}

	public String getDropTableFileFormat() {
		return dropTableFileFormat;
	}

	public void setDropTableFileFormat(String dropTableFileFormat) {
		this.dropTableFileFormat = dropTableFileFormat;
	}

	public String getCreateSequenceFileFormat() {
		return createSequenceFileFormat;
	}

	public void setCreateSequenceFileFormat(String createSequenceFileFormat) {
		this.createSequenceFileFormat = createSequenceFileFormat;
	}

	public String getDropSequenceFileFormat() {
		return dropSequenceFileFormat;
	}

	public void setDropSequenceFileFormat(String dropSequenceFileFormat) {
		this.dropSequenceFileFormat = dropSequenceFileFormat;
	}

	public String getCreateIndexFileFormat() {
		return createIndexFileFormat;
	}

	public void setCreateIndexFileFormat(String createIndexFileFormat) {
		this.createIndexFileFormat = createIndexFileFormat;
	}

	public String getDropIndexFileFormat() {
		return dropIndexFileFormat;
	}

	public void setDropIndexFileFormat(String dropIndexFileFormat) {
		this.dropIndexFileFormat = dropIndexFileFormat;
	}

	public String getCreateUniqueIndexFileFormat() {
		return createUniqueIndexFileFormat;
	}

	public void setCreateUniqueIndexFileFormat(String createUniqueIndexFileFormat) {
		this.createUniqueIndexFileFormat = createUniqueIndexFileFormat;
	}

	public String getDropUniqueIndexFileFormat() {
		return dropUniqueIndexFileFormat;
	}

	public void setDropUniqueIndexFileFormat(String dropUniqueIndexFileFormat) {
		this.dropUniqueIndexFileFormat = dropUniqueIndexFileFormat;
	}

	public String getCreateFKeyFileFormat() {
		return createFKeyFileFormat;
	}

	public void setCreateFKeyFileFormat(String createFKeyFileFormat) {
		this.createFKeyFileFormat = createFKeyFileFormat;
	}

	public String getDropFKeyFileFormat() {
		return dropFKeyFileFormat;
	}

	public void setDropFKeyFileFormat(String dropFKeyFileFormat) {
		this.dropFKeyFileFormat = dropFKeyFileFormat;
	}

	public String getCreateConstraintFileFormat() {
		return createConstraintFileFormat;
	}

	public void setCreateConstraintFileFormat(String createConstraintFileFormat) {
		this.createConstraintFileFormat = createConstraintFileFormat;
	}

	public String getImportForEachSchemaFileFormat() {
		return importForEachSchemaFileFormat;
	}

	public void setImportForEachSchemaFileFormat(String importForEachSchemaFileFormat) {
		this.importForEachSchemaFileFormat = importForEachSchemaFileFormat;
	}

	public String getImportForEachTableFileFormat() {
		return importForEachTableFileFormat;
	}

	public void setImportForEachTableFileFormat(String importForEachTableFileFormat) {
		this.importForEachTableFileFormat = importForEachTableFileFormat;
	}

	public String getTargetSchemaName(String sourceSchemaName) {
		String mapToName = this.schemaNameMapping.get(sourceSchemaName);
		if (mapToName != null) {
			return mapToName;
		} else {
			return sourceSchemaName;
		}
	}

//    public String getDateDataType(String schema, String table, String column) {
//        String returnVal = null;
//        HashMap<String, DataTypeConversion> dateDataTypeMapping = dataTypeMapping.get("DATE");
//        if (dateDataTypeMapping != null)
//        {
//        	DataTypeConversion
//	        //First check to see if an entry is found for that combination
//	        returnVal = dateDataTypeMapping.get(schema + "." + table + "." + column);
//	        if(returnVal == null) {
//	            returnVal = dateDataTypeMapping.get("*.*." + column);
//	        }
//        }
//        
//        return returnVal;
//    }
//    
//    public String getDecimalDataType(String schema, String table, String column) {
//        String returnVal = null;
//        //First check to see if an entry is found for that combination
//        returnVal = decimalDataTypeMapping.get(schema + "." + table + "." + column);
//        if(returnVal == null) {
//            returnVal = decimalDataTypeMapping.get("*.*." + column);
//        }
//        
//        return returnVal;
//    }
//
//    public String getNumericDataType(String schema, String table, String column) {
//        String returnVal = null;
//        //First check to see if an entry is found for that combination
//        returnVal = numericDataTypeMapping.get(schema + "." + table + "." + column);
//        if(returnVal == null) {
//            returnVal = numericDataTypeMapping.get("*.*." + column);
//        }
//        
//        return returnVal;
//    }
//
//    public String getTimestampDataType(String schema, String table, String column) {
//        String returnVal = null;
//        //First check to see if an entry is found for that combination
//        returnVal = numericDataTypeMapping.get(schema + "." + table + "." + column);
//        if(returnVal == null) {
//            returnVal = numericDataTypeMapping.get("*.*." + column);
//        }
//        
//        return returnVal;
//    }

	public void cleanUp() throws SQLException {
		closeSourceDatabaseConnection();
		closeTargetDatabaseConnection();
	}

	/**
	 * Displays the usage for the Generator
	 * 
	 * @param options
	 */
	public void printUsage(Options options) {
		HelpFormatter hlpfrmt = new HelpFormatter();
		hlpfrmt.printHelp("MigrateDatabase", options);
	}

	/**
	 * Prints the parameter values
	 */
	public void printParameters() {

		writeMessage("sourceJDBCUrl:" + this.sourceJDBCUrl);
		writeMessage("sourceJDBCDriver:" + this.sourceJDBCDriver);
		writeMessage("sourceUser:" + this.sourceUser);
		writeMessage("sourcePassword:" + this.sourcePassword);
		MigrateDatabaseUtils.writeMessage("sourceCatalog:" + this.sourceCatalog);
		writeMessage("sourceSchema:" + this.sourceSchema);
		writeMessage("directConnection:" + this.directConnection);
		MigrateDatabaseUtils.writeMessage("targetJDBCUrl:" + this.targetJDBCUrl);
		writeMessage("targetJDBCDriver:" + this.targetJDBCDriver);
		writeMessage("targetUser:" + this.targetUser);
		writeMessage("targetPassword:" + this.targetPassword);
		writeMessage("outputPath:" + this.outputPath);
		writeMessage("createTableScript:" + this.createTableScript);
		writeMessage("createDropTableScript:" + this.createDropTableScript);
		writeMessage("createIndexScript:" + this.createIndexScript);
		writeMessage("createSequenceScript:" + this.createSequenceScript);
		writeMessage("exportData:" + this.exportData);
		writeMessage("exportColumnNames:" + this.exportColumnNames);
		writeMessage("exportOutputType:" + this.exportOutputType);
		writeMessage("exportOutputPath:" + this.exportOutputPath);
		writeMessage("maxRecordsPerTable:" + this.maxRecordsPerTable);
		writeMessage("maxRecordsPerFile:" + this.maxRecordsPerFile);
		writeMessage("compress:" + this.compress);
		writeMessage("delimiter:" + this.delimiter);
		MigrateDatabaseUtils.writeMessage("cellDelimiter:" + this.cellDelimiter);
		writeMessage("fileNumberPadding:" + this.fileNumberPadding);
		writeMessage("printListOfTables:" + this.printListOfTables);
		writeMessage("printListOfTablesWithRecordCount:" + this.printListOfTablesWithRecordCount);
		writeMessage("printSchemaStats:" + this.printSchemaStats);
		writeMessage("columnNameReplacements:" + this.columnNameReplacements);
		writeMessage("doubleQuoteColumnNames:" + this.doubleQuoteColumnNames);

		writeMessage("otherDataTypeMapping:" + this.dataTypeMapping);

		writeMessage("verbose:" + this.verbose);
		writeMessage("debug:" + this.debug);
	}

	public void writeMessage(String msg) {
		MigrateDatabaseUtils.writeMessage(msg);
	}
}

class SchemaIncludeExclude {
	String schemaName = "";
	ArrayList<String> exclude = new ArrayList<String>();
	ArrayList<String> include = new ArrayList<String>();

	public SchemaIncludeExclude() {

	};

	public SchemaIncludeExclude(String schema, List<Object> include, List<Object> exclude) {
		this.schemaName = schema;
		addInclude(include);
		addExclude(exclude);
	};

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public ArrayList<String> getExclude() {
		return exclude;
	}

	public void setExclude(ArrayList<String> exclude) {
		this.exclude = exclude;
	}

	public void addExclude(String e) {
		this.exclude.add(e);
	}

	public void addExclude(List<Object> source) {
		updateList(source, this.exclude);
	}

	public boolean isExclude(String e) {
		return this.exclude.contains(e);
	}

	public ArrayList<String> getInclude() {
		return include;
	}

	public void setInclude(ArrayList<String> include) {
		this.include = include;
	}

	public void addInclude(List<Object> source) {
		updateList(source, this.include);
	}

	public void updateList(List<Object> source, ArrayList<String> target) {
		int numRcds = source == null ? 0 : source.size();
		for (int i = 0; i < numRcds; i++) {
			String table = source.get(i).toString();
			if (table == null || table.length() == 0)
				continue;
			target.add(table);
		}
	}

	public void addInclude(String i) {
		this.include.add(i);
	}

	public boolean isInclude(String i) {
		return this.include.contains(i);
	}

	public void updateIncludeExclude(List<Object> inc, List<Object> exc) {
		addInclude(inc);
		addExclude(exc);
	}

}
