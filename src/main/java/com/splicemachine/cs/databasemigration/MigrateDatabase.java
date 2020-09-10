package com.splicemachine.cs.databasemigration;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.splicemachine.cs.databasemigration.output.BaseOutput;
import com.splicemachine.cs.databasemigration.schema.Table;
import com.splicemachine.cs.databasemigration.schema.ColumnMetadata;
import com.splicemachine.cs.databasemigration.schema.DataTypeConversion;
import com.splicemachine.cs.databasemigration.schema.TableConversionMetadata;
import com.splicemachine.cs.databasemigration.vendors.BaseDatabaseVendor;

/**
 * This tool is used to generate splice schemas from other (or even splice)
 * databases. It can: 
 * - Migrate schemas, tables, foreign keys, sequences, constraints. 
 * 	 These can be migrated directly or intermediate scripts can be
 *   created. 
 * - Export objects such as views, functions, stored procedures and
 *   packages 
 * - Generate import scripts - Generate sqoop scripts - Export Data.
 *   The data can be migrated directly or written to a file.
 * 
 * There are many configuration options. The full list of options can be found
 * in the /resources/config/my-config.xml file.
 * 
 * The syntax for running the Migration is as follows:
 * 
 * java -cp $DM_JAR:$ORACLE_JDBC_JAR
 * com.splicemachine.cs.databasemigration.MigrateDatabase -configFile
 * my-config.xml -connectionId oracle -configId default
 *
 * The connectionId needs to map an id in the 'connections' section of the
 * configuration file. The configId needs to map to an id in the 'configs'
 * section of the configuration file.
 * 
 * TODO: Test other database platforms after all of these changes 
 * TODO: Test direct database to database data import 
 * TODO: Test direct database to database schema creation
 * 
 * @author tedmonddong
 * 
 */
public class MigrateDatabase {

	private static final Logger LOG = Logger.getLogger(MigrateDatabase.class);

	private static final String version = "1.0";

	ArrayList<String> userList = new ArrayList<String>();

	ArrayList<String> primaryKeyNames = new ArrayList<String>();
	ArrayList<String> foreignKeyNames = new ArrayList<String>();

	public MigrateDatabaseConfig config = new MigrateDatabaseConfig();

	public static void main(String[] args) {
		new MigrateDatabase(args);
	}

	public MigrateDatabase(String[] args) {
		MigrateDatabaseUtils.writeMessage("************** Begin processing - version:" + version);
		try {
			if (config.processParameters(args)) {
				if (config.verbose)
					MigrateDatabaseUtils.printSQLDataTypes();

				try {
					// We need the list of users to compare them to the schemas
					// We want to make sure that when we create the schemas
					// We do not create schemas if there is also a user with
					// that name
					if (config.createTableScript || config.createUsers) {
						userList = this.getUsers(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor, null);
						config.setUserList(userList);
					}

					// Retrieve the list of schemas to process
					ArrayList<String> schemaList = getSchemaList();

					/**
					 * Prints out the details about the database connection for the source database.
					 * This is rarely used but good for informational purposes.
					 */
					if (config.printSchemaStats) {
						MigrateDatabaseUtils.writeMessage("======= Print Schema Stats ======= ");

						for (String currentSchema : schemaList) {
							MigrateDatabaseUtils.writeMessage("**** Schema:" + currentSchema);
							MigrateDatabaseUtils.printSchemaStats(config.getSourceDatabaseConnection(),
									config.sourceDatabaseVendor, config.getSourceMetaData(), null, currentSchema);
						}
					}

					String schemaName = "";

					for (String currentSchema : schemaList) {

						ArrayList<Table> tableList = getTableListForSchema(currentSchema);

						schemaName = config.getTargetSchemaName(currentSchema);

						MigrateDatabaseUtils.writeMessage("======= Processing Schema:" + schemaName + " ======= ");

						// might want to add something to get the table counts
						// (this should be optional)
						if (config.printListOfTablesWithRecordCount || config.exportData) {

							MigrateDatabaseUtils.writeMessage("======= Print Table Counts ======= ");

							setTableCounts(config.getSourceDatabaseConnection(), tableList);
						}

						if (config.printListOfTables || config.printListOfTablesWithRecordCount) {

							MigrateDatabaseUtils.writeMessage("======= Print List of Tables ======= ");

							createTableCountFile(tableList);
						}

						if (config.createTableScript || config.createIndexScript || config.exportData) {

							MigrateDatabaseUtils.writeMessage("======= Determine Load Sequence ======= ");

							// Add something to determine the sequence
							determineLoadSequence(tableList);

						}
						
						MigrateDatabaseUtils.writeMessage("======= Process Source Tables  ======= ");

						HashMap<String, TableConversionMetadata> tableConversions = processSchemaTables(
								config.sourceDatabaseVendor, config.getSourceDatabaseConnection(),
								config.getSourceMetaData(), currentSchema, schemaName, tableList,
								config.createTableScript,
								config.createSqoopQueryFiles);
						
						if (config.createIndexScript) {

							MigrateDatabaseUtils.writeMessage("======= Create Index Script ======= ");

							createIndexScript(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
									currentSchema, schemaName, tableList);

							MigrateDatabaseUtils.writeMessage("======= Create Unique Index Script ======= ");

							createUniqueIndexScript(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
									currentSchema, schemaName, tableList);
						}

						if (config.createSequenceScript) {

							MigrateDatabaseUtils.writeMessage("======= Create Sequence Script ======= ");

							createSequenceScript(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
									config.sourceDatabaseVendor, currentSchema, schemaName);
						}
						
						if (config.exportData) {

							MigrateDatabaseUtils.writeMessage("======= Export Data ======= ");

							exportData(config.sourceDatabaseVendor, config.getSourceDatabaseConnection(), tableList, tableConversions);
						}

						if (config.createSqoopScripts) {
							MigrateDatabaseUtils.writeMessage("======= Create Sqoop Script ======= ");
							createSqoopScripts(schemaName, tableList);
						}

						if (config.createRoleScript) {
							MigrateDatabaseUtils.writeMessage("======= Create Role Script ======= ");
							createRoleScript(schemaName, config.getRolesToCreate());
						}

						if (config.createGrantReadScript) {
							MigrateDatabaseUtils.writeMessage("======= Create Grant Read Script ======= ");
							createGrantScript(schemaName, tableList, "read");
						}

						if (config.createGrantWriteScript) {
							MigrateDatabaseUtils.writeMessage("======= Create Grant Write Script ======= ");
							createGrantScript(schemaName, tableList, "write");
						}

						if (config.createGrantExecuteScript) {
							MigrateDatabaseUtils.writeMessage("======= Create Grant Execute Script ======= ");
							createGrantScript(schemaName, tableList, "execute");
						}

						tableList = new ArrayList<Table>();
					}

					if (config.exportTriggers) {
						MigrateDatabaseUtils.writeMessage("======= Export Triggers ======= ");
						exportTriggers(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor);
					}

					if (config.exportProcedure) {
						MigrateDatabaseUtils.writeMessage("======= Export Procedures ======= ");
						exportProcedure(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor);
					}

					if (config.exportFunction) {
						MigrateDatabaseUtils.writeMessage("======= Export Functions ======= ");
						exportFunction(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor);
					}

					if (config.exportViews) {
						MigrateDatabaseUtils.writeMessage("======= Export Views ======= ");
						exportViews(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor);
					}

					if (config.createCheckConstraints) {
						MigrateDatabaseUtils.writeMessage("======= Export Check Constraints ======= ");
						exportCheckConstraints(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor, config.schemaList);
					}

					if (config.exportPackage) {
						MigrateDatabaseUtils.writeMessage("======= Export Package ======= ");
						exportPackage(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor);
					}

					if (config.createUsers) {
						MigrateDatabaseUtils.writeMessage("======= Export Users ======= ");
						exportUsers(userList);
					}

					if (config.exportRoles) {
						MigrateDatabaseUtils.writeMessage("======= Export Roles ======= ");
						exportRoles(config.getSourceDatabaseConnection(), config.getSourceMetaData(),
								config.sourceDatabaseVendor, config.schemaList);
					}

				} catch (SQLException ex) {
					MigrateDatabaseUtils.logError("Exception creating schema for table", ex);
				} catch (UnsupportedEncodingException uee) {
					// handle the error
					MigrateDatabaseUtils.logError("UnsupportedEncodingException: ", uee);
				} catch (FileNotFoundException fnfe) {
					// handle the error
					MigrateDatabaseUtils.logError("FileNotFoundException: ", fnfe);
				} catch (Exception fnfe) {
					// handle the error
					MigrateDatabaseUtils.logError("Exception: ", fnfe);
				} finally {
					try {
						config.cleanUp();
					} catch (SQLException e) {
						// Ignore
					}
				}
			}
		} catch (ParseException e) {
			MigrateDatabaseUtils.logError("Parse Exception: ", e);
		}
		MigrateDatabaseUtils.writeMessage("End processing");
	}

	/**
	 * Retrieves a list of schemas based on the configuration file.
	 * 
	 * @return
	 */
	public ArrayList<String> getSchemaList() {
		try {
			// Check to see if the <processAllSchemas>false</processAllSchemas> is true
			// if it is then get the list from the database
			if (config.processAllSchemas) {
				return getListOfSchemas(config.getSourceMetaData());
			} else {
				return config.schemaList;
			}
		} catch (Exception e) {
			MigrateDatabaseUtils.logError("Exception retrieving the list of schemas: ", e);
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Retrieves a list of schemas
	 * 
	 * @param md
	 * @return
	 * @throws SQLException
	 */
	public ArrayList<String> getListOfSchemas(DatabaseMetaData md) throws SQLException {
		ResultSet rs = md.getSchemas();
		while (rs.next()) {
			String schemaName = rs.getString(1);
			if (config.excludeSchemaList.contains(schemaName))
				continue;
			if (config.sourceDatabaseVendor.isValidSchema(schemaName))
				config.schemaList.add(schemaName);
		}
		return config.schemaList;
	}

	public ArrayList<Table> getTableListForSchema(String currentSchema) {
		MigrateDatabaseUtils.writeMessage("Getting tables for schema:" + currentSchema);
		ArrayList<Table> tableListForSchema = null;
		try {
			// We first want to see if there is a list of tables to be
			// included for the selected schema - if there is then we
			// will use that.
			ArrayList<String> tableList = config.getSchemaInclusions(currentSchema);

			// Otherwise we will goto the database and get a list of the
			// tables from there. We need to check each table against
			// the configuration definition to make sure it is not excluded
			if (tableList == null || tableList.size() == 0) {
				MigrateDatabaseUtils.writeMessage(
						"There are no records inclusionsExclusions -> schema -> tablesToInclude: so getting all tables in that schema");
				return getListOfTables(config.sourceDatabaseVendor, config.getSourceDatabaseConnection(),
						config.getSourceMetaData(), currentSchema);
			} else {
				tableListForSchema = new ArrayList<Table>();
				for (String table : tableList) {
					tableListForSchema.add(new Table(currentSchema, table, config.schemaNameMapping));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tableListForSchema;
	}

	/**
	 * Surrounds identifier name with double quotes and escapes quote characters in
	 * it.
	 * 
	 * @param identifier
	 * @throws Exception
	 */
	public String quoteIdentifier(String identifier) throws Exception {
		String result = new String();

		// avoid quoting already quoted strings, but still escape quotes in the content
		if (!(identifier.startsWith("\"") && identifier.endsWith("\""))) {
			result = "\"" + identifier.replace("\"", "\\\"") + "\"";
		} else {
			result = "\"" + identifier.substring(1, identifier.length() - 1).replace("\"", "\\\"") + "\"";
		}

		return result;
	}

	/**
	 * Builds SELECT statement that will produce export data in the format expected for the target table.
	 * @param sourceFullTableName
	 * @param sourceColumns
	 * @param targetColumns
	 * @param includeWhereConditions
	 * @param maxRecords 
	 * @return
	 */
	public String getSourceSelectSQL(
			BaseDatabaseVendor dbVendor, 
			String sourceFullTableName, 
			ArrayList<ColumnMetadata> sourceColumns, 
			HashMap<Integer,ColumnMetadata> targetColumns, 
			boolean includeWhereConditions, 
			long maxRecords,
			boolean excludeLOBs
			)  {


		String SQLSYNTAX = "SELECT {COLUMNS} FROM {TABLE}";
		if (maxRecords > 0) {
			SQLSYNTAX = dbVendor.getAllColumnsLimitedCountSyntax();
		}
		
		String sql = SQLSYNTAX.replace("{TABLE}", sourceFullTableName);
		if (maxRecords > -1)
		{
			sql = sql.replace("{LIMIT_NUM}", Long.toString(maxRecords));
		}
		
		String columnStr = getSourceSelectExpressions(sourceFullTableName, sourceColumns, targetColumns);
		
		sql = sql.replace("{COLUMNS}", columnStr);

		if (includeWhereConditions)
		{
			sql = sql + " WHERE $CONDITIONS";
		}
				
		return sql;
	}

	private String getSourceSelectExpressions(String sourceFullTableName, ArrayList<ColumnMetadata> sourceColumns,
			HashMap<Integer, ColumnMetadata> targetColumns) {
		StringBuilder columns = new StringBuilder();
		int columnCount = 0;
		for (ColumnMetadata sourceCol : sourceColumns) {
			if (config.debug)
				MigrateDatabaseUtils.printColumnDetails(sourceCol);
            if (sourceCol.getTypeName().equals("BLOB") || sourceCol.getTypeName().equals("CLOB"))
            {
            	// TODO skip LOB columns for now, but address this later with VTI based JDBC insert/select from splice
            	MigrateDatabaseUtils.writeMessage(Level.WARN, String.format("Skipping source LOB column [%s.%s].", sourceFullTableName, sourceCol.getColumnName()));
            }
            else
            {
				if (columnCount > 0)
					columns.append(MigrateDatabaseConstants.DELIM_COMMA);
	
				ColumnMetadata targetCol = targetColumns.get(sourceCol.getOrdinalPosition());
				columns.append( getSourceConversionExpression(sourceCol, targetCol ) );	
				
				columnCount++;
            }
		}
		
		return columns.toString();
	}
	
	private String getTargetInsertColumnList(
			ArrayList<ColumnMetadata> sourceColumns,
			HashMap<Integer, ColumnMetadata> targetColumns
			) {
		StringBuilder columns = new StringBuilder();
		int columnCount = 0;
		for (ColumnMetadata sourceCol : sourceColumns) {
			if (config.debug)
				MigrateDatabaseUtils.printColumnDetails(sourceCol);
            if (!(sourceCol.getTypeName().equals("BLOB") || sourceCol.getTypeName().equals("CLOB")))
            {
            	
				if (columnCount > 0)
					columns.append(MigrateDatabaseConstants.DELIM_COMMA);
	
				ColumnMetadata targetCol = targetColumns.get(sourceCol.getOrdinalPosition());
				columns.append( targetCol.getColumnName() );	
				
				columnCount++;
            }
		}
		
		return columns.toString();
	}
	/**
	 * Generates select expression used to convert from sourceColumn data type to targetColumn data type.
	 * Also formats date columns as 'YYYY-MM-DD' output.
	 * 
	 * @param sourceColumn
	 * @param targetColumn
	 * @return
	 */
	private String getSourceConversionExpression(ColumnMetadata sourceColumn, ColumnMetadata targetColumn) 
	{
		String sourceExpression;
		String conversion = targetColumn.getConversion();
		// check for explicit conversion
		if (conversion!=null)
		{
			sourceExpression = String.format("%s AS %s",conversion, targetColumn.getColumnName());
		}
		else if (sourceColumn.getDataType() == targetColumn.getDataType())
		{
			// no conversion, just return the sourceColumn name
			if ( targetColumn.getTypeName().contains("DATE"))
			{
				sourceExpression = String.format("TO_CHAR(%s,'YYYY-MM-DD') AS %s", sourceColumn.getColumnName(), sourceColumn.getColumnName());
			}
			else
			{
				sourceExpression = sourceColumn.getColumnName();
			}
		}
		else
		{
			
			// use CAST to convert from source data type to target
			sourceExpression = String.format("CAST(%s AS %s) AS %s", sourceColumn.getColumnName(), targetColumn.getTypeName(), targetColumn.getColumnName());
		}
		
		if (config.debug)
		{
			MigrateDatabaseUtils.writeMessage(
						String.format("Source[%s %s] Target[%s %s] -> Expression [%s]", 
										sourceColumn.getColumnName(), 
										sourceColumn.getTypeName(), 
										targetColumn.getColumnName(), 
										targetColumn.getTypeName(), 
										sourceExpression)
						);
		}
		
		return sourceExpression;

	}

	/**
	 * Generates CREATE statement from column metadata, default definitions and primary key definition
	 * 
	 * @param fullTableName
	 * @param tableColumnDefaults
	 * @param targetColumns
	 * @param primaryKey
	 * @return
	 */
	public String getCreateTableSQL(String fullTableName, HashMap<String, String> tableColumnDefaults, HashMap<Integer, ColumnMetadata> targetColumns, String primaryKey) {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE ");
		sb.append(fullTableName);
		sb.append(" (");
	

		try {

			int columnCount = 0;
			
			// build columns in create statement in same order as source (ordinal position values are 1-based)
			for (Integer ordinalPos=1; ordinalPos<=targetColumns.size(); ordinalPos++)
			{
			    ColumnMetadata columnRes = targetColumns.get(ordinalPos);
				
				if (config.debug)
					MigrateDatabaseUtils.printColumnDetails(columnRes);
				if (columnCount > 0)
					sb.append(MigrateDatabaseConstants.DELIM_COMMA);
				
				sb.append(MigrateDatabaseConstants.NEW_LINE);
				sb.append(MigrateDatabaseConstants.TAB);

				sb.append(getColumnCreateText(columnRes));


				columnCount++;
			}

			// add primary key
			if (primaryKey.length() > 0) {
				sb.append(",");
				sb.append(MigrateDatabaseConstants.NEW_LINE);
				sb.append(MigrateDatabaseConstants.TAB);
				sb.append(primaryKey);
			}

			sb.append(MigrateDatabaseConstants.NEW_LINE);
			sb.append(")");

		} catch (Exception e) {
			MigrateDatabaseUtils.logError("Exception: ", e);
		}

		return sb.toString();
	}

	/**
	 * Creates the create table script for the specified schema
	 * 
	 * @param conn
	 * @param meta
	 * @param schema
	 * @param lTables
	 * @throws Exception
	 */
	public HashMap<String, TableConversionMetadata> processSchemaTables(BaseDatabaseVendor vendor, Connection conn, DatabaseMetaData meta,
			String schemaSource, String schemaTarget, ArrayList<Table> lTables, boolean createTableFile, boolean createQueryFile)
			throws Exception {

		HashMap<String, TableConversionMetadata> result = new HashMap<String, TableConversionMetadata>();
		
		BaseOutput output = config.getTargetOutput();
		output.startCreateTable(schemaSource);
		output.startCreateForeignKey(schemaSource);
		
		Statement statement = conn.createStatement();

		

		for (Table table : lTables) {
			String sFullTableName = table.getTargetFullTableName();
			String sFullSourceTableName = table.getSourceFullTableName();
			
			HashMap<String, String> tableColumnDefaults = null;
			LOG.info("WRITING SCRIPTS FOR TABLE: " + sFullSourceTableName);
			LOG.debug("**************************** addColumnDefault: " + config.isAddColumnDefault());
			if (config.isAddColumnDefault()) {
				tableColumnDefaults = vendor.getTableColumnDefault(conn, table);
			}

			try {
				ResultSet resColumns = meta.getColumns(null, table.getSourceSchema(), table.getSourceTableName(), null);
				ArrayList<ColumnMetadata> sourceColumns = readColumnDefinitions(resColumns);
				resColumns.close();
				HashMap<Integer,ColumnMetadata> targetColumns = convertColumnMetadata(sourceColumns, vendor, conn,
						table.getSourceSchema(), table.getSourceTableName(), tableColumnDefaults);
				
				String primaryKey = getPrimaryKey(meta, table);
				String sCreateStatement = getCreateTableSQL(sFullTableName, tableColumnDefaults, targetColumns, primaryKey);
				
				if (createTableFile)
				{
					output.outputCreateTable(schemaTarget, 
							table.getTargetFullTableName(true), 
							sFullTableName, 
							sCreateStatement, 
							getTargetInsertColumnList(sourceColumns, targetColumns), 
							true,  //TODO figure out if we can dynamically adjust this to address multi-line values automatically 
							null); //TODO parameterize the character set in the config file
				}
				
				if (config.createSqoopQueryFiles ) {
					String sSelectStatement = getSourceSelectSQL(
												vendor, 
												sFullSourceTableName, 
												sourceColumns, 
												targetColumns, 
												true, 
												0l,
												true); //TODO: skipping LOBs for now, find approach with VTI to address this
					
					output.outputSqoopQueryFiles(schemaTarget, table.getTargetTableName(), sSelectStatement);
					output.endSqoopQueryFiles();
				}

				if (config.isCreateForeignKeys()) {
					this.foreignKeyNames = vendor.exportForeignKeys(conn, meta, table, output);
				}
				
				// add table conversion to resulting list of conversions
				result.put(sFullSourceTableName, new TableConversionMetadata( sourceColumns, targetColumns) );
				
			} catch (Exception e) {
				MigrateDatabaseUtils.logError("Exception: ", e);
			}
			
		}

		try {
			statement.close();
		} catch (Exception ignore) {
		}
		if (config.isCreateForeignKeys()) {
			output.endCreateForeignKey();
		}
		output.endCreateTable();
		
		return result;
	}

	private ArrayList<ColumnMetadata> readColumnDefinitions(ResultSet res) throws SQLException {
		ArrayList<ColumnMetadata> result = new ArrayList<ColumnMetadata>();
		while (res.next()) {
			result.add(new ColumnMetadata(res));
		}
		return result;
	}

	private boolean dataTypeSupportsDefault(int type) {
		boolean result;

		switch (type) {
		case java.sql.Types.BLOB:
			result = false;
			break;
		default:
			result = true;
			break;
		}

		return result;
	}

	/**
	 * Builds the primary key for a table
	 * 
	 * @param meta
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public String getPrimaryKey(DatabaseMetaData meta, Table table) throws Exception {

		StringBuffer primaryKey = new StringBuffer();
		Map<String, String> pkMap = new HashMap<String, String>();

		ResultSet pk = meta.getPrimaryKeys(null, table.getSourceSchema(), table.getSourceTableName());

		String pkName = null;

		try {
			boolean pkFound = false;
			while (pk.next()) {
				pkFound = true;
				if (pkName == null) {
					pkName = pk.getString("PK_NAME");
				}
				pkMap.put(pk.getString("KEY_SEQ"), pk.getString("COLUMN_NAME"));
				if (config.debug) {
					MigrateDatabaseUtils.printPrimaryKeyDetails(pk);
				}
			}

			if (!pkFound && config.isUseUniqueIndexForMissingPrimary()) {
				// Let's see if there is a ERIN HERE

				String prevIndexName = null;
				ResultSet tableIndexes = meta.getIndexInfo(null, table.getSourceSchema(), table.getSourceTableName(),
						false, true);
				try {
					while (tableIndexes.next()) {
						if (config.verbose)
							MigrateDatabaseUtils.printIndexDetails(tableIndexes);
						if ("1".equals(tableIndexes.getString("NON_UNIQUE")))
							continue;
						// We only want indexes that have name
						pkName = tableIndexes.getString("INDEX_NAME");
						if (pkName == null) {
							pkName = null;
							continue;
						}
						// We only want unique indexes that start with a primary key indiator such as
						// PK_
						if (!pkName.matches(config.getPrimaryKeyUniqueIndexPrefix())) {
							continue;
						}

						if (prevIndexName == null) {
							prevIndexName = pkName;
						}

						if (pkName != null && prevIndexName != null) {

							if (!pkName.equals(prevIndexName) && prevIndexName.length() > 0 && pkMap.size() > 0) {
								// We already found a unique index as a primary key for this table
								break;
							}

							pkMap.put(tableIndexes.getString("ORDINAL_POSITION"),
									tableIndexes.getString("COLUMN_NAME"));
							prevIndexName = pkName;
						}
					}
				} finally {
					try {
						tableIndexes.close();
					} catch (Exception ignore) {
					}
				}

			}

		} finally {
			try {
				pk.close();
			} catch (Exception ignore) {
			}
		}

		if (pkMap.size() > 0) {

			if (pkName != null) {
				primaryKeyNames.add(pkName);
				primaryKey.append("CONSTRAINT ");
				primaryKey.append(pkName);
				primaryKey.append(" ");
			}

			primaryKey.append("PRIMARY KEY (");
			Map<String, String> sortedMap = new TreeMap<String, String>(pkMap);
			for (Iterator<Entry<String, String>> iterator = sortedMap.entrySet().iterator(); iterator.hasNext();) {
				Entry<String, String> entry = iterator.next();
				primaryKey.append(getConvertedColumnName(entry.getValue(), config.columnNameReplacements,
						config.doubleQuoteColumnNames));
				primaryKey.append(",");
			}
			primaryKey.deleteCharAt(primaryKey.length() - 1);
			primaryKey.append(")");
		}

		return primaryKey.toString();
	}

	/**
	 * Creates the create sequence script for the specified schema
	 * 
	 * @param conn
	 * @param meta
	 * @param schema
	 * @param lTables
	 * @throws Exception
	 */
	public void createSequenceScript(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor,
			String schemaSource, String schemaTarget) throws Exception {

		BaseOutput output = config.getTargetOutput();
		output.startCreateSequence(schemaSource);

		Statement statement = conn.createStatement();
		final ResultSet rs = meta.getTables(null, schemaSource, null, new String[] { "SEQUENCE" });
		while (rs.next()) {
			String sFullTableName = null;
			String sTableSchema = rs.getString("TABLE_SCHEM");
			String sSequenceName = rs.getString("TABLE_NAME");
			if (sTableSchema != null) {
				sFullTableName = sTableSchema + "." + sSequenceName;
			} else {
				sFullTableName = sSequenceName;
			}
			StringBuilder sb = new StringBuilder();

			MigrateDatabaseUtils.writeMessage("CREATE SEQUENCE :" + sFullTableName);

			try {
				HashMap<String, String> sequenceDetails = vendor.getSequenceDefinition(conn, sTableSchema,
						sSequenceName);
				if (sequenceDetails != null) {

					sb.append("CREATE SEQUENCE ");
					sb.append(sFullTableName);
					sb.append(" AS BIGINT MINVALUE ");
					sb.append((String) sequenceDetails.get("MinValue"));
					sb.append(" INCREMENT BY ");
					sb.append((String) sequenceDetails.get("IncrementBy"));
					sb.append(" START WITH ");
					sb.append((String) sequenceDetails.get("Last"));

					String cycle = (String) sequenceDetails.get("Cycle");

					if ("N".equals(cycle) || "NO".equals(cycle)) {
						sb.append(" NO CYCLE");
					} else {
						sb.append(" CYCLE");
					}
					output.ouputCreateSequence(sTableSchema, sFullTableName, sb.toString());
				}
			} catch (Exception e) {
				MigrateDatabaseUtils.logError("Exception: ", e);
			}

		}

		try {
			statement.close();
		} catch (Exception ignore) {
		}

		output.endCreateSequence();
	}

	/**
	 * Creates the index script
	 * 
	 * @param conn
	 * @param meta
	 * @param schema
	 * @throws Exception
	 */
	public void createIndexScript(Connection conn, DatabaseMetaData meta, String schemaSource, String schemaTarget,
			ArrayList<Table> tableList) throws Exception {

		BaseOutput output = config.getTargetOutput();
		output.startCreateIndex(schemaSource);

		String tableName = "";
		String indexName = "";
		String nonUnique = null;
		String prevIndexName = "";
		String prevTableName = "";
		Map<String, String> colMap = new HashMap<String, String>();

		for (Table table : tableList) {
			ResultSet tableIndexes = meta.getIndexInfo(null, schemaSource, table.getSourceTableName(), false, true);
			try {
				while (tableIndexes.next()) {
					if (config.verbose)
						MigrateDatabaseUtils.printIndexDetails(tableIndexes);
					if ("0".equals(tableIndexes.getString("NON_UNIQUE")))
						continue;
					indexName = tableIndexes.getString("INDEX_NAME");
					if (indexName == null) {
						indexName = null;
						continue;
					}

					if (prevIndexName == null) {
						prevIndexName = indexName;
					}

					if (indexName != null && prevIndexName != null) {

						if (!indexName.equals(prevIndexName) && prevIndexName.length() > 0 && prevTableName.length() > 0
								&& colMap.size() > 0) {
							if (!(primaryKeyNames.contains(prevIndexName) || foreignKeyNames.contains(prevIndexName))) {
								output.ouputCreateIndex(schemaTarget, prevIndexName, createIndexStatement(schemaTarget,
										tableName, prevIndexName, nonUnique, colMap));
							}
							colMap.clear();
						}
						tableName = tableIndexes.getString("TABLE_NAME");
						nonUnique = tableIndexes.getString("NON_UNIQUE");
						if ("D".equals(tableIndexes.getString("ASC_OR_DESC"))) {
							colMap.put(tableIndexes.getString("ORDINAL_POSITION"),
									tableIndexes.getString("COLUMN_NAME") + " DESC");
						} else {
							colMap.put(tableIndexes.getString("ORDINAL_POSITION"),
									tableIndexes.getString("COLUMN_NAME"));
						}

						prevIndexName = indexName;
						prevTableName = tableName;
					}
				}
			} finally {
				try {
					tableIndexes.close();
				} catch (Exception ignore) {
				}
			}
		}

		if (indexName != null && indexName.length() > 0 && tableName.length() > 0 && colMap.size() > 0) {

			if (!(primaryKeyNames.contains(indexName) || foreignKeyNames.contains(indexName))) {
				output.ouputCreateIndex(schemaTarget, indexName,
						createIndexStatement(schemaTarget, tableName, indexName, nonUnique, colMap));

			}
		}
		output.endCreateIndex();
	}

	/**
	 * Creates the index script
	 * 
	 * @param conn
	 * @param meta
	 * @param schema
	 * @throws Exception
	 */
	public void createUniqueIndexScript(Connection conn, DatabaseMetaData meta, String schemaSource,
			String schemaTarget, ArrayList<Table> tableList) throws Exception {

		BaseOutput output = config.getTargetOutput();
		output.startCreateUniqueIndex(schemaSource);

		String tableName = "";
		String indexName = "";
		String nonUnique = null;
		String prevIndexName = "";
		String prevTableName = "";
		Map<String, String> colMap = new HashMap<String, String>();

		for (Table table : tableList) {
			ResultSet tableIndexes = meta.getIndexInfo(null, schemaSource, table.getSourceTableName(), false, true);
			try {
				while (tableIndexes.next()) {
					if (config.verbose)
						MigrateDatabaseUtils.printIndexDetails(tableIndexes);
					if ("1".equals(tableIndexes.getString("NON_UNIQUE")))
						continue;
					indexName = tableIndexes.getString("INDEX_NAME");
					if (indexName == null) {
						indexName = null;
						continue;
					}

					if (prevIndexName == null) {
						prevIndexName = indexName;
					}

					if (indexName != null && prevIndexName != null) {

						if (!indexName.equals(prevIndexName) && prevIndexName.length() > 0 && prevTableName.length() > 0
								&& colMap.size() > 0) {

							if (!(primaryKeyNames.contains(prevIndexName) || foreignKeyNames.contains(prevIndexName))) {
								output.ouputCreateUniqueIndex(schemaTarget, prevIndexName,
										createIndexStatement(schemaTarget, tableName, prevIndexName, "f", colMap));
							}
							colMap.clear();
						}
						tableName = tableIndexes.getString("TABLE_NAME");
						nonUnique = tableIndexes.getString("NON_UNIQUE");
						if ("D".equals(tableIndexes.getString("ASC_OR_DESC"))) {
							colMap.put(tableIndexes.getString("ORDINAL_POSITION"),
									tableIndexes.getString("COLUMN_NAME") + " DESC");
						} else {
							colMap.put(tableIndexes.getString("ORDINAL_POSITION"),
									tableIndexes.getString("COLUMN_NAME"));
						}

						prevIndexName = indexName;
						prevTableName = tableName;
					}
				}
			} finally {
				try {
					tableIndexes.close();
				} catch (Exception ignore) {
				}
			}
		}

		if (indexName != null && indexName.length() > 0 && tableName.length() > 0 && colMap.size() > 0) {

			if (!(primaryKeyNames.contains(indexName) || foreignKeyNames.contains(indexName))) {
				output.ouputCreateUniqueIndex(schemaTarget, indexName,
						createIndexStatement(schemaTarget, tableName, indexName, "f", colMap));
			}
		}

		output.endCreateUniqueIndex();
	}

	/**
	 * Builds the create index statement
	 * 
	 * @param tableName
	 * @param indexName
	 * @param nonUnique
	 * @param colMap
	 * @return
	 */
	public String createIndexStatement(String schemaName, String tableName, String indexName, String nonUnique,
			Map<String, String> colMap) {
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE ");
		if ("false".equals(nonUnique) || "f".equals(nonUnique)) {
			sb.append("UNIQUE ");
		}
		sb.append("INDEX ");
		if (schemaName != null && schemaName.length() > 0) {
			sb.append(schemaName);
			sb.append(".");
		}
		sb.append(indexName);
		sb.append(" ON ");
		if (schemaName != null && schemaName.length() > 0) {
			sb.append(schemaName);
			sb.append(".");
		}
		sb.append(tableName);
		sb.append(" (");
		Map<String, String> sortedMap = new TreeMap<String, String>(colMap);
		for (Iterator<Entry<String, String>> colIter = sortedMap.entrySet().iterator(); colIter.hasNext();) {
			Entry<String, String> entry = colIter.next();
			sb.append(entry.getValue());
			sb.append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		sb.append(")");
		return sb.toString();
	}

	/**
	 * Need to convert the column name. Splice Machine supports column names that
	 * conform to SQL92Identifier syntax. An ordinary identifier must begin with a
	 * letter and contain only letters, underscore characters (_), and digits Cannot
	 * use reserved words as identifiers for dictionary objects
	 * 
	 * 
	 * @param columnName
	 * @return
	 */
	public String getConvertedColumnName(String columnName, Map<String, String> replacements, boolean useDoubleQuotes)
			throws Exception {
		String temp = columnName;
		// address keywords by adding _ suffix
		if (MigrateDatabaseConstants.isReservedKeyword(temp)) {
			temp = temp + "_";
		}
		// apply replacements if any exist
		for (Map.Entry<String, String> searchReplace : replacements.entrySet()) {
			temp = StringUtils.replace(temp, searchReplace.getKey(), searchReplace.getValue());
		}

		// double quote column name if requested
		if (useDoubleQuotes) {
			temp = quoteIdentifier(temp);
		}

		return temp;
	}

	public String getConvertedDefault(int targetDataType, String defaultValue) throws Exception {
		if (defaultValue.equals("SYSDATE") && targetDataType == java.sql.Types.TIMESTAMP) {
			return "CURRENT_TIMESTAMP";
		} else if (defaultValue.equalsIgnoreCase("(getdate())") && targetDataType == java.sql.Types.TIMESTAMP) {
			return "CURRENT_TIMESTAMP";
		} else if (defaultValue.equalsIgnoreCase("(getdate())") || defaultValue.equals("SYSDATE")) {
			return "CURRENT_DATE";

		} else if (targetDataType == java.sql.Types.INTEGER || targetDataType == java.sql.Types.SMALLINT
				|| targetDataType == java.sql.Types.BIGINT) {
			// target column is integer but default may not be integer
			// try to convert from double and cast to integer
			// addresses cases of DEFAULT 0.0 when the original column was not integer but
			// was converted to integer
			try {
				double tempDouble = Double.parseDouble(defaultValue);
				int tempInt = (int) tempDouble;
				return Integer.toString(tempInt);
			} catch (Exception e) {
				// do nothing, if the default expression is not convertible to integer,
				// then leave original default so it can be evaluated manually
			}

		}
		return defaultValue;
	}

	public ColumnMetadata getConvertedColumnMetadata(ColumnMetadata sourceCol, BaseDatabaseVendor vendor,
			Connection conn, String schema, String tableName, HashMap<String, String> sourceColumnDefaults)
			throws Exception {
		ColumnMetadata result = new ColumnMetadata();

		result.copyFrom(sourceCol); // most of the info should stay the same

		// the rest is changing the name, type, precision according to splice conversion
		// needs.

		int type = sourceCol.getDataType();
		String typeName = sourceCol.getTypeName();
		long precision = sourceCol.getColumnSize();
		int scale = sourceCol.getDecimalDigits();
		String temp=null;
		String expression = null;
		
		// get type conversion from config file, this should override any other conversion if present
		DataTypeConversion conversion = config.getDataTypeConversion(typeName, schema, tableName, sourceCol.getColumnName());
		
		if (conversion!=null)
		{
			temp = conversion.getTargetDataType();
			result.setDataType(convertDataTypeNameToJavaSQLType( temp ) );
			result.setTypeName(temp);
			if (temp.equalsIgnoreCase("NUMERIC") || temp.equalsIgnoreCase("DECIMAL")) {
				if (precision >= 0) {
					result.setColumnSize(precision);
				}
				if (scale >= 0) {
					result.setDecimalDigits(scale);
				}
			}
			expression = conversion.getConversionExpression();
			if (expression != null) {
				result.setConversion(expression.replace("{COLUMN}", sourceCol.getColumnName()));
			}
		}
		else
		{	
			// Oracle may return 1111 for a timestamp
			if (type == 1111 & typeName.startsWith("TIMESTAMP")) {
				type = java.sql.Types.TIMESTAMP;
			}

			switch (type) {
	
			case java.sql.Types.BIGINT:
				result.setDataType(type);
				result.setTypeName("BIGINT");
				break;
	
			case java.sql.Types.BINARY:
			case java.sql.Types.BLOB:
				result.setDataType(java.sql.Types.BLOB);
				result.setTypeName("BLOB");
				break;

			case java.sql.Types.VARBINARY:
				result.setDataType(java.sql.Types.VARBINARY);
				result.setColumnSize(precision);
				result.setTypeName(String.format("VARCHAR(%d) FOR BIT DATA", precision));
				break;

			case java.sql.Types.BIT:
			case java.sql.Types.BOOLEAN:
				result.setDataType(java.sql.Types.BOOLEAN);
				result.setTypeName("BOOLEAN");
				break;
	
			case java.sql.Types.NCHAR:
			case java.sql.Types.CHAR:
				result.setDataType(java.sql.Types.CHAR);
				if (config.isPadChar()) {
					precision = precision + config.getPadCharValue();
					result.setColumnSize(precision);
				}
				result.setTypeName(String.format("CHAR(%d)", precision));
	
				break;
	
			case java.sql.Types.CLOB:
				result.setDataType(java.sql.Types.CLOB);
				result.setTypeName("CLOB");
				break;
	
			case java.sql.Types.DATE:
				String targetTypeName = vendor.getDateDataType(conn, schema, tableName, sourceCol.getColumnName(), type,
							typeName, sourceCol.getColumnSize(), sourceCol.getDecimalDigits());
				if (targetTypeName.startsWith("DATE")) {
					result.setDataType(java.sql.Types.DATE);
					result.setTypeName(targetTypeName);
				} else if (targetTypeName.startsWith("TIMESTAMP")) {
					result.setDataType(java.sql.Types.TIMESTAMP);
					result.setTypeName(targetTypeName);
				}
				break;
	
			case java.sql.Types.DECIMAL:
			case java.sql.Types.NUMERIC:
				if (scale == 0) {
					if (precision < 5) {
						// It should be a small init
						result.setDataType(java.sql.Types.SMALLINT);
						result.setTypeName("SMALLINT");
					} else if (precision < 10) {
						// It should be an integer
						result.setDataType(java.sql.Types.INTEGER);
						result.setTypeName("INTEGER");
					} else {
						result.setDataType(java.sql.Types.BIGINT);
						result.setTypeName("BIGINT");
					}
				} else if (precision > 31) {
					result.setDataType(java.sql.Types.BIGINT);
					result.setTypeName("BIGINT");
				} else {

					if (precision > 0 && scale > 0) {
						result.setDataType(java.sql.Types.DECIMAL);

						result.setTypeName(String.format("DECIMAL(%d,%d)", precision, scale));
						result.setColumnSize(precision);
						result.setDecimalDigits(scale);

					} else {
						temp = vendor.getNumericDataType(conn, schema, tableName, sourceCol.getColumnName(), type,
								typeName, precision, scale);
						result.setDataType(convertDataTypeNameToJavaSQLType(temp));
						result.setTypeName(temp);

					}
				}
			
				break;
	
			case java.sql.Types.DOUBLE:
				result.setDataType(java.sql.Types.DOUBLE);
				result.setTypeName("DOUBLE");
				break;
	
			case java.sql.Types.FLOAT:
				if (precision > 52) {
					precision = 52;
				}
				result.setDataType(java.sql.Types.FLOAT);
				result.setTypeName(String.format("FLOAT(%d)", precision));
	
				break;
	
			case java.sql.Types.INTEGER:
				result.setDataType(java.sql.Types.INTEGER);
				result.setTypeName("INTEGER");
				break;
	
			case java.sql.Types.REAL:
				result.setDataType(java.sql.Types.REAL);
				result.setTypeName("REAL");
				break;
	
			case java.sql.Types.SMALLINT:
			case java.sql.Types.TINYINT:
				result.setDataType(java.sql.Types.SMALLINT);
				result.setTypeName("SMALLINT");
				break;
	
			case java.sql.Types.TIME:
				result.setDataType(java.sql.Types.TIME);
				result.setTypeName("TIME");
	
				break;
	
			case java.sql.Types.TIMESTAMP:
				if (typeName.equalsIgnoreCase("DATE")) {
					if (precision != 7) {
						System.out.println("precision is not 7, it is:" + precision);
					}
					temp = vendor.getDateDataType(conn, schema, tableName, sourceCol.getColumnName(), type, typeName,
							precision, scale);
					result.setDataType(convertDataTypeNameToJavaSQLType(temp));
					result.setTypeName(temp);

				} else {
					result.setDataType(java.sql.Types.TIMESTAMP);
					result.setTypeName("TIMESTAMP");
				}
			
				break;
	
			case java.sql.Types.NVARCHAR:
			case java.sql.Types.VARCHAR:
				if (config.isPadVarchar()) {
					precision = precision + config.getPadVarcharValue();
				}
				temp = vendor.getVarcharType(type, typeName, precision, scale);
				result.setDataType(convertDataTypeNameToJavaSQLType(temp));
				result.setTypeName(temp);
				break;
				
			default:
				temp = vendor.getColumnType(type, typeName, precision, scale);
				if (temp==null) {
					MigrateDatabaseUtils.writeErrorMessage("********* Splice Machine does not support the data type: "
							+ type + " and a typeName=" + typeName);
					MigrateDatabaseUtils.writeErrorMessage("*** try adding a conversion in config section <dataTypeMapping> \n"
															+"\t<dataType name=\"" + typeName 
															+"\">\n\t\t<column schema=\"*\" table=\"*\" column=\"*\" dataType=\"[REPLACE THIS WITH TARGET DATA TYPE]\" convert=\"[REPLACE WITH CONVERSION EXPRESSION USING {COLUMN} AS PLACEHOLDER]\"/>\n"
															+ "\t</dataType>\n");
				}
				else
				{
					result.setDataType(convertDataTypeNameToJavaSQLType(temp));
					result.setTypeName(temp);
				}
				break;
			}
		}

		if (config.isAddColumnDefault()) {
			String colDefault = sourceColumnDefaults.get(sourceCol.getColumnName());

			if (colDefault != null) {
				if (dataTypeSupportsDefault(result.getDataType())) {
					result.setColumnDefault(getConvertedDefault(result.getDataType(), colDefault));
				} else {
					MigrateDatabaseUtils.writeErrorMessage("DEFAULT for " + sourceCol.getColumnName()
							+ " not supported for target column definition.");
					result.setColumnDefault(null);
				}
			}
			else
			{
				result.setColumnDefault(null);
			}
		}
		else
		{
			// remove default if copied from source column
			result.setColumnDefault(null); 
		}
		

		result.setColumnName(getConvertedColumnName(sourceCol.getColumnName(), config.columnNameReplacements,
				config.doubleQuoteColumnNames));

		return result;

	}

	private String getColumnCreateText(ColumnMetadata column) {
		String columnTypeName = column.getTypeName();
		if (columnTypeName.equals("DECIMAL")) {
			columnTypeName = columnTypeName + "(" + column.getColumnSize() + "," + column.getDecimalDigits() + ")";
		}
		LOG.info("TARGET COLUMN TYPE NAME: " + columnTypeName);
		String result = column.getColumnName() + MigrateDatabaseConstants.TAB + columnTypeName;

		if (column.getIsNullable().equals("NO")) {
			result = result + MigrateDatabaseConstants.TAB + "NOT NULL";
		}

		if (column.getIsAutoIncrement().equals("YES")) {
			result = result + MigrateDatabaseConstants.SPACE
					+ "generated always as identity (START WITH 1, INCREMENT BY 1)";
		}

		String defaultVal = column.getColumnDefault();
		if (defaultVal != null) {
			result = result + " DEFAULT " + defaultVal;
		}

		return result;
	}

	private Integer convertDataTypeNameToJavaSQLType(String typeName) {
		int result = java.sql.Types.OTHER;
		String typeUpper = typeName.toUpperCase();
		
		
		if (typeUpper.equals( "DATE")) {
			result = java.sql.Types.DATE;
			}
		else if (typeUpper.equals( "BIGINT")) {
			result = java.sql.Types.BIGINT;
			}
		else if (typeUpper.equals( "BINARY")) {
			result = java.sql.Types.BINARY;
			}
		else if (typeUpper.equals( "BLOB")) {
			result = java.sql.Types.BLOB;
			}
		else if (typeUpper.equals( "VARBINARY")) {
			result = java.sql.Types.VARBINARY;
			}
		else if (typeUpper.equals( "BIT")) {
			result = java.sql.Types.BIT;
			}
		else if (typeUpper.equals( "BOOLEAN")) {
			result = java.sql.Types.BOOLEAN;
			}
		else if (typeUpper.equals( "NCHAR")) {
			result = java.sql.Types.NCHAR;
			}
		else if (typeUpper.equals( "CHAR")) {
			result = java.sql.Types.CHAR;
			}
		else if (typeUpper.equals( "CLOB")) {
			result = java.sql.Types.CLOB;
			}
		else if (typeUpper.equals( "TIMESTAMP")) {
			result = java.sql.Types.TIMESTAMP;
			}
		else if (typeUpper.equals( "DECIMAL")) {
			result = java.sql.Types.DECIMAL;
			}
		else if (typeUpper.equals( "NUMERIC")) {
			result = java.sql.Types.NUMERIC;
			}
		else if (typeUpper.equals( "DOUBLE")) {
			result = java.sql.Types.DOUBLE;
			}
		else if (typeUpper.equals( "FLOAT")) {
			result = java.sql.Types.FLOAT;
			}
		else if (typeUpper.equals( "INTEGER")) {
			result = java.sql.Types.INTEGER;
			}
		else if (typeUpper.equals( "REAL")) {
			result = java.sql.Types.REAL;
			}
		else if (typeUpper.equals( "SMALLINT")) {
			result = java.sql.Types.SMALLINT;
			}
		else if (typeUpper.equals( "TINYINT")) {
			result = java.sql.Types.TINYINT;
			}
		else if (typeUpper.equals( "TIME")) {
			result = java.sql.Types.TIME;
			}
		else if (typeUpper.equals( "NVARCHAR")) {
			result = java.sql.Types.NVARCHAR;
			}
		else if (typeUpper.equals( "VARCHAR")) {
			result = java.sql.Types.VARCHAR;
			}

		
		return result;
	}

	/**
	 * Converts a list of source columns into the list of columns converted into
	 * splice machine supported names and types.
	 * 
	 * @param sourceColumns
	 * @param vendor
	 * @param conn
	 * @param schema
	 * @param tableName
	 * @return
	 * @throws Exception
	 */
	public HashMap<Integer,ColumnMetadata> convertColumnMetadata(List<ColumnMetadata> sourceColumns, BaseDatabaseVendor vendor,
			Connection conn, String schema, String tableName, HashMap<String, String> tableColumnDefaults)
			throws Exception {

		HashMap<Integer,ColumnMetadata> result = new HashMap<Integer,ColumnMetadata>();

		for (ColumnMetadata sourceCol : sourceColumns) {
			ColumnMetadata targetCol = getConvertedColumnMetadata(sourceCol, vendor, conn, schema, tableName,
					tableColumnDefaults);
			result.put(sourceCol.getOrdinalPosition(), targetCol);
		}

		return result;
	}

	/**
	 * Populates the tableList ArrayList
	 * 
	 * @param md
	 * @param catalog
	 * @throws SQLException
	 */
	public ArrayList<Table> getListOfTables(BaseDatabaseVendor vendor, Connection con, DatabaseMetaData md,
			final String schema) throws SQLException {

		ArrayList<Table> tableList = new ArrayList<Table>();
		final ResultSet rs = vendor.getTables(con, md, null, schema);

		ArrayList<String> tableExlusions = config.getSchemaExclusions(schema);
		if (tableExlusions == null) {
			tableExlusions = new ArrayList<String>();
		}

		int count = 0;

		while (rs.next()) {
			count++;
			String schemaName = rs.getString("TABLE_SCHEM");
			String tableName = rs.getString("TABLE_NAME");

			if (schemaName != null) {
				schemaName = schemaName.trim();
			}

			if (!vendor.isValidSchema(schemaName)) {
				continue;
			}

			// it is not to be processed.
			if (tableExlusions.contains(tableName)) {
				continue;
			}

			tableList.add(new Table(schemaName, tableName, config.schemaNameMapping));
			if (config.debug)
				MigrateDatabaseUtils.writeMessage("FOUND TABLE: " + tableName);
		}

		MigrateDatabaseUtils.writeMessage("**** NO TABLES ");

		rs.close();
		return tableList;
	}

	/**
	 * Loops through all the tables and retrieves the counts for the table
	 * 
	 * @param st
	 * @throws SQLException
	 */
	public void setTableCounts(Connection dbConn, ArrayList<Table> tableList) throws SQLException {
		Statement st = dbConn.createStatement();
		for (Table table : tableList) {
			String sqlCountStmt = "select count(*) from " + table.getSourceFullTableName(true);
			if (config.debug) {
				MigrateDatabaseUtils.writeMessage("GETTING COUNT FOR TABLE: " + table.getSourceFullTableName(true));
			}
			final ResultSet rs2 = st.executeQuery(sqlCountStmt);
			if (rs2.next()) {
				table.setNumRecords(rs2.getLong(1));
			}
		}
	}

	/**
	 * Sets the foreign keys for a table. Useful when deciding the create or load
	 * sequence of files.
	 * 
	 * @param md
	 * @throws SQLException
	 */
	public HashMap<String, String> setTableForeignKeys(DatabaseMetaData md, ArrayList<Table> tableList)
			throws SQLException {

		HashMap<String, String> foreignKeyList = new HashMap<String, String>();
		for (Table table : tableList) {
			if (config.debug) {
				MigrateDatabaseUtils.writeMessage("GETTING FOREIGN KEY FOR TABLE: " + table.getSourceFullTableName());
			}
			ResultSet columns = md.getExportedKeys(null, "public", table.getSourceTableName());
			while (columns.next()) {
				table.addForeignKey(columns.getString("FKTABLE_NAME"));
				foreignKeyList.put(columns.getString("FKTABLE_NAME"), columns.getString("FKTABLE_NAME"));
			}
			columns.close();
		}
		return foreignKeyList;
	}

	/**
	 * Determines the sequence that the load file or the scripts need to be created
	 * in.
	 * 
	 */
	public void determineLoadSequence(ArrayList<Table> tables) {
		for (Table table : tables) {
			push(table, -1, tables);
		}
		if (config.debug) {
			MigrateDatabaseUtils.writeMessage("SORTING TABLE LIST");
		}

		Collections.sort(tables, Table.getComparator());
	}

	/**
	 * 
	 * @param table
	 * @param references
	 * @param previous
	 */
	public void push(Table table, int previous, ArrayList<Table> tableList) {
		int actual = table.getDistance();
		actual = Math.max(previous, actual - 1) + 1;
		table.setDistance(actual);

		for (final String ref : table.getForeignKeys()) {

			Table referencedTable = findTable(tableList, ref);

			if (!table.getSourceTableName().equals(ref) && referencedTable != null) {
				push(referencedTable, actual, tableList);
			}
		}
	}

	public static Table findTable(ArrayList<Table> tables, String tableName) {
		Table rtnVal = null;
		for (Table t : tables) {
			if (t.getSourceTableName().equals(tableName)) {
				rtnVal = t;
			}
		}
		return rtnVal;
	}

	public void exportData(BaseDatabaseVendor dbVendor, Connection con, ArrayList<Table> tableNameList, HashMap<String, TableConversionMetadata> tableConversions)
			throws Exception {

		BaseOutput output = config.getTargetOutput();
		long maxRecords = config.getMaxRecordsPerTable();
	

		for (Table table : tableNameList) {

			// find conversion for source table
			TableConversionMetadata tableConversion = tableConversions.get(table.getSourceFullTableName());
			
			String sql = getSourceSelectSQL( 
					        dbVendor,
							table.getSourceFullTableName(), 
							tableConversion.getSourceColumns(), 
							tableConversion.getTargetColumns(), 
							false,
							maxRecords,
							true); //TODO skip LOBs for now, potentially address this with VTI

			
			MigrateDatabaseUtils.writeMessage("Export sql:" + sql);

			Statement st = con.createStatement();
			ResultSet res = st.executeQuery(sql);
			ResultSetMetaData resMeta = res.getMetaData();

			output.startExportData(table, resMeta);
			output.ouputExportData(table, resMeta, res);
			output.endExportData();
		}
	}

	/**
	 * Returns the number of columns in resultset
	 * 
	 * @param res
	 * @return
	 * @throws SQLException
	 */
	public int getColumnCount(ResultSet res) throws SQLException {
		return res.getMetaData().getColumnCount();
	}

	public void createTableCountFile(ArrayList<Table> tables) {
		PrintWriter countFile = null;
		try {

			countFile = new PrintWriter(this.config.getScriptOutputPath() + "table_counts.txt", "UTF-8");

			for (Table table : tables) {
				countFile.write(table.getSourceFullTableName() + "," + table.getNumberOfRecords());
				countFile.write(MigrateDatabaseConstants.NEW_LINE);
			}
			countFile.flush();
		} catch (Exception e) {
			MigrateDatabaseUtils.logError("error creating count file", e);
		} finally {
			if (countFile != null) {
				countFile.close();
			}
		}
	}

	public void exportPackage(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor) {

		String[] packages = null;
		if (config.isExportAllPackages()) {
			packages = vendor.getListOfPackage(conn);
		} else {
			packages = StringUtils.split(config.packageList, ",");
		}

		if (packages != null) {
			for (int i = 0; i < packages.length; i++) {
				String[] packageDetails = StringUtils.split(packages[i], ".");
				if (packageDetails.length == 2) {
					vendor.exportPackage(conn, packageDetails[0], packageDetails[1],
							config.getScriptOutputPath() + config.getExportPackageDirectory());
				} else {
					vendor.exportPackage(conn, null, packageDetails[0],
							config.getScriptOutputPath() + config.getExportPackageDirectory());
				}
			}
		}
	}

	public void exportTriggers(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor) {

		String[] triggers = null;
		if (config.isExportAllProcedures()) {
			triggers = vendor.getListOfTriggers(conn);
		} else {
			triggers = StringUtils.split(config.triggerList, ",");
		}

		if (triggers != null) {
			for (int i = 0; i < triggers.length; i++) {
				String[] details = StringUtils.split(triggers[i], ".");
				if (details.length == 2) {
					vendor.exportTrigger(conn, details[0], details[1],
							config.getScriptOutputPath() + config.getExportTriggersDirectory());
				} else {
					vendor.exportTrigger(conn, null, details[0],
							config.getScriptOutputPath() + config.getExportTriggersDirectory());
				}
			}
		}
	}

	public void exportProcedure(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor) {

		String[] procedures = null;
		if (config.isExportAllProcedures()) {
			procedures = vendor.getListOfProcedures(conn);
		} else {
			procedures = StringUtils.split(config.procedureList, ",");
		}

		if (procedures != null) {
			for (int i = 0; i < procedures.length; i++) {
				String[] details = StringUtils.split(procedures[i], ".");
				if (details.length == 2) {
					vendor.exportProcedure(conn, details[0], details[1],
							config.getScriptOutputPath() + config.getExportProcedureDirectory());
				} else {
					vendor.exportProcedure(conn, null, details[0],
							config.getScriptOutputPath() + config.getExportProcedureDirectory());
				}
			}
		}
	}

	public void exportFunction(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor) {

		String[] functions = null;
		if (config.isExportAllFunctions()) {
			functions = vendor.getListOfFunctions(conn);
		} else {
			functions = StringUtils.split(config.functionList, ",");
		}

		if (functions != null) {
			for (int i = 0; i < functions.length; i++) {
				String[] details = StringUtils.split(functions[i], ".");
				if (details.length == 2) {
					vendor.exportFunction(conn, details[0], details[1],
							config.getScriptOutputPath() + config.getExportFunctionDirectory());
				} else {
					vendor.exportFunction(conn, null, details[0],
							config.getScriptOutputPath() + config.getExportFunctionDirectory());
				}
			}
		}
	}

	public void exportViews(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor) {

		String[] views = null;
		if (config.isExportAllViews()) {
			views = vendor.getListOfViews(conn);
		} else {
			views = StringUtils.split(config.viewList, ",");
		}

		if (views != null) {
			for (int i = 0; i < views.length; i++) {
				MigrateDatabaseUtils.writeMessage("Exporting View:" + views[i]);
				String[] details = StringUtils.split(views[i], ".");
				if (details.length == 2) {
					vendor.exportView(conn, details[0], details[1],
							config.getScriptOutputPath() + config.getExportViewsDirectory());
				} else {
					vendor.exportView(conn, null, details[0],
							config.getScriptOutputPath() + config.getExportViewsDirectory());
				}
			}
		}
	}

	public void exportCheckConstraints(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor,
			ArrayList<String> schemas) throws Exception {
		BaseOutput output = config.getTargetOutput();
		output.startCreateConstraint(schemas);
		vendor.exportCheckConstraints(conn, schemas, output);
		output.endCreateConstraint();
	}

	/**
	 * Returns a list of database users. Currently oracle is the only database that
	 * returns the list of users.
	 * 
	 * @param conn
	 * @param meta
	 * @param vendor
	 * @param schemas
	 * @return
	 */
	public ArrayList<String> getUsers(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor,
			ArrayList<String> schemas) {
		return vendor.getUsers(conn, config.getScriptOutputPath());
	}

	/**
	 * Creates a file to
	 * 
	 * @param users
	 */
	public void exportUsers(ArrayList<String> users) {
		BaseOutput output = config.getTargetOutput();
		output.createUsers(users);
	}

	public void exportRoles(Connection conn, DatabaseMetaData meta, BaseDatabaseVendor vendor,
			ArrayList<String> schemas) {
		vendor.exportRoles(conn, config.getScriptOutputPath());

	}

	/**
	 * Creates the sqoop scripts needed for the migration process
	 * 
	 * @param currentSchema
	 * @param tableList
	 */
	public void createSqoopScripts(String currentSchema, ArrayList<Table> tableList) {

		try {
			// Create the directory where the sqoop scripts reside
			String sqoopDir = this.config.getScriptOutputPath() + this.config.getSqoopFilesSubDirectory();
			File temp = new File(sqoopDir);
			if (!temp.exists()) {
				temp.mkdirs();
			}

			// Print out the list of tables for the schema to a file
			String fileName = sqoopDir + "/" + currentSchema.toLowerCase() + "-tables.txt";
			int numTables = tableList.size();

			File myFile = new File(fileName);
			PrintWriter sqoopOut = null;
			if (myFile.exists() && !myFile.isDirectory()) {
				FileWriter fw = new FileWriter(myFile, true);
				sqoopOut = new PrintWriter(fw);
			} else {
				sqoopOut = new PrintWriter(fileName, "UTF-8");
			}

			for (int i = 0; i < numTables; i++) {
				Table table = tableList.get(i);
				sqoopOut.println(table.getSourceFullTableName());
			}
			sqoopOut.close();

			String extractFileName = sqoopDir + "/extract-" + currentSchema.toLowerCase() + "-full.sh";
			sqoopOut = new PrintWriter(extractFileName, "UTF-8");
			sqoopOut.println("#!/bin/bash");
			sqoopOut.println("");
			sqoopOut.println("RUNDATE=$(date +\"%m-%d-%Y_%T\")");
			sqoopOut.println("CONFIG=" + config.getSqoopConfigFile());
			sqoopOut.println(
					"TABLES=" + config.getSqoopTableListPath() + "/" + currentSchema.toLowerCase() + "-tables.txt");
			sqoopOut.println("SCHEMA=" + currentSchema);
			sqoopOut.println("IMPORTDIR=" + config.getSqoopImportPath());
			sqoopOut.println("QUERYDIR=" + config.getSqoopQueryPath());
			sqoopOut.println("LOGFILE=" + config.getSqoopLogPath());
			sqoopOut.println("HADOOP_BIN=" + config.getSqoopHadoopBin());
			sqoopOut.println("SPLICE_BIN=" + config.getSqoopSpliceBin());
			sqoopOut.println("");
			sqoopOut.println(config.getSqoopDirectory()
					+ "/run-sqoop-full.sh $CONFIG $TABLES $SCHEMA $IMPORTDIR $QUERYDIR $LOGFILE $HADOOP_BIN $SPLICE_BIN > $LOGFILE 2>&1");
			sqoopOut.println("");
			sqoopOut.println("if [ $? -gt 0 ]; then");
			sqoopOut.println("        echo run-sqoop-full.sh failed");
			sqoopOut.println("else");
			sqoopOut.println("        echo run-sqoop-full.sh successful");
			sqoopOut.println("fi");
			sqoopOut.println("");
			sqoopOut.println("echo *********** Sqoop Export *************");
			sqoopOut.println("grep 'Sqoop Export ' $LOGFILE");
			sqoopOut.println("echo *********** Splice Import *************");
			sqoopOut.println("grep 'Splice Import' $LOGFILE");
			sqoopOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the grant scripts for roles specified in the config file
	 * 
	 * @param roleList
	 */
	public void createGrantScript(String currentSchema, ArrayList<Table> tableList, String type) {
		try {
			String grantDir = this.config.getScriptOutputPath() + this.config.getCreateGrantSubDirectory();
			File temp = new File(grantDir);
			if (!temp.exists()) {
				temp.mkdirs();
			}

			String fileName = grantDir + "/" + currentSchema.toLowerCase() + "-grant-" + type.toLowerCase() + ".sql";

			File myFile = new File(fileName);
			PrintWriter grantOut = null;
			if (myFile.exists() && !myFile.isDirectory()) {
				FileWriter fw = new FileWriter(myFile, true);
				grantOut = new PrintWriter(fw);
			} else {
				grantOut = new PrintWriter(fileName, "UTF-8");
			}

			String grantCmd = "";
			String roles = "";
			String permissions = "";
			if (type.equalsIgnoreCase("read")) {
				roles = config.getRolesToGrantRead();
				permissions = "SELECT";
			} else if (type.equalsIgnoreCase("write")) {
				roles = config.getRolesToGrantWrite();
				permissions = "INSERT,UPDATE,DELETE,SELECT";
			} else if (type.equalsIgnoreCase("execute")) {
				roles = config.getRolesToGrantExecute();
				permissions = "EXECUTE";
			}
			roles = roles.replace("{SCHEMA}", currentSchema);
			for (Table table : tableList) {
				grantCmd = "GRANT " + permissions + " ON " + table.getTargetFullTableName(true).toUpperCase() + " TO "
						+ roles + ";";
				grantOut.println(grantCmd);
			}
			grantOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates the role scripts for roles specified in the config file
	 * 
	 * @param roleList
	 */
	public void createRoleScript(String currentSchema, ArrayList<String> roleList) {

		try {
			String roleDir = this.config.getScriptOutputPath() + this.config.getCreateRoleSubDirectory();
			File temp = new File(roleDir);
			if (!temp.exists()) {
				temp.mkdirs();
			}

			String fileName = roleDir + "/" + currentSchema.toLowerCase() + "-create-roles.sql";
			PrintWriter roleOut = new PrintWriter(fileName, "UTF-8");
			String roleCmd;
			for (String role : roleList) {
				roleCmd = role.replace("{SCHEMA}", currentSchema);
				roleOut.println("CREATE ROLE " + roleCmd + ";");
			}
			roleOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
