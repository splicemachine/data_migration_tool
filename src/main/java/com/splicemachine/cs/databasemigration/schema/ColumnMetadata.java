package com.splicemachine.cs.databasemigration.schema;

import java.sql.ResultSet;
import java.sql.SQLException;

/* This class is used to hold column metadata info
	 * 
	* <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>,
    * <code>TABLE_NAME</code>, and <code>ORDINAL_POSITION</code>.
    *
    * <P>Each column description has the following columns:
    *  <OL>
    *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
    *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
    *  <LI><B>TABLE_NAME</B> String {@code =>} table name
    *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
    *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
    *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
    *  for a UDT the type name is fully qualified
    *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
    *  <LI><B>BUFFER_LENGTH</B> is not used.
    *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
    * DECIMAL_DIGITS is not applicable.
    *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
    *  <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
    *      <UL>
    *      <LI> columnNoNulls - might not allow <code>NULL</code> values
    *      <LI> columnNullable - definitely allows <code>NULL</code> values
    *      <LI> columnNullableUnknown - nullability unknown
    *      </UL>
    *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
    *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
    *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
    *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
    *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
    *       maximum number of bytes in the column
    *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
    *      (starting at 1)
    *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
    *       <UL>
    *       <LI> YES           --- if the column can include NULLs
    *       <LI> NO            --- if the column cannot include NULLs
    *       <LI> empty string  --- if the nullability for the
    * column is unknown
    *       </UL>
    *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
    *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
    *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
    *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
    *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
    *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
    *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
    *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
    *      isn't DISTINCT or user-generated REF)
    *   <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
    *       <UL>
    *       <LI> YES           --- if the column is auto incremented
    *       <LI> NO            --- if the column is not auto incremented
    *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
    *       </UL>
    *   <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
    *       <UL>
    *       <LI> YES           --- if this a generated column
    *       <LI> NO            --- if this not a generated column
    *       <LI> empty string  --- if it cannot be determined whether this is a generated column
    *       </UL>
    *  </OL>
    */
public class ColumnMetadata {

	String tableCat=null; 
	String tableSchema=null;
	String tableName=null;
	String columnName=null;
	Integer dataType=null;
	String typeName=null;
	long columnSize=0;
	Integer bufferLength=null;
	Integer decimalDigits=null;
	int numPrecRadix=0;
	Integer nullable=0;
	String remarks=null;
	String columnDefault=null;
	Integer sqlDataType=null;
    Integer sqlDateTimeSub=null;
    Integer charOctetLength=null;
    Integer ordinalPosition=null;
    String isNullable=null;
    String scopeCatalog=null;
    String scopeSchema=null;
    String scopeTable=null;
    Integer sourceDataType=null;
    String isAutoIncrement=null;
    String isGenerateColumn=null;
    String conversion=null;
    
    
	public ColumnMetadata ( ResultSet getColumnsResultSet) throws SQLException {
			setTableCat( getColumnsResultSet.getString("TABLE_CAT"));
			setTableSchema( getColumnsResultSet.getString("TABLE_SCHEM"));
			setTableName( getColumnsResultSet.getString("TABLE_NAME"));
			setColumnName( getColumnsResultSet.getString("COLUMN_NAME"));
			setDataType( getColumnsResultSet.getInt("DATA_TYPE"));
			setTypeName( getColumnsResultSet.getString("TYPE_NAME"));
			setColumnSize( getColumnsResultSet.getLong("COLUMN_SIZE"));
			setBufferLength( getColumnsResultSet.getInt("BUFFER_LENGTH"));
			setDecimalDigits( getColumnsResultSet.getInt("DECIMAL_DIGITS"));
			setNumPrecRadix( getColumnsResultSet.getInt("NUM_PREC_RADIX"));
			setNullable( getColumnsResultSet.getInt("NULLABLE"));
			setRemarks( getColumnsResultSet.getString("REMARKS"));
			setColumnDefault( getColumnsResultSet.getString("COLUMN_DEF"));
			setSqlDataType( getColumnsResultSet.getInt("SQL_DATA_TYPE"));
			setSqlDateTimeSub( getColumnsResultSet.getInt("SQL_DATETIME_SUB"));
			setCharOctetLength( getColumnsResultSet.getInt("CHAR_OCTET_LENGTH"));
			setOrdinalPosition( getColumnsResultSet.getInt("ORDINAL_POSITION"));
			setIsNullable( getColumnsResultSet.getString("IS_NULLABLE"));
			setScopeCatalog( getColumnsResultSet.getString("SCOPE_CATALOG"));
			setScopeSchema( getColumnsResultSet.getString("SCOPE_SCHEMA"));
			setScopeTable( getColumnsResultSet.getString("SCOPE_TABLE"));
			setSourceDataType( getColumnsResultSet.getInt("SOURCE_DATA_TYPE"));
			setIsAutoIncrement( getColumnsResultSet.getString("IS_AUTOINCREMENT"));
			setIsGenerateColumn( getColumnsResultSet.getString("IS_GENERATEDCOLUMN"));
		
	}
	
	public ColumnMetadata() {
		//empty definition
	}

	public void setConversion(String conversion) {
		this.conversion = conversion;
	}
	public String getConversion() {
		return conversion;
	}
	public void setTableCat(String newValue ) { 
	this.tableCat = newValue;
	}
	public void setTableSchema(String newValue ) { 
		this.tableSchema = newValue;
	}
	public void setTableName(String newValue ) { 
		this.tableName = newValue;
	}
	public void setColumnName(String newValue ) { 
		this.columnName = newValue;
	}
	public void setDataType(Integer newValue ) { 
		this.dataType = newValue;
	}
	public void setTypeName(String newValue ) { 
		this.typeName = newValue;
	}
	public void setColumnSize(long newValue ) { 
		this.columnSize = newValue;
	}
	public void setBufferLength(Integer newValue ) { 
		this.bufferLength = newValue;
	}
	public void setDecimalDigits(Integer newValue ) { 
		this.decimalDigits = newValue;
	}
	public void setNumPrecRadix(int newValue ) { 
		this.numPrecRadix = newValue;
	}
	public void setNullable(Integer newValue ) { 
		this.nullable = newValue;
	}
	public void setRemarks(String newValue ) { 
		this.remarks = newValue;
	}
	public void setColumnDefault(String newValue ) { 
		this.columnDefault = newValue;
	}
	public void setSqlDataType(Integer newValue ) { 
		this.sqlDataType = newValue;
	}
	public void setSqlDateTimeSub(Integer newValue ) { 
		this.sqlDateTimeSub = newValue;
	}
	public void setCharOctetLength(Integer newValue ) { 
		this.charOctetLength = newValue;
	}
	public void setOrdinalPosition(Integer newValue ) { 
		this.ordinalPosition = newValue;
	}
	public void setIsNullable(String newValue ) { 
		this.isNullable = newValue;
	}
	public void setScopeCatalog(String newValue ) { 
		this.scopeCatalog = newValue;
	}
	public void setScopeSchema(String newValue ) { 
		this.scopeSchema = newValue;
	}
	public void setScopeTable(String newValue ) { 
		this.scopeTable = newValue;
	}
	public void setSourceDataType(Integer newValue ) { 
		this.sourceDataType = newValue;
	}
	public void setIsAutoIncrement(String newValue ) { 
		this.isAutoIncrement = newValue;
	}
	public void setIsGenerateColumn(String newValue ) { 
		this.isGenerateColumn = newValue;
	}

	public String getTableCat(  ) { 
		return this.tableCat;
	}
	public String getTableSchema(  ) { 
		return this.tableSchema;
	}
	public String getTableName(  ) { 
		return this.tableName;
	}
	public String getColumnName(  ) { 
		return this.columnName;
	}
	public Integer getDataType(  ) { 
		return this.dataType;
	}
	public String getTypeName(  ) {
		return this.typeName;
	}
	public long getColumnSize(  ) {
		return this.columnSize;
	}
	public Integer getBufferLength(  ) { 
		return this.bufferLength;
	}
	public Integer getDecimalDigits(  ) { 
		return this.decimalDigits;
	}
	public int getNumPrecRadix(  ) { 
		return this.numPrecRadix;
	}
	public Integer getNullable(  ) { 
		return this.nullable;
	}
	public String getRemarks(  ) { 
		return this.remarks;
	}
	public String getColumnDefault(  ) { 
		return this.columnDefault;
	}
	public Integer getSqlDataType(  ) { 
		return this.sqlDataType;
	}
	public Integer getSqlDateTimeSub(  ) { 
		return this.sqlDateTimeSub;
	}
	public Integer getCharOctetLength(  ) { 
		return this.charOctetLength;
	}
	public Integer getOrdinalPosition(  ) { 
		return this.ordinalPosition;
	}
	public String getIsNullable(  ) { 
		return this.isNullable;
	}
	public String getScopeCatalog(  ) { 
		return this.scopeCatalog;
	}
	public String getScopeSchema(  ) { 
		return this.scopeSchema;
	}
	public String getScopeTable(  ) { 
		return this.scopeTable;
	}
	public Integer getSourceDataType(  ) { 
		return this.sourceDataType;
	}
	public String getIsAutoIncrement(  ) { 
		return this.isAutoIncrement;
	}
	public String getIsGenerateColumn(  ) { 
		return this.isGenerateColumn;
	}
	
	public void copyFrom(ColumnMetadata source)
	{
		setBufferLength(source.getBufferLength());
		setCharOctetLength(source.getCharOctetLength());
		setColumnDefault(source.getColumnDefault());
		setColumnName(source.getColumnName());
		setColumnSize(source.getColumnSize());
		setDataType(source.getDataType());
		setDecimalDigits(source.getDecimalDigits());
		setIsAutoIncrement(source.getIsAutoIncrement());
		setIsGenerateColumn(source.getIsGenerateColumn());
		setIsNullable(source.getIsNullable());
		setNullable(source.getNullable());
		setNumPrecRadix(source.getNumPrecRadix());
		setOrdinalPosition(source.getOrdinalPosition());
		setRemarks(source.getRemarks());
		setScopeCatalog(source.getScopeCatalog());
		setScopeSchema(source.getScopeSchema());
		setScopeTable(source.getScopeTable());
		setSourceDataType(source.getSourceDataType());
		setTableSchema( source.getTableSchema());
		setTableName( source.getTableName());
		setTypeName( source.getTypeName());
		setSqlDataType( source.getSqlDataType());
		setSqlDateTimeSub( source.getSqlDateTimeSub());
		
	}
}
