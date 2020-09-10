package com.splicemachine.cs.databasemigration.schema;

import java.util.ArrayList;
import java.util.HashMap;

public class TableConversionMetadata {
	ArrayList<ColumnMetadata> SourceColumns;
	HashMap<Integer,ColumnMetadata> TargetColumns;
	
	public TableConversionMetadata(ArrayList<ColumnMetadata> sourceCols, HashMap<Integer,ColumnMetadata> targetCols )
	{
		this.SourceColumns = sourceCols;
		this.TargetColumns = targetCols;
	}
	
	public ArrayList<ColumnMetadata> getSourceColumns()
	{
		return this.SourceColumns;
	}
	public HashMap<Integer,ColumnMetadata> getTargetColumns()
	{
		return this.TargetColumns;
	}
	
}
