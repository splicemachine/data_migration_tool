package com.splicemachine.cs.databasemigration.schema;

public class DataTypeConversion {
	String targetDataType = null;
	String conversionExpression = null;

	public String getConversionExpression() {
		return conversionExpression;
	}
	public String getTargetDataType() {
		return targetDataType;
	}
	public void setConversionExpression(String conversionExpression) {
		this.conversionExpression = conversionExpression;
	}
	public void setTargetDataType(String targetDataType) {
		this.targetDataType = targetDataType;
	}
	
}
