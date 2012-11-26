/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.oneline.dao.util;

import java.util.ArrayList;
import java.util.List;

public class GenerateVO extends Generate {

	private static StringBuilder builder = new StringBuilder(1000);
	
	public void process(DbSchema schema) {
		builder.delete(0, builder.length());
		
		int tableT = schema.tables.size(); 
		for ( int i=0; i< tableT; i++ ) {

			builder.append("\nimport java.io.PrintWriter;");
			builder.append("\nimport java.util.List;");
			DbTable table =  schema.tables.get(i);
			builder.append("\npublic class ").append(toCamelCase(table.name, true)).append(" {\n\n");

			//Field Variables
			clazzVariables(table.columns,builder,"\t");
			
			builder.append("\n\t/** Default constructor */");
			builder.append("\n\tpublic ").append(toCamelCase(table.name, true)).append("() {\n");
			builder.append("\t}\n\n");
			
			builder.append("\n\t/** Constructor with primary keys (Insert with primary key)*/\n");
			constuctorSignature(table, table.columns,builder,"\t");
			thisAssginment(table.columns, builder, "\t\t");
			builder.append("\n\t}\n\n");
			
			builder.append("\n\t/** Constructor with Non Primary keys (Insert with autoincrement)*/\n");
			constuctorSignature(table, table.getNonPKCols(),builder,"\t");
			thisAssginment(table.getNonPKCols(), builder, "\t\t");
			builder.append("\n\t}\n\n");
			
			builder.append("\n\t/** Params for (Insert with autoincrement)*/\n");
			createNewPrint(table.getNonPKCols(), builder, "\t");
			builder.append("\n\t/** Params for (Insert with primary key)*/\n");
			createNewPrintWithPK(table.columns, builder, "\t");
			builder.append("\n\t/** Params for (Update)*/\n");
			createExistingPrint(table.columns, 
					table.primaryKeys, builder, "\t");
			
			builder.append("\n\t/** TO XML Preparation*/\n");
			toXmlString(table, builder);
			
			builder.append("\n\t/** Write XML to Print Writer */\n");
			toXmlPrintWriter(builder);
			
			builder.append("\n\t/** To Compact XML */\n");
			toCompactXmlString(table, builder);
			
			builder.append("\n\t/** To Compact XML Print Writer*/\n");
			toCompactXmlPrintWriter(builder);
			
			builder.append("\n\t/** Print to XML List */\n");
			toListXmlPrintWriter(table, builder);

			builder.append("}\n\n\n\n\n"); //Class declation ends
			
			System.out.println(builder.toString());
			
		}
		
	}
	
	public void clazzVariables(List<DbColumn> columns,
			StringBuilder builder, String space ) {
		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			builder.append(space).append("public ").append(column.javaTypeName).append(" ").append(toCamelCase(column.name, false)).append(";\n");
		}
	}
	
	public void constuctorSignature(DbTable table, List<DbColumn> columns,
			StringBuilder builder, String space ) {
		
		builder.append(space).append("public ").append(toCamelCase(table.name, true)).append("(");

		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			builder.append(column.javaTypeName).append(" ").append(toCamelCase(column.name, false));
			
			if ( col != (columnsT - 1) ) {
				builder.append(','); //Not last one
				if ( col == 3 || col == 6 || col == 9 || col == 12 || col == 15 || col == 18 ) {
					builder.append("\n").append(space).append("\t");
				}
			} else {
				builder.append(") {\n\n"); //Last one
			}
		}
	}

	
	public void thisAssginment(List<DbColumn> columns,
			StringBuilder builder, String space ) {
		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			builder.append(space).append("this.").append(toCamelCase(column.name, false)).append(" = ").append(toCamelCase(column.name, false)).append(";\n");
		}
	}
	
	public void createNewPrint(List<DbColumn> columns,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("public Object[] getNewPrint() {\n");
		createObjectArray(columns,builder,space);
		builder.append(space).append("}\n\n");
	}
	
	public void createNewPrintWithPK(List<DbColumn> columns,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("public Object[] getNewPrintWithPK() {\n");
		createObjectArray(columns,builder,space);
		builder.append(space).append("}\n\n");
	}

	public void createExistingPrint( List<DbColumn> allcolumns,
			List<String> pks,
			StringBuilder builder, String space ) {
	
		List<DbColumn> appendedPKcolumns = new ArrayList<DbColumn>();
		List<DbColumn> pkColumns = new ArrayList<DbColumn>();
		
		int columnsT = allcolumns.size();
		int pksT = pks.size();
		boolean isMatched = false;
		
		for (int i=0; i< columnsT; i++ ) {
			DbColumn aColumn = allcolumns.get(i);
			isMatched = false;
			
			for (int j=0; j< pksT; j++ ) {
				if ( pks.get(j).equals(aColumn.name) ) {
					pkColumns.add(aColumn);
					isMatched = true;
					break;
				}
			}
			if ( !isMatched ) appendedPKcolumns.add(aColumn);
		}
		
		int pkColumnsT = pkColumns.size();
		for (int i=0; i< pkColumnsT; i++ ) {
			appendedPKcolumns.add(pkColumns.get(i));
		}
		builder.append(space).append("public Object[] getExistingPrint() {\n");
		createObjectArray(appendedPKcolumns,builder,space);
		builder.append(space).append("}\n\n");
	}

	public void createObjectArray(List<DbColumn> columns,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("\t").append(
				"return new Object[] {\n");
		builder.append(space).append("\t\t");

		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			builder.append(toCamelCase(column.name, false));
			if ( col != columnsT - 1) {
				builder.append(", ");
				if ( col == 5 || col == 10 || col == 15 || col == 20 ) {
					builder.append("\n").append(space).append("\t\t");
				}
				
			} else {
				builder.append("\n");
			}
		}
		builder.append(space).append("\t};\n");
	}

	
	public void toXmlString(DbTable table, StringBuilder builder ) {
		
		builder.append('\t').append("public String toXml() {\n");
		
		builder.append("\t\tStringBuilder sb = new StringBuilder(1024);\n");
		String docName = toCamelCase(table.name, false);
		builder.append("\t\tsb.append(\"<").append(docName).append(">\");\n");
		
		for (DbColumn column : table.columns) {
			String colName = toCamelCase(column.name, false);
			builder.append("\t\tsb.append(\"<").append(colName).append(">\")");

			switch(column.dataType) {
				case java.sql.Types.VARCHAR:
				case java.sql.Types.LONGVARCHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.CHAR:
					builder.append(".append(\"<![CDATA[\").append(this.").append(colName).append(").append(\"]]>\")");
					break;
				default:
					builder.append(".append(this.").append(colName).append(")");
					break;
			}
			builder.append(".append(\"</").append(colName).append(">\");\n");
		}
		
		builder.append("\t\tsb.append(\"</").append(docName).append(">\");\n");
		builder.append("\t\treturn sb.toString();\n");
		builder.append("\t}\n");
	}	
	
	public void toXmlPrintWriter(StringBuilder builder) {
		builder.append('\t').append("public void toXmlPrintWriter(PrintWriter writer) {\n");
		builder.append('\t').append("\twriter.print(toXml());\n\t}\n");
	}
	
	public void toCompactXmlString(DbTable table, StringBuilder builder ) {
		if ( table.columns.size() > 25) return;
		
		char[] colShortCodes = "abcdefghijklmnopqstuvwxyz".toCharArray();
		
		builder.append('\t').append("public String toCompactXml(String recordTag) {\n");
		
		builder.append("\t\tStringBuilder sb = new StringBuilder(1024);\n");
		builder.append("\t\tif (null == recordTag) sb.append(\"<r>\");\n");
		builder.append("\t\telse sb.append('<').append(recordTag).append('>');\n");

		int colsT = table.columns.size();
		for (int i=0; i<colsT; i++) {
			DbColumn column = table.columns.get(i);
			String colName = toCamelCase(column.name, false);
			builder.append("\t\tsb.append(\"<").append(colShortCodes[i]).append(">\")");

			switch(column.dataType) {
				case java.sql.Types.VARCHAR:
				case java.sql.Types.LONGVARCHAR:
				case java.sql.Types.NCHAR:
				case java.sql.Types.CHAR:
					builder.append(".append(\"<![CDATA[\").append(this.").append(colName).append(").append(\"]]>\")");
					break;
				default:
					builder.append(".append(this.").append(colName).append(")");
					break;
			}
			builder.append(".append(\"</").append(colShortCodes[i]).append(">\");\n");
		}
		
		builder.append("\t\tif (null == recordTag) sb.append(\"</r>\");\n");
		builder.append("\t\telse sb.append(\"</\").append(recordTag).append('>');\n");

		builder.append("\t\treturn sb.toString();\n");
		builder.append("\t}\n");
	}
	
	public void toCompactXmlPrintWriter(StringBuilder builder) {
		builder.append("\tpublic void toCompactXmlPrintWriter(String recordTag, PrintWriter writer) {\n");
		builder.append("\t\twriter.print(toCompactXml(recordTag));\n\t}\n");
	}

	public void toListXmlPrintWriter(DbTable table, StringBuilder builder ) {
		if ( table.columns.size() > 25) return;
		
		String className = toCamelCase(table.name, true);
		builder.append("\tpublic static void toCompactXmlPrintWriter(String listTag,\n");
		builder.append("\t\tString recordTag, List<").append(className).append("> records, PrintWriter writer) {\n");

		String docName = toCamelCase(table.name, false) + "s";
		builder.append("\t\tif (null == recordTag) writer.print(\"<").append(docName).append(">\");\n");
		builder.append("\t\telse writer.print(\"<\" + recordTag + \">\");\n");

		builder.append("\t\tfor (").append(className).append(" record : records) {\n");
		builder.append("\t\t\trecord.toCompactXmlPrintWriter(recordTag,writer);\n");
		builder.append("\t\t}\n");

		builder.append("\t\tif (null == recordTag) writer.print(\"</").append(docName).append(">\");\n");
		builder.append("\t\telse writer.print(\"</\" + recordTag + \">\");\n");
		builder.append("\t}\n");
	}
	
}
