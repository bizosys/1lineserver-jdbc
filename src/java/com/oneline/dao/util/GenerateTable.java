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

import java.util.List;

public class GenerateTable extends Generate {

	private static StringBuilder builder = new StringBuilder(1000);
	
	public void process(DbSchema schema) {
		builder.delete(0, builder.length());
		
		int tableT = schema.tables.size(); 
		for ( int i=0; i< tableT; i++ ) {
			DbTable table =  schema.tables.get(i);

			builder.append("import java.util.List;\n");
			builder.append("import java.sql.SQLException;\n\n");
			builder.append("import com.oneline.dao.ReadObject;\n");
			builder.append("import com.oneline.dao.ReadXml;\n");
			builder.append("import com.oneline.dao.WriteBase;\n\n");
			
			builder.append("\npublic class ").append(toCamelCase(table.name, true)).append("Table {\n\n");

			builder.append("\t/** The VO Class */\n\t" );
			builder.append("public static final Class<" + 
				toCamelCase(table.name, true) + "> clazz = ").append(toCamelCase(table.name, true)).append(".class;\n\n");
			
			builder.append("\t/** The SQL Select statement */\n" );
			sqlSelect(table, builder, "\t");

			builder.append("\t/** The SQL Select statements of all records */\n" );
			sqlSelectAll(table, builder, "\t");

			builder.append("\t/** The SQL Select statements on indexed fields and primary keys */\n" );
			sqlSelectBy(table, builder, "\t");

			builder.append("\t/** The SQL Insert statement with auto increment */\n" );
			sqlInsert(table, builder, "\t");
			
			builder.append("\t/** The SQL Insert statement with primary key */\n" );
			if ( table.primaryKeys.size() > 0 ) {
				sqlInsertWithPK(table, builder, "\t");
			}

			builder.append("\t/** The SQL Update statement */\n" );
			sqlUpdate(table, builder, "\t");
			
			builder.append("\n\n\t/** The protected constructor. All methods are static public */" );
			builder.append("\n\tprotected ").append(toCamelCase(table.name, true)).append("Table() {\n");
			builder.append("\t}\n\n");
			
			builder.append("\n\t/** Sql select functions */\n" );
			if ( table.primaryKeys.size() > 0 ) {
				sqlSelectByFunc(table, builder, "\t");
			}
			
			
			builder.append("\n\t/** Sql Insert with Auto increment function */\n" );
			sqlInsertFunc(table, builder, "\t");
			
			builder.append("\n\t/** Sql Insert with PK function */\n" );
			sqlInsertPKFunc(table, builder, "\t");

			builder.append("\n\t/** Sql Update function */\n" );
			sqlUpdateFunc(table, builder, "\t");

			builder.append("}\n\n\n\n"); //Class declation ends
			
			System.out.println(builder.toString());
			
		}
		
	}
	
	private void sqlSelect(DbTable table,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("public static String sqlSelect =\n");
		builder.append(space).append("\t\"select ");
		createColumns(table.columns,builder,", ","", space, true);
		builder.append(space).append(" from ").append(table.name).append("\";\n\n");
	}
	
	private void sqlSelectBy(DbTable table,
			StringBuilder builder, String space ) {
		
		for ( int i=0; i < table.uniqueIndexes.size(); i++ ) {
			sqlSelectAppend(table.uniqueIndexes.get(i), builder, space);
		}

		for ( int i=0; i < table.nonUniqueIndexes.size(); i++ ) {
			sqlSelectAppend(table.nonUniqueIndexes.get(i), builder, space);
		}

	}
	
	private void sqlSelectAppend(String by,
	StringBuilder builder, String space) {
		if ( null == by ) return;
		builder.append(space).append("protected static String ");
		builder.append(this.selectBy(by));
		builder.append(" = sqlSelect + \" where ");
		builder.append(by);
		builder.append(" = ?\";\n\n");
	}

	private void sqlInsert(DbTable table,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("protected static String sqlInsert =\n");
		builder.append(space).append("\t\"insert into ").append(table.name).append(" (");
		createColumns(table.getNonPKCols(),builder,", ","", space, false);
		builder.append(space).append(" ) \" + \n ").append(space);
		builder.append(space).append("\"values (");
		generateQuestions(table.getNonPKCols().size(),builder);
		builder.append(")\";\n\n");
	}
	
	private void sqlInsertWithPK(DbTable table,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("protected static String sqlInsertPK =\n");
		builder.append(space).append("\t\"insert into ").append(table.name).append(" (");
		createColumns(table.columns,builder,", ","", space, false);
		builder.append(space).append(" ) \" + \n ").append(space);
		builder.append(space).append("\"values (");
		generateQuestions(table.columns.size(),builder);
		builder.append(")\";\n\n");
	}

	private void sqlUpdate(DbTable table,
			StringBuilder builder, String space ) {
	
		builder.append(space).append("protected static String sqlUpdate =\n");
		builder.append(space).append("\t\"update ").append(table.name).append(" SET ");
		createColumns(table.getNonPKCols(),builder," = ?, "," = ? ",space, false);
		builder.append("\" + \n").append(space).append("\t\"");
		builder.append("where ");

		for ( int i=0; i < table.primaryKeys.size(); i++  ) {
			builder.append(table.primaryKeys.get(i));
			if ( i != table.primaryKeys.size() - 1) {
				builder.append(" = ? and ");
			} else {
				builder.append(" = ?");
			}
		}
		
		builder.append(space).append("\";\n\n");
	}	

	private void createColumns(List<DbColumn> columns,
			StringBuilder builder, 
			String columnSeparator, String endSeparator,
			String space, boolean as ) {
		
		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			if ( as ) builder.append(selectAs(column.name));
			else builder.append(column.name);
			
			if ( col != columnsT - 1) {
				builder.append(columnSeparator);
				if ( col == 5 || col == 10 || col == 15 || col == 20 ) {
					builder.append("\" + \n").append(space).append("\t\"");
				}
				
			} else {
				builder.append(endSeparator);
			}
		}
	}
	
	private void generateQuestions(int total, StringBuilder builder) {
		for ( int i=0; i< total; i++) {
			builder.append("?");
			if ( i != total - 1) builder.append(", ");
			else builder.append(" ");
		}
	}
	
	public void sqlSelectByFunc(DbTable table,
			StringBuilder builder, String space ) {
		
		for ( int i=0; i < table.uniqueIndexes.size(); i++ ) {
			sqlSelectPKFunc(table, table.uniqueIndexes.get(i), builder, space);
			sqlSelectPKXmlFunc(table, table.uniqueIndexes.get(i), builder, space);
		}

		for ( int i=0; i < table.nonUniqueIndexes.size(); i++ ) {
			sqlSelectFunc(table, table.nonUniqueIndexes.get(i), builder, space);
			sqlSelectXmlFunc(table, table.nonUniqueIndexes.get(i), builder, space);
		}

	}
	
	private void sqlSelectAll(DbTable table, StringBuilder builder, 
			String space) {
		
		builder.append(space).append("public static List<");
		builder.append(toCamelCase(table.name, true));
		builder.append("> selectAll() throws SQLException {\n");
		builder.append(space).append("\t");
		builder.append("return new ReadObject<" + 
			toCamelCase(table.name, true) + ">(clazz).execute(sqlSelect);\n");
		builder.append(space).append("}\n\n");
	}
	
	private void sqlSelectFunc(DbTable table, String by,
	StringBuilder builder, String space) {
		if ( null == by ) return;
		builder.append(space).append("public static List<");
		builder.append(toCamelCase(table.name, true));
		builder.append("> selectBy").append(toCamelCase(by, true));
		builder.append("( Object ").append(by);
		builder.append( ") throws SQLException {\n");
		
		builder.append(space).append("\t");
		builder.append("return new ReadObject<" + 
			toCamelCase(table.name, true) + ">(clazz).execute(");
		builder.append(selectBy(by));
		builder.append(", new Object[]{").append(by).append("});\n");
		builder.append(space).append("}\n\n");
	}
	
	private void sqlSelectXmlFunc(DbTable table, String by,
	StringBuilder builder, String space) {
		if ( null == by ) return;
		builder.append(space).append("public static List<");
		builder.append("String");
		builder.append("> selectXmlBy").append(toCamelCase(by, true));
		builder.append("( Object ").append(by);
		builder.append( ") throws SQLException {\n");
		
		builder.append(space).append("\t");
		builder.append("return new ReadXml<" + 
			toCamelCase(table.name, true) + ">(clazz).execute(");
		builder.append(selectBy(by));
		builder.append(", new Object[]{").append(by).append("});\n");
		builder.append(space).append("}\n\n");
	}

	private void sqlSelectPKFunc(DbTable table, String by,
		StringBuilder builder, String space) {
			if ( null == by ) return;
			builder.append(space).append("public static ");
			builder.append(toCamelCase(table.name, true));
			builder.append(" selectBy").append(toCamelCase(by, true));
			builder.append("( Object ").append(by);
			builder.append( ") throws SQLException {\n");
			
			builder.append(space).append("\t");
			builder.append("Object record = new ReadObject<" + 
				toCamelCase(table.name, true) + ">(clazz).selectByPrimaryKey(");
			builder.append(selectBy(by));
			builder.append(",").append(by).append(");\n");
			builder.append(space).append("\t");
			builder.append("if ( null == record) return null;\n");
			builder.append(space).append("\t");
			builder.append("return (").append(toCamelCase(table.name, true)).append(") record;\n");
			builder.append(space).append("}\n\n");
		}
			
	private void sqlSelectPKXmlFunc(DbTable table, String by,
	StringBuilder builder, String space) {
		if ( null == by ) return;
		builder.append(space).append("public static String");
		builder.append(" selectXmlBy").append(toCamelCase(by, true));
		builder.append("( Object ").append(by);
		builder.append( ") throws SQLException {\n");
		
		builder.append(space).append("\t");
		builder.append("Object record = new ReadXml<" + 
			toCamelCase(table.name, true) + ">(clazz).selectByPrimaryKey(");
		builder.append(selectBy(by));
		builder.append(",").append(by).append(");\n");
		builder.append(space).append("\t");
		builder.append("if ( null == record) return null;\n");
		builder.append(space).append("\t");
		builder.append("return (String) record;\n");
		builder.append(space).append("}\n\n");
	}
			
	private void sqlInsertFunc(DbTable table, 
	StringBuilder builder, String space) {
		builder.append(space).append("public static void insert( ");
		builder.append(toCamelCase(table.name, true));
		builder.append(" record, WriteBase sqlWriter) throws SQLException {");
		builder.append("\n").append(space).append("\t");
		builder.append("if ( sqlWriter == null ) {");
		builder.append("\n").append(space).append("\t\t");
		builder.append("sqlWriter = new WriteBase();");
		builder.append("\n").append(space).append("\t");
		builder.append("}\n");
		builder.append("\n").append(space).append("\t");
		if ( table.primaryKeys.size() == 1 ) {
			builder.append("record.").append(table.primaryKeys.get(0)).append(" = ");
			builder.append("sqlWriter.insert(sqlInsert, record.getNewPrint());");
		} else {
			builder.append("sqlWriter.execute(sqlInsert, record.getNewPrint());");
		}
		
		builder.append("\n").append(space).append("}\n\n");
	}	
	
	private void sqlInsertPKFunc(DbTable table, 
			StringBuilder builder, String space) {
				builder.append(space).append("public static void insertPK( ");
				builder.append(toCamelCase(table.name, true));
				builder.append(" record, WriteBase sqlWriter) throws SQLException {");
				builder.append("\n").append(space).append("\t");
				builder.append("if ( sqlWriter == null ) {");
				builder.append("\n").append(space).append("\t\t");
				builder.append("sqlWriter = new WriteBase();");
				builder.append("\n").append(space).append("\t");
				builder.append("}\n");
				builder.append("\n").append(space).append("\t");
				builder.append("sqlWriter.execute(sqlInsertPK, record.getNewPrintWithPK());");
				builder.append("\n").append(space).append("}\n\n");
	}		

	private void sqlUpdateFunc(DbTable table, 
		StringBuilder builder, String space) {
			builder.append(space).append("public static void update( ");
			builder.append(toCamelCase(table.name, true));
			builder.append(" record, WriteBase sqlWriter) throws SQLException {");
			builder.append("\n").append(space).append("\t");
			builder.append("if ( sqlWriter == null ) {");
			builder.append("\n").append(space).append("\t\t");
			builder.append("sqlWriter = new WriteBase();");
			builder.append("\n").append(space).append("\t");
			builder.append("}\n");
			builder.append("\n").append(space).append("\t");
			builder.append("sqlWriter.execute(sqlUpdate, record.getExistingPrint());");
			builder.append("\n").append(space).append("}\n\n");
	}	

	private String selectBy(String by) {
		return "sqlSelectBy" + by;
	}
	
	private String selectAs(String field) {
		String camelField = toCamelCase(field, false);
		if ( field.equals(camelField)) return field; 
		else return (field + " as " + camelField);  
		
	}
}