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

public class GenerateAS extends Generate {

	private static StringBuilder builder = new StringBuilder(1000);
	
	public void process(DbSchema schema) {
		builder.delete(0, builder.length());
		
		int tableT = schema.tables.size(); 
		for ( int i=0; i< tableT; i++ ) {
			DbTable table =  schema.tables.get(i);
			builder.append("\n\t//import mx.collections.ArrayCollection;");
			builder.append("\n\timport com.oneline.util.DateHelper;");
			builder.append("\n\tpublic class ").append(toCamelCase(table.name, true)).append(" {\n\n");

			//Field Variables
			clazzVariables(table.columns,builder,"\t\t");

			builder.append("\n\t\tpublic function ").append(
					toCamelCase(table.name, true)).append("(input:XML = null) {");
			builder.append("\n\t\t\tif (input != null) this.fromXML(input);");
			builder.append("\n\t\t}\n\n");
			
			//To XML
			builder.append("\n\t\tpublic function toXML():XML {");
			builder.append("\n\t\t\tvar result:XML = new XML(\"<java.lang.Object>\"");
			
			int columnsT = table.columns.size();
			for ( int col=0; col < columnsT; col++ ) {
				DbColumn column = table.columns.get(col);
				String dataType = this.convertJavaTypeToAsType(column.javaTypeName);
				if ("int".equals(dataType) || "Number".equals(dataType)) {
					builder.append("\n\t\t\t\t+ \"<").append(toCamelCase(column.name, false)).append(">\"");
					builder.append(" + this.").append(toCamelCase(column.name, false));
					builder.append(" + \"</").append(toCamelCase(column.name, false)).append(">\"");
				} else {
				    builder.append("\n\t\t\t\t+ (this.").append(toCamelCase(column.name, false)).append(" == null ? \"\" : ");
				    builder.append("(\"<").append(toCamelCase(column.name, false)).append(">\"");
				    
				    if("Date".equals(dataType)) {
					    builder.append(" + DateHelper.toServerText(this.").append(toCamelCase(column.name, false)).append(")");
				    } else {
					    builder.append(" + this.").append(toCamelCase(column.name, false));
				    } 
				    builder.append(" + \"</").append(toCamelCase(column.name, false)).append(">\"))");
				}
			}
			builder.append(" + \"</java.lang.Object>\");"); 
			builder.append("\n\t\t\treturn result;");
			builder.append("\n\t\t}\n\n");

			builder.append("\n\t\tpublic function fromXML(input:XML):void {\n");
			for ( int col=0; col < columnsT; col++ ) {
				DbColumn column = table.columns.get(col);
				String dataType = this.convertJavaTypeToAsType(column.javaTypeName);
				if("Date".equals(dataType)) {
				    builder.append("\n\t\t\t").append(toCamelCase(column.name, false)).
				    append(" = DateHelper.fromServerText(input.").
				    append(toCamelCase(column.name, false)).append(");");
			    } else {
				    builder.append("\n\t\t\t").append(toCamelCase(column.name, false)).
				    append(" = input.").append(toCamelCase(column.name, false)).
				    append(";");
			    }
			}
			builder.append("\n\n\t\t\t//var xmllist:XMLList = input.<<foreignfields>>.children();\n");
			builder.append("\t\t\t//for ( var i:int = 0; i< xmllist.length() ; i++ ) {\n");
			builder.append("\t\t\t//\tvar foreignfield:ForeignField = new ForeignField();\n");
			builder.append("\t\t\t//\tforeignfield.fromXML(xmllist[i]);\n");
			builder.append("\t\t\t//\tforeignFieldL.push(foreignfield);\n");
			builder.append("\t\t\t//}\n");

			builder.append("\n\t\t}\n\n");
	
			builder.append("\n\t}\n\n\n"); //Class declation ends
			
			System.out.println(builder.toString());
			
		}
		
	}
	
	public void clazzVariables(List<DbColumn> columns,
			StringBuilder builder, String space ) {
		int columnsT = columns.size();
		for ( int col=0; col < columnsT; col++ ) {
			DbColumn column = columns.get(col);
			builder.append(space).append("public var ").append(
					toCamelCase(column.name, false)).append(":").append(
					convertJavaTypeToAsType(column.javaTypeName)).append(";\n");
		}
		builder.append(space).append(
				"//public var foreignFieldL:Array = new Array();\n");
	}
	
	public String convertJavaTypeToAsType( String javaType) {
		if ( DbJavaType.StringClazz.equals(javaType) ) {
			return "String";
		} else if ( DbJavaType.ObjectClazz.equals(javaType) ) {
			return "Object";
		} else if ( DbJavaType.BooleanClazz.equals(javaType) ) {
			return "Boolean";
		}else if ( DbJavaType.DateClazz.equals(javaType) ) {
			return "Date";
		}else if ( DbJavaType.DoubleClazz.equals(javaType) ) {
			return "Number";
		}else if ( DbJavaType.LongClazz.equals(javaType) ) {
			return "Long";
		}else if ( DbJavaType.FloatClazz.equals(javaType) ) {
			return "Float";
		}else if ( DbJavaType.IntegerClazz.equals(javaType) ) {
			return "int";
		}else if ( DbJavaType.ShortClazz.equals(javaType) ) {
			return "Short";
		}else if ( DbJavaType.ByteClazz.equals(javaType) ) {
			return "Byte";
		} else if ( DbJavaType.BigDecimalClazz.equals(javaType) ) {
			return "BigDecimal";
		}
		return null;
	}

	
}
