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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;

public class DbJavaType {
	
	public static String StringClazz = "String";
	public static String ObjectClazz = "Object";
	public static String BooleanClazz = "Boolean";
	public static String DateClazz = "java.sql.Timestamp";
	public static String DoubleClazz = "Double";
	public static String LongClazz = "Long";
	public static String FloatClazz = "Float";
	public static String IntegerClazz = "Integer";
	public static String ShortClazz = "Short";
	public static String ByteClazz = "Byte";
	public static String BigDecimalClazz = BigDecimal.class.getName();
		
	public static void resolve(DbColumn column) throws SQLException {
		switch (column.dataType) {

			case Types.VARCHAR:
				column.javaTypeName = StringClazz;
				break;
		
			case Types.CHAR:
				column.javaTypeName = StringClazz;
				break;

			case Types.BOOLEAN:
				column.javaTypeName = BooleanClazz;
				break;
				
			case Types.DATE:
				column.javaTypeName = DateClazz;
				break;
				
			case Types.DOUBLE:
				column.javaTypeName = DoubleClazz;
				break;
				
			case Types.FLOAT:
				column.javaTypeName = FloatClazz;
				break;
				
			case Types.INTEGER:
				column.javaTypeName = IntegerClazz;
				break;
				
			case Types.DECIMAL:
				if ( column.columnSize > 9) {
					column.javaTypeName = BigDecimalClazz;
				} else if ( column.columnSize <= 9  && column.columnSize > 4 ) {
						column.javaTypeName = DoubleClazz;
				} else {
					column.javaTypeName = FloatClazz;
				}
				break;

			case Types.NUMERIC:
				column.javaTypeName = ObjectClazz;
				if ( column.columnSize > 18 ) {
					column.javaTypeName = BigDecimalClazz;
				} else if ( column.columnSize > 9) {
					column.javaTypeName = LongClazz;
				} else if ( column.columnSize > 4) {
					column.javaTypeName = IntegerClazz;
				} else {
					column.javaTypeName = ShortClazz;
				}
				break;

			case Types.TIME:
				column.javaTypeName = DateClazz;
				break;
								
			case Types.TIMESTAMP:
				column.javaTypeName = DateClazz;
				break;
				
			case Types.TINYINT:
				column.javaTypeName = ByteClazz;
				break;
				
			case Types.SMALLINT:
				column.javaTypeName = ShortClazz;
				break;

			case Types.ARRAY:
				column.javaTypeName = ObjectClazz;
				break;

			case Types.BIGINT:
				column.javaTypeName = LongClazz;
				break;

			case Types.BINARY:
				column.javaTypeName = ObjectClazz;
				break;

			case Types.BIT:
				column.javaTypeName = ObjectClazz;
				break;

			case Types.BLOB:
				column.javaTypeName = "byte[]";
				break;
				
				
			case Types.CLOB:
				column.javaTypeName = StringClazz;
				break;
				
			case Types.DATALINK:
				column.javaTypeName = ObjectClazz;
				break;
				
				
			case Types.DISTINCT:
				column.javaTypeName = ObjectClazz;
				break;
				
			case Types.JAVA_OBJECT:
				column.javaTypeName = ObjectClazz;
				break;
				
			case Types.LONGVARBINARY:
				column.javaTypeName = "byte[]";
				break;

			case Types.LONGVARCHAR:
				column.javaTypeName = StringClazz;
				break;

			case Types.NULL:
				column.javaTypeName = ObjectClazz;
				break;

			case Types.OTHER:
				column.javaTypeName = ObjectClazz;
				break;
				
			case Types.REAL:
				if ( "FLOAT".equals(column.dataTypeName) ) column.javaTypeName = FloatClazz; 
				else column.javaTypeName = DoubleClazz;
				break;
				
			case Types.REF:
				column.javaTypeName = ObjectClazz;
				break;
				
			case Types.STRUCT:
				column.javaTypeName = ObjectClazz;
				break;
				
			case Types.VARBINARY:
				column.javaTypeName = ObjectClazz;
				break;
				

			default:
				throw new SQLException("UnsupportedDataTypeException");
		}
	}
}
