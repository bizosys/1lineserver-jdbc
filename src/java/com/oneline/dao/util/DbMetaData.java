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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.log4j.Logger;

//import com.oneline.dao.Pool;
import com.oneline.dao.PoolFactory;

public class DbMetaData {
	protected Connection con = null;
	protected DatabaseMetaData metadata = null; 
	protected ResultSet rs = null;
	
	private final static Logger LOG = Logger.getLogger(DbMetaData.class);

	
	/**
	 * @param catalog - null
	 * @param schemaPattern - %
	 * @param tableNamePattern - %
	 * @param types - {"TABLE"} 
	 * @return
	 * @throws SQLException
	 */
	public DbSchema execute(String catalog, String schemaPattern, 
	String tableNamePattern, String[] types, HashMap<String, String>includeTables, HashMap<String, String> excludeColumns) throws SQLException {

		try {
			this.con = PoolFactory.getDefaultPool().getConnection();
			metadata = this.con.getMetaData();
			return this.getTables(catalog, schemaPattern, tableNamePattern, types, includeTables, excludeColumns);
		} catch (SQLException ex) {
			LOG.fatal(ex);
			throw(ex);
		} finally {
			this.release();
		}
	}
	
	public DbSchema getTables(String catalog, String schemaPattern, 
	String tableNamePattern, String[] types, 
	HashMap<String, String>includeTables,
	HashMap<String, String> excludeColumns) throws SQLException {
		
		this.rs = this.metadata.getTables(null,"%", "%", types);
		DbSchema dbSchema = new DbSchema();
		while (this.rs.next()) { 

			String name = this.rs.getString("TABLE_NAME");
			if ( ! includeTables.containsKey(name) ) continue;
			
			DbTable table = new DbTable();
			table.catalog = this.rs.getString("TABLE_CAT");
			table.schema = this.rs.getString("TABLE_SCHEM");
			table.name = name;
			
			dbSchema.tables.add(table);
		}
		this.rs.close(); //Reuse resultset
		
		//Primary Key
		for ( int i=0; i< dbSchema.tables.size(); i++) { 
			DbTable table = dbSchema.tables.get(i);
			this.getPrimaryKeys(table);
			this.getIndexes(table);
			this.getColumns(table, excludeColumns);
		}
		
		return dbSchema;
	}
	
	public void getPrimaryKeys(DbTable table) throws SQLException {
		
		this.rs = this.metadata.getPrimaryKeys(
				table.catalog,table.schema, table.name);
		while (this.rs.next()) { 
			table.primaryKeys.add(this.rs.getString("COLUMN_NAME"));
		}
		this.rs.close();
	}
	
	public void getIndexes(DbTable table) throws SQLException {
		
		this.rs = this.metadata.getIndexInfo(
				table.catalog,table.schema, table.name, false, true);
		while (this.rs.next()) { 
			String colName = this.rs.getString("COLUMN_NAME");
			boolean nonUnique = this.rs.getBoolean("NON_UNIQUE");
			if ( nonUnique ) {
				table.nonUniqueIndexes.add(colName);
			} else {
				table.uniqueIndexes.add(colName);
			}
		}
		this.rs.close();
	}


	public void getColumns(DbTable table,  HashMap<String, String> excludeColumns) throws SQLException {
		
		this.rs = this.metadata.getColumns(
				table.catalog,table.schema, table.name, "%");
		
		while (this.rs.next()) { 
			String name = this.rs.getString("COLUMN_NAME");
			if ( excludeColumns.containsKey(name) ) continue;
			
			DbColumn column = new DbColumn();
			column.name = name;
			column.dataType = this.rs.getInt("DATA_TYPE");
			column.dataTypeName = this.rs.getString("TYPE_NAME");
			column.columnSize = this.rs.getInt("COLUMN_SIZE");
			column.decimalDigits = this.rs.getInt("DECIMAL_DIGITS");
			column.isNullable = "YES".equals(this.rs.getString("IS_NULLABLE"));
			DbJavaType.resolve(column);
			table.columns.add(column);
		}
		this.rs.close();
	}
	
	
	protected void release() {
	    if (this.rs != null ) {
	        try {
	            this.rs.close();
	            this.rs = null;
	        } catch (SQLException ex) {
	        	LOG.fatal(ex);
	        }
	    }
		
	    if (this.con != null ) {
	        try {
	        	this.con.close();
	        	this.con = null;
	        } catch (SQLException ex) {
	        	LOG.fatal(ex);
	        }
	    }
	}
}