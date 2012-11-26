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

public class DbTable {
	String catalog;
	String schema;
	String name;
	
	List<String> primaryKeys = new ArrayList<String>(1);
	List<String> uniqueIndexes = new ArrayList<String>(1);
	List<String> nonUniqueIndexes = new ArrayList<String>(1);
	List<DbColumn> columns = new ArrayList<DbColumn>(1);
	private List<DbColumn> nonPKColumns = new ArrayList<DbColumn>();
	
	public List<DbColumn> getNonPKCols() {
		if ( 0 == nonPKColumns.size()) {
			int columnsT = columns.size();
			int primaryKeysT = primaryKeys.size();

			boolean isPK = false;
			for ( int j=0; j< columnsT; j++) {
				isPK = false;
				for ( int i=0; i< primaryKeysT ; i++) {
					if ( primaryKeys.get(i).equals(columns.get(j).name) ) {
						isPK = true;
					}
				}
				if ( ! isPK ) nonPKColumns.add(columns.get(j));
			}
		} 
		return nonPKColumns;
	}
	public void print() {
		System.out.println("-------------");
		System.out.println("Table Name=" + name);
		System.out.print("Primary Keys=" );
		for ( int i = 0; i < primaryKeys.size(); i++ ) {
			System.out.print(primaryKeys.get(i) + ":");
		}
		System.out.println(" ");
		
		System.out.print("Unique Indexes=" );
		for ( int i = 0; i < uniqueIndexes.size(); i++ ) {
			System.out.print(uniqueIndexes.get(i) + ":");
		}
		System.out.println(" ");

		System.out.print("Non Unique Indexes=" );
		for ( int i = 0; i < nonUniqueIndexes.size(); i++ ) {
			System.out.print(nonUniqueIndexes.get(i) + ":");
		}
		System.out.println(" ");	
		
		System.out.println("Columns =" );
		for ( int i = 0; i < columns.size(); i++ ) {
			columns.get(i).print();
		}
		System.out.println("-------------");
	}
}
