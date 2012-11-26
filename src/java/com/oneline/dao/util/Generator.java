/**
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
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import com.oneline.dao.DbConfig;
import com.oneline.dao.PoolFactory;

public class Generator {
	public static void main(String[] args) {
		
		if ( null == args || args.length  != 5 	) {
			System.out.println("Usage Generaor <driver> <connectionUrl> <user> <password> <commaSeparatedTableNames>");
			System.exit(1);
		}
		int argsCount=0;	
		String driver = args[argsCount++];
		String connectionUrl = args[argsCount++];
		String user = args[argsCount++];
		String password = args[argsCount++];
		String[] tables = Generator.getStrings(args[argsCount++]);
		
		DbConfig dbConfig = new DbConfig();
		dbConfig.driverClass  = driver;
		dbConfig.connectionUrl  = connectionUrl;
		dbConfig.login = user;
		dbConfig.password  = password;
		dbConfig.idleConnections = 1;
		dbConfig.incrementBy = 1;
		dbConfig.maxConnections = 2;
		dbConfig.timeBetweenConnections = 5;
		PoolFactory.getInstance().setup(dbConfig);
		
		DbMetaData meta = new DbMetaData();
		try {
			String[] names = {"TABLE"}; 
			HashMap<String, String> excludeFields = new HashMap<String, String>();
			excludeFields.put("touchTime", "touchTime");
			
			HashMap<String, String> includeTables = new HashMap<String, String>();
			for ( int i=0; i< tables.length; i++ ) {
				includeTables.put(tables[i], null);
			}
			
			DbSchema schema = meta.execute("%", "%", "%", names, includeTables, excludeFields );
			
			GenerateBroker broker = new GenerateBroker();

			Generate voGenerator = new GenerateVO();
			broker.registerObserver(voGenerator);

			Generate tableGenerator = new GenerateTable();
			broker.registerObserver(tableGenerator);

			Generate asGenerator = new GenerateAS();
			broker.registerObserver(asGenerator);
			
			//schema.print();
			broker.notifyObservers(schema);
			
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	/**
	   * returns an arraylist of strings  
	   * @param str the comma seperated string values
	   * @return the arraylist of the comma seperated string values
	   */
	public static String[] getStrings(String str){
	    if (str == null)
	      return null;
	    StringTokenizer tokenizer = new StringTokenizer (str,",");
	    List<String> values = new ArrayList<String>();
	    while (tokenizer.hasMoreTokens()) {
	      values.add(tokenizer.nextToken());
	    }
	    return (String[])values.toArray(new String[values.size()]);
	}		
}
