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

import java.util.Hashtable;

public abstract class Generate {
	public abstract void process(DbSchema schema);

	public static Hashtable<String, String> camelHash = new Hashtable<String, String>();
	public static String toCamelCase(String name, boolean firstBig) {
		
		if ( firstBig && !name.startsWith("_") ) name = "_" + name;
		if ( camelHash.containsKey(name) ) return camelHash.get(name);
		
		String camelName = name;
		String[] aToz = new String[] {"a","b","c","d","e","f","g","h","i",
				"j","k","l","m","n","o","p","q","r",
				"s","t","u","v","w","x","y","z"};

		String[] AToZ = new String[] {"A","B","C","D","E","F","G","H","I",
				"J","K","L","M","N","O","P","Q","R",
				"S","T","U","V","W","X","Y","Z"};
		
		for ( int i=0; i< 26; i++ ) {
			camelName = camelName.replaceAll("[_| ]" + aToz[i],AToZ[i]);
		}
		
		camelHash.put(name, camelName);
		return camelName;
	}
	
}
