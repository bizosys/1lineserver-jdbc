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
package com.oneline.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.XppDriver;

public class PoolFactory 
{
    private static PoolFactory instance = new PoolFactory();
    private Map<String, Pool> poolMap;
    private Pool defaultPool;

    private final static Logger LOG = Logger.getLogger(PoolFactory.class);

    public static PoolFactory getInstance() 
    {
    	return PoolFactory.instance;
    }
    
    private PoolFactory() 
    {
    	this.poolMap = new HashMap<String, Pool>();
	}
    
    public static Pool getDefaultPool()
    {
    	return PoolFactory.getInstance().defaultPool;
    }
    
    public Pool getPool(String poolType)
    {
    	if (!this.poolMap.containsKey(poolType)) this.poolMap.put(poolType, new Pool(poolType));
    	
			return this.poolMap.get(poolType);
    }
    
    public void returnConnection(Connection conn)
    {
    	if (conn instanceof PoolConnection) this.returnConnection((PoolConnection) conn);
    }
    
    public void returnConnection(PoolConnection poolConn)
    {
    	try 
    	{
			if (poolConn != null && this.poolMap.containsKey(poolConn.poolType))
			{
				if (LOG.isDebugEnabled()) LOG.debug("Returning connection to pool " + poolConn.poolType);
				this.poolMap.get(poolConn.poolType).returnConnection(poolConn);
			}
		} 
    	catch (SQLException e) 
    	{
    		LOG.error("Unable to return Pool Connection for pool type: " +poolConn.poolType, e);
		}
    }
    
    public boolean setup(String configXml)
    {
		try
		{
			List<DbConfig> dbcL = (List<DbConfig>) new XStream(new XppDriver()).fromXML(configXml);
			
			if (dbcL != null && !dbcL.isEmpty())
			{
				for (DbConfig config : dbcL)
				{
					this.startPool(config);
				}
				return true;
			}
		}
		catch (Exception e)
		{
			LOG.error("Error in starting database service with config: " + configXml, e);
		}
		return false;
    }

    public boolean stop()
    {
    	if (this.poolMap == null || this.poolMap.isEmpty()) return true;
    	for (Pool pool : this.poolMap.values())
		{
			pool.stop();
		}
    	this.poolMap.clear();
    	return true;
    }

    public boolean setup(DbConfig config)
    {
		try
		{
			this.startPool(config);
		}
		catch (Exception e)
		{
			LOG.error("Error in starting database service with config: " + config, e);
			return false;
		}
		return true;
    }

    private void startPool(DbConfig config)
	{
		LOG.info("Initializing DB Pool - " + config.poolName);
		System.out.println("Setting up :" + config.toString());
		Pool pool = this.getPool(config.poolName);
		pool.start(config);
		if (this.defaultPool == null && config.defaultPool) this.defaultPool = pool;
	}
    
}