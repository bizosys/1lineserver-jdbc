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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.google.appengine.api.rdbms.AppEngineDriver;

public class GaePool implements IPool{

	private DbConfig config = null;
    private int activeConnection = 0;

    private String poolType;
    private final static Logger LOG = Logger.getLogger(GaePool.class);

    public GaePool(String poolType) 
    {
    	this.poolType = poolType;
	}
    
	private synchronized PoolConnection createConnection()
	{
		if (this.config == null) return null;
		
		if (LOG.isDebugEnabled()) LOG.debug("Creating a new connection for " + this.poolType);
		try
		{
			PoolConnection poolCon = null;
			Connection con = null;
			if ( this.config.allowMultiQueries ) 
			{
				Properties props = new Properties();
				props.put("allowMultiQueries", "true");
				props.put("user", this.config.login);
				props.put("password", this.config.password);
				DriverManager.registerDriver(new AppEngineDriver());
				con = DriverManager.getConnection(this.config.connectionUrl, props);
			} 
			else 
			{
				DriverManager.registerDriver(new AppEngineDriver());
				con = DriverManager.getConnection(this.config.connectionUrl, 
						this.config.login, this.config.password);
			}
			con.setTransactionIsolation(this.config.isolationLevel);
			poolCon = new PoolConnection (con,this.poolType );
			this.activeConnection++;
			return poolCon;
		} 
		catch (SQLException ex) 
		{
			LOG.fatal("Error in accessing database", ex);
			return null;
		} 
		catch (Exception ex) 
		{
			LOG.fatal("Error in creating connection", ex);
			return null;
		}
	}
	
    
	/**
	 * This is explicitly called when people call Connection.close().
	 * Make sure no other place it is called.
	 * @param returnedConnection
	 * @throws IllegalStateException
	 */
	public void returnConnection(Connection returnedConnection) throws SQLException
	{
		PoolConnection poolConnection = null;
		if (  returnedConnection instanceof PoolConnection) {
			poolConnection = (PoolConnection) returnedConnection;
		}
		
		if ( null != poolConnection )
		{
			poolConnection.setTransactionIsolation(this.config.isolationLevel);
			poolConnection.setAutoCommit(true);
		}
	}
	

	/**
	 * This gives a connection from pool. If nothing is in the pool,
	 * it creates one and gives back. 
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		Connection con = this.getNTestConnection(); //First try
		if ( null != con ) return con;
		
		LOG.debug("Trying second time..");
		con = this.getNTestConnection(); //Second try
		if ( null != con ) return con;
		
		LOG.debug("Trying third time..");
		con = this.getNTestConnection(); //Third try
		if ( null != con ) return con;

		LOG.fatal("Database is not available");
		throw new SQLException ("NO_CONNECTION");
	}
	
	private Connection getNTestConnection() throws  SQLException 
	{
		return this.createConnection();
    }
    
    public int getActiveConnection() 
    {
    	return this.activeConnection;
    }
    
    public int getAvailableConnection() 
    {
    	return 0;
    }

    public synchronized void start(DbConfig dbConfig) 
    {
    	this.config = dbConfig;
    }
    
    public synchronized void stop() 
    {
    	
    }

    //--------------Health Check Module-------------------
    public synchronized void healthCheck() {
    	
    }
    
}