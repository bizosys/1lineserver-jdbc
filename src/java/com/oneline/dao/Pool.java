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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Stack;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

public class Pool implements IPool{

	private DbConfig config = null;
    private Stack<PoolConnection> availablePool = new Stack<PoolConnection>();
    private Stack<PoolConnection> destroyPool =  new Stack<PoolConnection>();
    
    private final Timer AGENT_TIMER = new Timer(true);
    private HealthCheckAgent hcAgent = null;
    
    private int activeConnection = 0;

    private String poolType;
    private final static Logger LOG = Logger.getLogger(Pool.class);

    public Pool(String poolType) 
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
				con = DriverManager.getConnection(this.config.connectionUrl, props);
			} 
			else 
			{
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
	
	private void increment(int count) 
	{
		if (LOG.isDebugEnabled()) LOG.debug("Incrementing Connections by " + count);
		for (int i=0; i < count; i++)
		{
			try 
			{
				PoolConnection con = this.createConnection();
				if ( null != con ) con.close(); //Returns to the stack
				Thread.currentThread().sleep(this.config.timeBetweenConnections); 
			} 
			catch (Exception ex) 
			{
				LOG.fatal("Error in creating connection", ex);
			}
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
			this.poolReturn(poolConnection);
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
	
	private Connection getNTestConnection() throws  SQLException {

		/** Step # 1  Get a connection */
		PoolConnection poolCon = null;
    	if (! this.availablePool.empty() ) poolCon = this.poolGet();
    	if ( null == poolCon ) poolCon = this.createConnection();
    	
		/** Step # 2  Test for the null, closed and dirty connection*/
    	boolean goodConnection = true;
    	if ( poolCon == null ) { 
    		LOG.debug("Pool connection gave a null conncetion");
    		goodConnection = false;
    	}
    	if ( poolCon.isClosed() ) {
    		LOG.debug("Pool connection is already closed");
    		goodConnection = false;
    	}
    	if ( poolCon.isDirty() ) {
    		LOG.debug("Pool connection is dirty.");
    		goodConnection = false;
    	}

		/** Step # 3  Give a null connection and destroy the bad connections.*/
    	if ( goodConnection ) 
    	{
    		return poolCon;
    	} 
    	else 
    	{
			this.activeConnection--;
			try 
			{
				poolCon.destroy();
			} 
			catch (Exception ex) 
			{
				LOG.error("Potential connection leakage. check the root cause ..", ex);
			}
			return null;
    	}
	}
    
    public int getActiveConnection() 
    {
    	return this.activeConnection;
    }
    
    public int getAvailableConnection() 
    {
    	return this.availablePool.size();
    }

    private synchronized void poolReturn(PoolConnection poolConnection) 
    {
		this.availablePool.push(poolConnection);
    	if ( LOG.isDebugEnabled()) LOG.debug("Connections Available/Total = " + this.getAvailableConnection() + "/" + this.getActiveConnection());
	}

	private synchronized PoolConnection poolGet() 
	{
		return this.availablePool.pop();
    }

    public synchronized void start(DbConfig dbConfig) 
    {
    	if ( null == dbConfig) 
    	{
    		String errMsg = "Null db configuration file provided."; 
    		LOG.fatal(errMsg + ":" + dbConfig);
    		throw new RuntimeException(errMsg);
    	} 
    	this.config = dbConfig;
    	LOG.debug("Starting the database service for " + this.config.poolName);

    	try 
    	{
			Class.forName(this.config.driverClass).newInstance();
			LOG.debug("Driver instantiated with " + this.config.driverClass);
	        this.availablePool.ensureCapacity( this.config.idleConnections );
	        this.healthCheck();
		} 
    	catch (Exception ex) 
		{
			LOG.fatal("Pool creation issues.", ex);
		} 

    	this.hcAgent = new HealthCheckAgent();
    	AGENT_TIMER.schedule(this.hcAgent, this.config.timeBetweenConnections, this.config.healthCheckDurationMillis);
    }
    
    public synchronized void stop() 
    {
    	LOG.debug("Stoping the database service");
    	if ( null != this.hcAgent ) 
    	{
    		this.hcAgent.cancel();
    		this.hcAgent = null;
    	}
    }

    
    //--------------Health Check Module-------------------
    public synchronized void healthCheck() {
    	if ( LOG.isDebugEnabled()) 
    	{
    		LOG.debug("Connections Available/Total = " + this.getAvailableConnection() + "/" + this.getActiveConnection());
    	}
		
    	if ( ! this.healthAddConnections() ) 
    	{
    		this.healthRemoveConnections();
    	}
    	this.healthRefreshConnections();
    }
    
    private boolean healthAddConnections() 
    {
    	boolean canIncrease = this.activeConnection < this.config.maxConnections;
    	int availablePoolT = this.availablePool.size();
        
    	boolean isLess = availablePoolT < this.config.idleConnections;
    	boolean toAdd = isLess &&  canIncrease;	
    	
    	if ( toAdd ) this.increment(this.config.incrementBy);
    	return toAdd;
    }
    
    private void healthRemoveConnections() {

		//Old destroy pool remove everything.
		int destroySize = this.destroyPool.size();
		for ( int i=0; i< destroySize ; i++ ) {
			PoolConnection con = this.destroyPool.pop();
			if (con == null ) break;
			this.destroyPoolConnection(con);
		}

		//Current pool take care.
		while ( ( this.availablePool.size() - this.config.idleConnections ) > 0 ) {
			
			PoolConnection con = null;
			try {
				con = (PoolConnection) this.getConnection();
			} catch (SQLException ex) {
				LOG.fatal("Error in destroying connection", ex);
				continue;
			}
			this.destroyPool.push(con);
		}
		
    }
    
    private void healthRefreshConnections() 
    {
    	if (!this.config.testConnectionOnIdle) return;

    	int availablePoolT = this.getAvailableConnection();
    	
		PoolConnection[] conL = new PoolConnection[availablePoolT]; 

		//Get all connections from the pool. Otherwise the same connection will be tested.
		for ( int i=0; i < availablePoolT; i++ ) 
		{
			try 
			{
				conL[i] = (PoolConnection) this.getConnection();
				LOG.info("Got idle connection - " + i + ". " + (conL[i] != null));
			} 
			catch (SQLException ex) 
			{
				LOG.fatal("Error in getting connection from pool for refresh", ex);
				continue;
			}
		}
		
		if (this.config.runTestSql)
		{
			this.runTestSqlOnConnections(conL);
		}
		else
		{
			this.releaseConnections(conL);
		}
    }

	private void releaseConnections(PoolConnection[] conL)
	{
		int availablePoolT = conL.length;
		for ( int i=0; i < availablePoolT; i++ ) 
		{
			this.releaseConnection(conL[i]);
			LOG.info("Released idle connection - " + i );
		}
	}

	private void releaseConnection(PoolConnection con)
	{
		if (con == null) return;
		try 
		{
			con.close();
		} 
		catch (SQLException ex) 
		{
			LOG.error("Destroying a bad connection from the pool", ex);
			this.destroyPool.push(con);
		}
	}

	private void runTestSqlOnConnections(PoolConnection[] conL)
	{
		int availablePoolT = conL.length;
		for ( int i=0; i < availablePoolT; i++ ) 
		{
			if (conL[i] != null)
			{
				LOG.info("Testing idle connection - " + i );
				try 
				{
					Statement stmt = conL[i].createStatement() ;
					ResultSet rs = stmt.executeQuery(this.config.testSql);
					rs.close();
					stmt.close();
				} 
				catch (SQLException ex) 
				{
					//Nothing to do;
					LOG.error("Exception in testing connections.", ex);
				}
				this.releaseConnection(conL[i]);
				LOG.info("Released idle connection after testing - " + i );
			}
		}
	}
    
    private void destroyPoolConnection(Connection con) 
    {
		try 
		{
			if (  con instanceof PoolConnection) {
				PoolConnection poolCon = (PoolConnection) con;
				try { Thread.currentThread().sleep(this.config.timeBetweenConnections); } 
				catch (InterruptedException ex) { LOG.fatal("Error in sleeping for destroy action",ex); }
				this.activeConnection--;
				poolCon.destroy();
			} else {
				con.close();
			}
			con = null;
		} catch (SQLException ex) {
			LOG.fatal("Error in cleaning up connection", ex);
		}
    }
    
    private class HealthCheckAgent extends TimerTask {
        public void run() {
            try {
            	healthCheck();
            } catch(Exception e) {
    			LOG.fatal("Error in running health check", e);
            }
        }
    }    
}