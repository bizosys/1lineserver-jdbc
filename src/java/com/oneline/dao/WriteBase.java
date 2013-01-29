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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

public class WriteBase {
	protected Connection con = null;
	protected PreparedStatement prepareStmt = null; 
	protected ResultSet rs = null;
	protected int recordsTouched = -1;
	protected boolean isInTransaction = false;
    private final static Logger LOG = Logger.getLogger(WriteBase.class);
	
	//Only time to create a connection if not there.
	public void beginTransaction() throws SQLException {
		if ( ! this.isInTransaction ) {
			this.createConnection(false);
			this.isInTransaction = true;
			if ( LOG.isDebugEnabled()) LOG.debug("beginTransaction" + this.hashCode());
		}
	} 
	
	//End the transaction now.
	public void commitTransaction() throws SQLException {
		if ( this.isInTransaction ) {
			this.isInTransaction = false;
			this.con.commit();
			this.releaseResources();
			if ( LOG.isDebugEnabled()) LOG.debug("endTransaction" + this.hashCode());
		}
	}

	//Rollback the transaction now.
	public void rollbackTransaction() {
		if ( this.isInTransaction ) {
			this.isInTransaction = false;
			if ( LOG.isDebugEnabled()) LOG.debug("Rolling back the connection" + this.hashCode());
			try {
				this.con.rollback();
			} catch (SQLException ex) {
				LOG.info("Rollback failed", ex);
			} finally {
				this.releaseResources();
			}
		}
	}
	
	// If the transaction is not there, create it.
	protected void createConnection(boolean autoCommit) throws SQLException {
    	if ( ! this.isInTransaction ) {
			if ( LOG.isDebugEnabled()) LOG.debug("Creating pool connection" + this.hashCode());
			this.con = PoolFactory.getDefaultPool().getConnection();
			this.con.setAutoCommit(autoCommit);
    	}
	}

	// If the transaction is not there, release connection.
	protected void releaseResources() {
		if ( ! this.isInTransaction ) {
			if ( LOG.isDebugEnabled()) LOG.debug("Releasing connection, statement and resultset." + this.hashCode());
			this.release(true, true, true);
		} else {
			this.release(true, true, false);
		}
	}

	//The insert statement
	public Integer insert(String query, Object[] columns) throws SQLException {
	    try {
	    	this.createConnection(true);
			this.prepareStmt = this.con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

			for ( int i=1; i  <= columns.length; i++ ) {
				if ( LOG.isDebugEnabled()) LOG.debug ( i + "-" + columns[i-1]);
				this.prepareStmt.setObject(i,columns[i-1]) ;
			}
			this.recordsTouched = prepareStmt.executeUpdate();
			if ( LOG.isDebugEnabled()) LOG.debug("Records Touched= " + this.recordsTouched + "-" +  this.hashCode());

		    Integer autoIncKey = new Integer(-1);
		    rs = prepareStmt.getGeneratedKeys();
		    if (rs.next()) {
		    	autoIncKey = rs.getInt(1);
		    } else {
		    	throw new SQLException("NO_KEY_GENERATED");
		    }
		    return autoIncKey;
		} catch (SQLException ex) {
			logException(query, columns, ex);
			throw ex;
		} finally {
			this.releaseResources();
		}
	}
	
	public Integer insert(String query, List<Object> columns) throws SQLException {
        try {
           this.createConnection(true);
                  this.prepareStmt = this.con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

                  int colsT = ( null == columns) ? 0 : columns.size();
                  for ( int i=1; i  <= colsT; i++ ) {
                        this.prepareStmt.setObject(i, columns.get(i-1)) ;
                  }
                  
                  this.recordsTouched = prepareStmt.executeUpdate();
                  if ( LOG.isDebugEnabled()) LOG.debug("Records Touched= " + this.recordsTouched + "-" +  this.hashCode());

               Integer autoIncKey = new Integer(-1);
               rs = prepareStmt.getGeneratedKeys();
               if (rs.next()) {
                  autoIncKey = rs.getInt(1);
               } else {
                  throw new SQLException("NO_KEY_GENERATED");
               }
               return autoIncKey;
           } catch (SQLException ex) {
                  logException(query, columns, ex);
                  throw ex;
           } finally {
                  this.releaseResources();
           }
    }
	
	
	public int execute(String query, Object[] columns) throws SQLException {
	    try {
	    	this.createConnection(true);
			this.prepareStmt = this.con.prepareStatement(query);

			for ( int i=1; i  <= columns.length; i++ ) {
				this.prepareStmt.setObject(i,columns[i-1]) ;
			}
			this.recordsTouched = prepareStmt.executeUpdate();
	    	return this.recordsTouched;
		} catch (SQLException ex) {
			logException(query, columns, ex);
			throw ex;
		} finally {
			this.releaseResources();
		}
	}
	
	public int execute(String query, List<Object> columns) throws SQLException {
	    try {
	    	this.createConnection(true);
			this.prepareStmt = this.con.prepareStatement(query);

			int colsT = ( null == columns) ? 0 : columns.size();
			for ( int i=1; i  <= colsT; i++ ) {
				this.prepareStmt.setObject(i, columns.get(i-1)) ;
			}
			this.recordsTouched = prepareStmt.executeUpdate();
	    	return this.recordsTouched;
		} catch (SQLException ex) {
			logException(query, columns, ex);
			throw ex;
		} finally {
			this.releaseResources();
		}
	}	

	private void logException(String query, Object[] columns, SQLException ex){
		StringBuilder sb = new StringBuilder(256);
		if ( null != columns) {
			for (Object column : columns) {
				sb.append('[').append(column).append(']');
			}
		}
		LOG.fatal("Query = " + query + "\n" + sb.toString());
		LOG.fatal(ex);
	}
	
	private void logException(String query, List<Object> columns, SQLException ex){
		StringBuilder sb = new StringBuilder(256);
		if ( null != columns) {
			for (Object column : columns) {
				sb.append('[').append(column).append(']');
			}
		}
		LOG.fatal("Query = " + query + "\n" + sb.toString());
		LOG.fatal(ex);
	}	

	protected void release(boolean closeRs, boolean closeStmt, boolean closeCon ) {
	    if (this.rs != null && closeRs ) {
	        try {
	            this.rs.close();
	            this.rs = null;
	        } catch (SQLException ex) {
	        	LOG.fatal(ex);
	        }
	    }
	    if (this.prepareStmt != null && closeStmt ) {
	        try {
	        	this.prepareStmt.close();
	        	this.prepareStmt = null;
	        } catch (SQLException ex) {
	        	LOG.fatal(ex);
	        }
	    }
		
	    if (this.con != null && closeCon) {
	        try {
	        	this.con.close();
	        	this.con = null;
	        } catch (SQLException ex) {
	        	LOG.fatal(ex);
	        }
	    }
	}
	
    /**
     * To execute a batch of updates or inserts onto a single table.
     * @param query - For all records to execute with
     * @param records - List of column values for each record. List of Object[] objects
     * @return int[] - rows updated per record
     * @throws SQLException
     */
    public int[] executeBatch(String query, List<Object[]> records) throws SQLException
    {
    	boolean transactionByBatch = !(this.isInTransaction);
    	
    	this.startBatch(query, transactionByBatch);
    	for (Object[] record : records)
    	{
    		this.addToBatch(record);
    	}
    	return this.executeBatch(transactionByBatch);
    }
    
	private void startBatch(String query, boolean transactionByBatch) throws SQLException
	{
	    try 
	    {
			if (transactionByBatch) this.beginTransaction();
	    	this.createConnection(true);
			this.prepareStmt = this.con.prepareStatement(query);
		} 
	    catch (SQLException ex) 
	    {
			LOG.fatal(ex);
			throw ex;
		}
	}

	private void addToBatch(Object[] columns) throws SQLException
	{
		if (this.prepareStmt == null)
		{
			throw new SQLException("Illegal call. startBatch has to be done before addToBatch");
		}

		int columnLength = columns.length;
		for ( int i=1; i  <= columnLength; i++ ) 
		{
			this.prepareStmt.setObject(i,columns[i-1]) ;
		}
		this.prepareStmt.addBatch();
	}

	private int[] executeBatch(boolean transactionByBatch) throws SQLException
	{
		if (this.prepareStmt == null)
		{
			throw new SQLException("Illegal call. startBatch has to be done before addToBatch");
		}
	    try 
	    {
			int[] results = this.prepareStmt.executeBatch();
			if (transactionByBatch) this.commitTransaction();
			return results;
		} 
	    catch (SQLException ex) 
	    {
			LOG.fatal(ex);
			if (transactionByBatch) this.rollbackTransaction();
			throw ex;
		} 
	    finally 
	    {
			this.releaseResources();
		}
	}
	
}