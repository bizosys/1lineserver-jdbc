package com.oneline.dao;

import java.sql.Connection;
import java.sql.SQLException;

import com.oneline.dao.DbConfig;

public interface IPool 
{
	public Connection getConnection() throws SQLException;
	public void returnConnection(Connection returnedConnection) throws SQLException;
	public int getActiveConnection();
	public int getAvailableConnection();
	public void start(DbConfig dbConfig);
	public void stop();
	public void healthCheck();
}
