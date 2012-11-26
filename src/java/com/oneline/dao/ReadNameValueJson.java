package com.oneline.dao;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class ReadNameValueJson extends ReadBase<String>  {

	private final static Logger LOG = Logger.getLogger(ReadNameValueJson.class);
	
	@Override
	protected List<String> populate() throws SQLException {
		
		checkCondition();
		ResultSetMetaData md = rs.getMetaData() ;
		int totalCol = md.getColumnCount();
		String[] cols = createLabels(md, totalCol);
		List<String> records = new ArrayList<String>(totalCol);

		StringBuilder aRecord = new StringBuilder(256);
		while (this.rs.next()) {
			createRecord(totalCol, cols, aRecord);
			records.add(aRecord.toString());
			aRecord.delete(0, aRecord.capacity());
		}
		return records;
	}

	@Override
	protected String getFirstRow() throws SQLException {
		checkCondition();
		ResultSetMetaData md = rs.getMetaData() ;
		int totalCol = md.getColumnCount();
		String[] cols = createLabels(md, totalCol);

		StringBuilder aRecord = new StringBuilder(256);
		if (this.rs.next()) {
			createRecord(totalCol, cols, aRecord);
		}
		return aRecord.toString();
	}

	
	private void checkCondition() throws SQLException {
		if ( null == this.rs) {
			LOG.warn("Rs is not initialized.");
			throw new SQLException("Rs is not initialized.");
		}
	}

	private String[] createLabels(ResultSetMetaData md, int totalCol) throws SQLException {
		String[] cols = new String[totalCol];
		for ( int i=0; i<totalCol; i++ ) {
			cols[i] = md.getColumnLabel(i+1);
		}
		return cols;
	}

	private void createRecord(int totalCol, String[] cols, StringBuilder recordsSb) throws SQLException {
		recordsSb.append('"').append(cols[0]).append("\":\"").append(cols[1]).append('"');
	}
		
}
