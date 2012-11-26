package com.oneline.dao;

import java.io.File;
import java.sql.SQLException;

public class DbLoader {
	
	static String lineSeparator = System.getProperty("line.separator");
	public static void loadDataFromCsvFile( String fileName, String tableName ) throws SQLException {
		File dataFile = new File (fileName);
		//This is for windows only. Just leave it as it is.
		String absolutePath = dataFile.getAbsolutePath().replace('\\', '/');
		StringBuilder strBuild = new StringBuilder(100);
		strBuild.append("LOAD DATA INFILE '");
		strBuild.append(absolutePath);
		strBuild.append("' INTO TABLE ");
		strBuild.append(tableName);
		strBuild.append(" FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '");
		strBuild.append(lineSeparator);
		strBuild.append("'");
		new WriteBase().execute(strBuild.toString(), new Object[] {});
	}
}
