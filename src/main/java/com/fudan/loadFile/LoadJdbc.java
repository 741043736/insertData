package com.fudan.loadFile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class LoadJdbc {
	  public static Dataset<Row> loadJdbc(SparkSession spark, String tableName)
	  {
		  Properties connectionProperties = new Properties();
			connectionProperties.put("user", "ldreport");
			connectionProperties.put("password", "123456");
		  return spark.read()
		    .jdbc("jdbc:oracle:thin:@localhost:1521:Orcl",tableName, connectionProperties);
	  }
}
