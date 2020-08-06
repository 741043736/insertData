package com.fudan.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
public class FPGrowth {
	private static String dataset = "test";

	public static void main(String[] args) {
//		JavaRDD<Map<String, Object>> values = JavaEsSpark.esRDD(init.sc,dataset).values();
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FPGrowth");
		SparkContext sc = new SparkContext(conf);
		SparkSession ss = new SparkSession(sc);
		
		Dataset<Row> df = JavaEsSparkSQL.esDF(ss,dataset);
		df.show(1000);
	}
}
