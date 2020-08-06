package com.spark.example.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Map;
import java.util.Vector;

public class ReadRDD {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("RemoveField");
		conf.set("es.nodes","10.141.222.31");
		conf.set("es.port","9200").set("es.scroll.size", "2000");
		conf.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Map<String, Object>> data = JavaEsSpark.esRDD(jsc,"mixed_dataset_tmp").values();
		JavaRDD<Vector> parsedData =  data.map(t->{
			double[] values = (double[]) t.get("");
			return (Vector) Vectors.dense(values);
		});
	}
}
