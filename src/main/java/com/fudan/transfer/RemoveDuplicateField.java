package com.fudan.transfer;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class RemoveDuplicateField {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("RemoveField");
		conf.set("es.nodes","10.141.222.31");
		conf.set("es.port","9200").set("es.scroll.size", "2000");
//		conf.setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Map<String, Object>> data = JavaEsSpark.esRDD(jsc,"test").values();
		JavaRDD<Map<String, Object>> result = data.map(new Function<Map<String,Object>, Map<String,Object>>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Object> call(Map<String, Object> t) throws Exception {
				t.remove("DEDUCT_AMOUNT");
				t.remove("F_AMOUNT");
				t.remove("QTY");
				return t;
			}
		});
//		data.foreach(t->
//		{
//			t.remove("DEDUCT_AMOUNT");
//			t.remove("F_AMOUNT");
//			t.remove("QTY");
//		});
		JavaEsSpark.saveToEs(result,"mixed_dataset_tmp");
	}
}
