package com.fudan.loadFile;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fudan.basic.InitConf;

public class LoadText {
	public static void main(String[] args) {
		SparkConf conf = InitConf.init("LoadText", true);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> input = jsc.textFile("C:\\Users\\Meng\\Documents\\GitHub\\learning-spark\\README.md");
		input.foreach(t->System.out.println(t));
		jsc.close();
	}
}
