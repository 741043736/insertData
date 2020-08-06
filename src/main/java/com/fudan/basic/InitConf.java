package com.fudan.basic;

import org.apache.spark.SparkConf;
public class InitConf {
	public static SparkConf init(String appName,boolean isLocal)
	{
		SparkConf conf = new SparkConf().setAppName(appName);
		conf.set("es.nodes",  "10.141.222.31");
		conf.set("es.port", "9200");
		conf.set("es.scroll.size", "2000");
		if(isLocal) 
		{
			conf.setMaster("local[*]");
		}
		return conf;
	}
}
