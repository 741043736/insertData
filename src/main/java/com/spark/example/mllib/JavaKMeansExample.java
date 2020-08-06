package com.spark.example.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

// $example on$

public class JavaKMeansExample {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("JavaKMeansExample").setMaster("local[*]");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// Load and parse data
		String path = "src/main/resources/data/mllib/kmeans_data.txt";

		JavaRDD<String> data = jsc.textFile(path);
		JavaRDD<Vector> parsedData = data.map(s ->
		{
			String[] sarray = s.split(" ");
			double[] values = new double[sarray.length];
			for (int i = 0; i < sarray.length; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);
		});
		parsedData.cache();

		// Cluster the data into two classes using KMeans
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		// Save and load model
		clusters.save(jsc.sc(), "target/KMeansModel");
		KMeansModel sameModel = KMeansModel.load(jsc.sc(),
				"target/org/apache/spark/JavaKMeansExample/KMeansModel");
		// $example off$

    jsc.stop();
	}
}
