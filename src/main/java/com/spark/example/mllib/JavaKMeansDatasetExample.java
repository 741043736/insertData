/*
package com.spark.example.mllib;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
*/
/**
 * An example demonstrating k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaKMeansExample
 * </pre>
 *//*

public class JavaKMeansDatasetExample {

  public static void main(String[] args) {
    // Create a SparkSession.
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaKMeansExample").master("local")
      .getOrCreate();

    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm").load("./data/sample_kmeans_data.txt");

    // Trains a k-means model.
    KMeans kmeans = new KMeans().setK(2).setSeed(1L);
    KMeansModel model = kmeans.fit(dataset);

    // Make predictions
    Dataset<Row> predictions = model.transform(dataset);

    // Evaluate clustering by computing Silhouette score
    ClusteringEvaluator evaluator = new ClusteringEvaluator();

    double silhouette = evaluator.evaluate(predictions);
    System.out.println("Silhouette with squared euclidean distance = " + silhouette);

    // Shows the result.
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }
    // $example off$

    spark.stop();
  }
}
*/
