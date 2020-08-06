package com.spark.example.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.api.java.JavaSQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;


public class ReadDataFrame {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				  .setAppName("ReadDataFrame")
				  .set("es.nodes","10.141.222.31")
				  .set("es.port","9200")
				  .set("es.scroll.size", "2000")
				  .setMaster("local[*]");
		SparkSession ss = SparkSession
			      .builder()
			      .config(conf)
			      .getOrCreate();
		Dataset<Row> dataset = JavaEsSparkSQL.esDF(ss,"mixed_dataset_tmp");
		System.out.println(dataset.schema().treeString());
		dataset.show(1000);
		/**
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
		*/
	    ss.stop();

	}
}
