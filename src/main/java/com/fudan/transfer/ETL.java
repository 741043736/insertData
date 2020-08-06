package com.fudan.transfer;

import com.fudan.basic.Tags;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;


public class ETL {

//	private static final boolean isLocal = true;

	private static final boolean isLocal = false;

	private static SparkSession spark;

	public static SparkConf init(String appName) {
		SparkConf conf = new SparkConf().setAppName(appName);
		conf.set("es.nodes", "10.141.222.31");
		conf.set("es.port", "9200");
		conf.set("es.scroll.size", "2000");
		if (isLocal) {
			conf.setMaster("local[*]");
		}
		return conf;
	}

	public static Dataset<Row> loadJdbc(String tableName) {
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("driver", "oracle.jdbc.driver.OracleDriver")
				.option("url", "jdbc:oracle:thin:@10.131.1.61:1521:Orcl")
				.option("dbtable", tableName)
				.option("user", "ldreport")
				.option("password", "123456")
				.load();
		return jdbcDF;
	}

	//list of java convert to seq of scala
	public static Seq<String> convertListToSeq(ArrayList<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	//筛选字段，删除测试数据
	public static Dataset<Row> getTable(String tableName, String[] tmlHdrTags) {
		Dataset<Row> table = loadJdbc(tableName).filter((FilterFunction<Row>) t -> {
			if (t.getAs("DATA_SIGN").toString().equals("1"))
				return false;
			else
				return true;
		}).selectExpr(tmlHdrTags).na().fill("null");
		return transferTableType(table, "String");
	}

	public static Dataset<Row> transferTableType(Dataset<Row> table, String type) {
		//转变dataset的数据格式
		System.out.println("转变前" + table.schema().treeString());
		String[] colNames = table.columns();
		for (String colName : colNames) {
			if (table.col(colName) != null) {
				table = table.withColumn(colName, table.col(colName).cast(type));
			}
		}
		System.out.println("转变后" + table.schema().treeString());
		return table;
	}

	public static void main(String[] args) {
		SparkConf conf = init("ETL");
		spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<Row> tml_hdr = getTable("temp_tml_hdr", Tags.tml_hdr_tags);
		Dataset<Row> tml_dtl = getTable("temp_tml_dtl", Tags.tml_dtl_tags);
		Dataset<Row> sub_unit = getTable("temp_sub_unit", Tags.sub_unit_tags);
		Dataset<Row> product_basic = getTable("temp_product_basic", Tags.product_basic_tags);
		Dataset<Row> pay_type = getTable("temp_pay_type", Tags.pay_type_tags);
		Dataset<Row> cash_dtl = getTable("temp_cash_dtl", Tags.cash_dtl_tags);

		//trasfer list of java to Seq of scala
//		ArrayList<String> list = new ArrayList<>();
//		list.add("RESERVED_NO");
//		list.add("TML_NUM_ID");
//		Seq<String> usingColumns = convertListToSeq(list);
//		tml_dtl.describe("TML_NUM_ID").show();

		Dataset<Row> tml_hdr_sub_unit = tml_hdr.join(sub_unit, "SUB_UNIT_NUM_ID");
		Dataset<Row> cash_pay = pay_type.join(cash_dtl, "PAY_TYPE_ID");
		Dataset<Row> tml_dtl_product_basic = tml_dtl.join(product_basic, "ITEM_NUM_ID");
		Dataset<Row> tml = tml_hdr_sub_unit.join(tml_dtl_product_basic, "TML_NUM_ID");
		Dataset<Row> cash = cash_pay.join(tml_hdr, cash_pay.col("RESERVED_NO").equalTo((tml_hdr).col("TML_NUM_ID")));
		Dataset<Row> join = tml.join(cash, "TML_NUM_ID");

		join.foreach((ForeachFunction<Row>) t -> System.out.println(t));
//		JavaEsSparkSQL.saveToEs(join, "join");
	}
}
