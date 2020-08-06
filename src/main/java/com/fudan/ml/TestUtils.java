package com.fudan.ml;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

public class TestUtils {
	
	//������=���Ŷ�/֧�ֶ�
	public static void env() {
		System.setProperty("hadoop.home.dir", "E:\\��רҵ��ص����\\eclipse\\ESprojectmust\\hadoop-common");
	}

	public static SparkConf conf(String name) {
		SparkConf conf = new SparkConf().setAppName(name);
		return conf;
	}
	public static void esConfig(SparkConf conf) {
		conf.set("es.nodes", "10.141.222.31");
		conf.set("es.port", "9200");
		conf.set("es.input.max.docs.per.partition", "1000000");
		conf.set("es.scroll.size", "4000");
		
		conf.set("es.http.retries", "10");
		
	    conf.set("es.nodes.discovery", "false");
	    conf.set("es.nodes.data.only", "false");	
	}
	public static void local(SparkConf conf) {
		conf.setMaster("local[*]");
	}
	


	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();//��ȡ��ǰʱ��

		testFPGrowth();
		long endTime = System.currentTimeMillis();
		System.out.println("��������ʱ�䣺"+(endTime-startTime)+"ms");

	}

	public static void testFPGrowth() throws Exception {
		@SuppressWarnings("resource")
		PrintStream ps = new PrintStream("D:\\������ѧ��������\\��Ʒ����\\�������\\1.txt");
		env();
		SparkConf conf = new SparkConf().setAppName("test1").setMaster("local[*]");//setMasterΪlocal��ʾ�ڱ�������
		local(conf);
		esConfig(conf);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		//SparkSession session = SparkSession.builder().config(conf).getOrCreate();//SparkSession�Ƕ�ȡ���ݵ������
		JavaRDD<Map<String,Object>> dataset = JavaEsSpark.esRDD(jsc, "test").values();//testΪES�ϵ����ݿ���

		JavaRDD<HashSet<String>> rdd = dataset.filter(l->{

			return (l.get("TML_NUM_ID")!=null );
		
		}).mapToPair(line->{
			String key = line.get("TML_NUM_ID").toString();
			return Tuple2.apply(key, line);  
		}).groupByKey().map(line->{
			Iterator<Map<String, Object>> iterator = line._2.iterator();
			
			HashSet<String> set = new HashSet<>();
			//int i=0;
			while(iterator.hasNext()) {
				//i++;        //������i �����
				Map<String, Object> item = iterator.next();
				set.add(item.get("PTY_NUM_3").toString());
			}
			return set; 
			
		});

		
		FPGrowth fp = new FPGrowth().setMinSupport(0.001)//������õ���0.5
				  .setNumPartitions(10);
		FPGrowthModel<String> model = fp.run(rdd);
		
		
		for (FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
			  ps.println("[" + itemset.javaItems() + "], " + itemset.freq());
			}

			double minConfidence = 0.04;//�ʼ���ó�0.8̫���˳����˽��
			Map<String,String> map = new HashMap<>();
			for (AssociationRules.Rule<String> rule
			  : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {	
				//�����������ʲô����
				
					if(map.containsKey(rule.javaAntecedent().toString())) {
						map.put(rule.javaAntecedent().toString(), map.get(rule.javaAntecedent().toString())+","+rule.javaConsequent().toString()+" "+String.valueOf(rule.confidence()));
					//map.get(key)��ʾ���ǻ�ȡkey��Ӧ��ֵ
					}else {
						
						map.put(rule.javaAntecedent().toString(), rule.javaConsequent().toString()+","+String.valueOf(rule.confidence()));
					}
			}
			Set<Entry<String,String>> set = map.entrySet();//entrySet����ֵ�Եļ���
			for (Entry<String, String> entry : set) {
				ps.println(entry.getKey()+"==>"+entry.getValue());
			}
			
			
	}
	
	
	

	//dataframe
	public static void testDataFrame() {
		env();
		SparkConf conf = new SparkConf().setAppName("test1");
		local(conf);
		esConfig(conf);
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		Dataset<String> ds = session.read().textFile("hdfs://10.141.212.26:8020/XMTPCB/mes/sh-SMT/S06/20180422-2/AOI-work.json");
		
		Dataset<String> fjson = ds.map(r->"["+r+"]", Encoders.STRING());
		Dataset<Row> df = session.read().json(fjson);
		df.foreach(r->{
			System.out.println(r.schema());
			System.out.println(r);
		});
	}

	public static void testAddFrame() {
		env();
		SparkConf conf = new SparkConf().setAppName("test3");
		local(conf);
		esConfig(conf);
		SparkSession session = SparkSession.builder().config(conf).getOrCreate();
		List<Row> list = new ArrayList<Row>();
		list.add(RowFactory.create("1001","xxx8"));
		Dataset<Row> df = session.createDataFrame(list, new StructType(new StructField[] {
				new StructField("sn", DataTypes.StringType, false, Metadata.empty()),
				new StructField("name", DataTypes.StringType, false, Metadata.empty())
		}));
		df.map(v->RowFactory.create(v.get(v.fieldIndex("name"))), Encoders.bean(Row.class))
		.foreach(r->System.out.println(r));

		//JavaEsSparkSQL.saveToEs(df, "test/_doc");
	}

	public static void testRDD() {
		env();
		SparkConf conf = new SparkConf().setAppName("test2");
		local(conf);
		esConfig(conf);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<Map<String,Object>> rdd = JavaEsSpark.esRDD(jsc, "test").values();
		rdd.map(r->{System.out.println(r);return r;}).collect();
	}

	private static boolean contaionsAny(Collection<String> c,String...k) {
		for(String v : c) {
			for (String s : k) {
				if(v.matches(".*"+s+".*")) {
					return true;
				}
			}
		}
		return false;
	}
}
//