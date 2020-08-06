package com.fudan.transfer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;


public class InsertData {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\Meng\\Documents\\eclipse-workspace\\hadoop-common");
		SparkConf conf = new SparkConf().setAppName("task-p_rec").setMaster("local[*]");
		conf.set("es.nodes", "10.141.222.31");
		conf.set("es.port", "9200");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String rex = "Insert into ([^(]+) \\((.+)\\) values \\((.+)\\);$";
		String timeRex = "([0-9]{2})-([0-9]{1,2})月 -([0-9]{2}) ([0-9]{2}).([0-9]{2}).([0-9]{2}).[0-9]+ (.{2})";
		Pattern linePattern = Pattern.compile(rex);
		Pattern timePattern = Pattern.compile(timeRex);

		Broadcast<Pattern> lp = sc.broadcast(linePattern);
		Broadcast<Pattern> tp = sc.broadcast(timePattern);
//		JavaRDD<String> textRDD = sc.textFile("hdfs://10.141.222.32:8020/data/temp_eh",32);
		JavaPairRDD<LongWritable,Text> s = sc.hadoopFile("hdfs://10.141.222.32:8020/data/temp_eh",org.apache.hadoop.mapred.TextInputFormat.class, LongWritable.class,
                Text.class,32);
		
		JavaRDD<Map<String, Object>> map = s.mapValues(text->new String(text.getBytes(), 0, text.getLength(), "GBK")).values().map(line->{
			Pattern pattern = lp.getValue();
			Matcher matcher = pattern.matcher(line);
			Map<String,Object> source = new HashMap<>();
			if(matcher.find()) {
				//1808012070003555,6,1808012070002555,901708018120970555,100042,to_timestamp('01-8月 -1812.00.00.000000000 上午','DD-MON-RR HH.MI.SS.FF AM'),0,' ',null,null,' ','1',null,null,null,-32.8,to_timestamp('01-8月 -18 12.00.00.000000000 上午','DD-MON-RR HH.MI.SS.FF AM'),null,to_timestamp('01-8月 -18 12.00.00.000000000 上午','DD-MON-RR HH.MI.SS.FF AM'),to_timestamp('01-8月 -18 12.00.00.000000000 上午','DD-MON-RRHH.MI.SS.FF AM'),473,473,'N','Y','N','Y',1,1808012070002555,1808012070002555,'N',0,'1808010070003',null,0,0,0,0,555,0,null,0,null
				String tableName = matcher.group(1);
				source.put("tableNam", tableName);
				String label = matcher.group(2);
				String value = matcher.group(3);
				value = value.replaceAll("to_timestamp\\(([^,)]+)[,][^)]+\\)", "$1");
				String[] labels = label.split(",");
				String[] values = value.split(",");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				for(int i =0;i<labels.length;i++) {
					String original = values[i].replaceAll("'", "");
					//修复时间数据
					if(original.matches(timeRex)) {
						Pattern tc = tp.getValue();
						Matcher m = tc.matcher(original);
						if(m.find()) {
							String day = m.group(1);
							String mon = "0"+m.group(2);
							String year = "20"+m.group(3);
							String hour = m.group(4);
							String min = m.group(5);
							String sec = m.group(6);
							String ap = m.group(7);

							String f = "";
							if("上午".equals(ap)) {
								f = String.format("%s-%s-%s %s:%s:%s", year,mon.substring(mon.length()-2),day,hour,min,sec);
							}else {
								f = String.format("%s-%s-%s %s:%s:%s", year,mon.substring(mon.length()-2),day,12+(Integer.parseInt(hour)+12)%12,min,sec);
							}
							source.put(labels[i]+"_hformat", f);
							try {

								Date date = sdf.parse(f);
								source.put(labels[i]+"_hdate", date);
							} catch (ParseException e) {
								e.printStackTrace();
							}
						}
					}
					source.put(labels[i], original);
				}

			}
			return source;
		}).filter(m->m.size()!=0);
		
//		map.foreach(line->System.out.println(line));
		JavaEsSpark.saveToEs(map, "dataset_tmp/_doc");
		sc.close();
	}
}
