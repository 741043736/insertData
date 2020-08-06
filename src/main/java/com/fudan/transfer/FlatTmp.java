package com.fudan.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FlatTmp {
	public static JavaRDD<Map<String, Object>> tmp(String[] tags, String query, JavaSparkContext sc) {
		Broadcast<String[]> btags = sc.broadcast(tags);
		JavaRDD<Map<String, Object>> query_result = JavaEsSpark.esRDD(sc, "dataset_tmp/_doc", query).values();
		return query_result.map(line -> {
			List<String> keys = Arrays.asList(btags.getValue());
			line.entrySet().removeIf(entry -> !keys.contains(entry.getKey()));
			return line;
		});
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\Meng\\Documents\\Java\\hadoop-common");
//		System.setProperty("file.encoding", "UTF-8");
//		SparkConf conf = new SparkConf().setAppName("task-mix");
		SparkConf conf = new SparkConf().setAppName("task-mix").setMaster("local[*]");
		conf.set("es.nodes", "10.141.222.31");
		conf.set("es.port", "9200");
		conf.set("es.scroll.size", "2000");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String[] product_basic_tags = new String[]{
				"ITEM_NUM_ID", "ITEM_NAME", "ITEMID", "ITEM_GRADE", "PTY_NUM_1", "PTY_NUM_2", "PTY_NUM_3", "TENANT_NUM_ID"
				, "PURCHASE_TYPE_NUM_ID", "BRAND_ID", "DATA_SIGN"
		};
		String[] tml_dtl_tags = new String[]{
				"TML_NUM_ID", "ITEM_NUM_ID", "TENANT_NUM_ID", "DEDUCT_AMOUNT", "ITEM_NAME", "SALES_TYPE_NUM_ID", "F_AMOUNT", "QTY"
				, "SIM_ITEM_NAME", "DATA_SIGN"
		};
		String[] tml_hdr_tags = new String[]{
				"DATA_SIGN", "VIP_TYPE", "TYPE_NUM_ID", "TRAN_TYPE_NUM_ID", "PRV_NUM_ID", "TML_NUM_ID", "TENANT_NUM_ID", "SO_FROM_TYPE"
				, "SUB_UNIT_NUM_ID", "USR_NUM_ID", "VIP_NO", "CHANNEL_NUM_ID", "ORDER_DATE"
		};
		String[] cash_dtl_tags = new String[]{
				"RESERVED_NO", "CASH_NUM_ID", "PAY_AMOUNT", "PAY_TYPE_ID", "CASH_TYPE_NUM_ID", "DATA_SIGN"
		};
		String[] pay_type_tags = new String[]{
				"PAY_TYPE_ID", "PAY_TYPE_NAME", "IS_REAL", "DATA_SIGN"
		};
		String[] sub_unit_tags = new String[]{
				"SUB_UNIT_NUM_ID", "SUB_UNIT_NAME", "SUB_UNIT_TYPE", "DATA_SIGN"
		};

		String product_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_PRODUCT_BASIC\"}}]}}}";
		String tml_dtl_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_TML_DTL\"}}]}}}";
		String tml_hdr_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_TML_HDR\"}}]}}}";
		String sub_unit_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_SUB_UNIT\"}}]}}}";
		String cash_dtl_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_CASH_DTL\"}}]}}}";
		String pay_type_query = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_PAY_TYPE\"}}]}}}";

//		String tml_dtl_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_TML_DTL\"}},{\"term\":{\"TML_NUM_ID.keyword\":\"901388171521182555\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[],\"aggs\":{}}";
//		String tml_hdr_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_TML_HDR\"}},{\"term\":{\"TML_NUM_ID.keyword\":\"901388171521182555\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[],\"aggs\":{}}";
//		String cash_dtl_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_CASH_DTL\"}},{\"term\":{\"RESERVED_NO.keyword\":\"901388171521182555\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[],\"aggs\":{}}";
//		String product_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_PRODUCT_BASIC\"}}]}}}";
//		String sub_unit_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_SUB_UNIT\"}}]}}}";
//		String pay_type_query="{\"query\":{\"bool\":{\"must\":[{\"term\":{\"tableNam.keyword\":\"TEMP_PAY_TYPE\"}}]}}}";
//		
		JavaRDD<Map<String, Object>> product = tmp(product_basic_tags, product_query, sc);
		JavaRDD<Map<String, Object>> tml_dtl = tmp(tml_dtl_tags, tml_dtl_query, sc);
		JavaRDD<Map<String, Object>> tml_hdr = tmp(tml_hdr_tags, tml_hdr_query, sc);
		JavaRDD<Map<String, Object>> sub_unit = tmp(sub_unit_tags, sub_unit_query, sc);
		JavaRDD<Map<String, Object>> cash_dtl = tmp(cash_dtl_tags, cash_dtl_query, sc);
		JavaRDD<Map<String, Object>> pay_type = tmp(pay_type_tags, pay_type_query, sc);

		JavaPairRDD<String, Map<String, Object>> productPairRDD = product.mapToPair(t -> {
			Object id = t.get("ITEM_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			String data_sign = t._2.get("DATA_SIGN").toString();
			return data_sign.equals("0") && !t._1.equals("null");
		});
		JavaPairRDD<String, Map<String, Object>> tmlDtlPairRDD = tml_dtl.mapToPair(t -> {
			Object id = t.get("ITEM_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			String data_sign = t._2.get("DATA_SIGN").toString();
			return data_sign.equals("0") && !t._1.equals("null");
		});
		JavaRDD<Map<String, Object>> tml_dtl_product = productPairRDD.join(tmlDtlPairRDD).map(
				new Function<Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>>, Map<String, Object>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Object> call(Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>> l) {
						Map<String, Object> tml_dtl = l._2._1();
						Map<String, Object> product = l._2._2();
						tml_dtl.putAll(product);
						return tml_dtl;
					}
				});
		JavaPairRDD<String, Map<String, Object>> tmlHdrPairRDD = tml_hdr.mapToPair(t -> {
			Object id = t.get("SUB_UNIT_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			String data_sign = t._2.get("DATA_SIGN").toString();
			if (data_sign.equals("0") && !t._1.equals("null")) {
				return true;
			} else {
				return false;
			}
		});
		//
		JavaPairRDD<String, Map<String, Object>> subUnitPairRDD = sub_unit.mapToPair(t -> {
			Object id = t.get("SUB_UNIT_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			int data_sign = Integer.parseInt((String) t._2.get("DATA_SIGN"));
			if (data_sign == 0 && !t._1.equals("null")) {
				return true;
			} else {
				return false;
			}
		});
		//
		JavaRDD<Map<String, Object>> tml_hdr_sub_unit = tmlHdrPairRDD.join(subUnitPairRDD).map(
				new Function<Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>>, Map<String, Object>>() {
					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Object> call(Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>> l)
							throws Exception {
						Map<String, Object> tml_hdr = l._2._1();
						Map<String, Object> sub_unit = l._2._2();
						tml_hdr.putAll(sub_unit);

						return tml_hdr;
					}
				});
		//
		JavaPairRDD<String, Map<String, Object>> CashDtlPairRDD = cash_dtl.mapToPair(t -> {
			Object id = t.get("PAY_TYPE_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			String data_sign = t._2.get("DATA_SIGN").toString();
			if (data_sign.equals("0") && !t._1.equals("null")) {
				return true;
			} else {
				return false;
			}
		});
		JavaPairRDD<String, Map<String, Object>> PayTypePairRDD = pay_type.mapToPair(t -> {
			Object id = t.get("PAY_TYPE_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).filter(t -> {
			String data_sign = t._2.get("DATA_SIGN").toString();
			if (data_sign.equals("0") && !t._1.equals("null")) {
				return true;
			} else {
				return false;
			}
		});
		JavaRDD<Map<String, Object>> cash_dtl_pay_type = CashDtlPairRDD.join(PayTypePairRDD).map(
				new Function<Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>>, Map<String, Object>>() {

					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Object> call(Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>> l)
							throws Exception {
						Map<String, Object> cash_dtl = l._2._1();
						Map<String, Object> pay_type = l._2._2();
						cash_dtl.put("pay_amount", Double.valueOf(cash_dtl.get("PAY_AMOUNT").toString()));
						cash_dtl.putAll(pay_type);
						return cash_dtl;
					}
				});

		JavaRDD<Map<String, Object>> tml_hdr_cash = tml_hdr_sub_unit.mapToPair(t -> {
			Object id = t.get("TML_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).cogroup(cash_dtl_pay_type.mapToPair(t -> {
			Object id = t.get("RESERVED_NO");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		})).map(new Function<Tuple2<String, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>>, Map<String, Object>>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Object> call(
					Tuple2<String, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>> l) {
				Map<String, Object> map = new HashMap<>();
				Iterator<Map<String, Object>> tml_hdr = l._2._1().iterator();
				Iterator<Map<String, Object>> cash_dtl = l._2._2().iterator();
				List<Map<String, Object>> cashList = new LinkedList<>();
				if (tml_hdr.hasNext()) {
					map = tml_hdr.next();
				}
				while (cash_dtl.hasNext()) {
					cashList.add(cash_dtl.next());
				}
				map.put("cashList", cashList);
				return map;
			}
		});

		JavaRDD<Map<String, Object>> tml = tml_hdr_cash.mapToPair(t -> {
			Object id = t.get("TML_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		}).join(tml_dtl_product.mapToPair(t -> {
			Object id = t.get("TML_NUM_ID");
			if (id == null) {
				id = "null";
			}
			return Tuple2.apply(id.toString(), t);
		})).map(new Function<Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>>, Map<String, Object>>() {

			/**
			 *
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Object> call(Tuple2<String, Tuple2<Map<String, Object>, Map<String, Object>>> l) {
				Map<String, Object> tml_hdr = l._2._1();
				Map<String, Object> tml_dtl = l._2._2();
				putTimeMap(tml_hdr);
				tml_hdr.putAll(tml_dtl);
				tml_hdr.put("deduct_amount", Double.valueOf(tml_hdr.get("DEDUCT_AMOUNT").toString()));
				tml_hdr.put("f_amount", Double.valueOf(tml_hdr.get("F_AMOUNT").toString()));
				tml_hdr.put("qty", Double.valueOf(tml_hdr.get("QTY").toString()));
				return tml_hdr;
			}
		});
//		tml.foreach(line->System.out.println(line));
		JavaEsSpark.saveToEs(tml, "test/_doc");
	}

	public static Map<String, Object> putTimeMap(Map<String, Object> map) {
		String time = map.get("ORDER_DATE").toString();
		String timeRex = "([0-9]{2})-([0-9]{1,2}).{1}[ ]?-([0-9]{2}) ([0-9]{2}).([0-9]{2}).([0-9]{2}).[0-9]+ (.{2})";
		Pattern timePattern = Pattern.compile(timeRex);
		Matcher m = timePattern.matcher(time);
		String f = "";
		if (m.find()) {
			String day = m.group(1);
			String mon = m.group(2);
			String year = m.group(3);
			String hour = m.group(4);
			String min = m.group(5);
			String sec = m.group(6);
			String ap = m.group(7).equals("上午") ? "AM" : "PM";
			f = String.format("%s-%s-%s %s:%s:%s %s", year, mon, day, hour, min, sec, ap);
		}
		if (!"".equals(f)) {
			SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd hh:mm:ss a");
			SimpleDateFormat nsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				Date date = sdf.parse(f);
				map.put("order_fdate", nsdf.format(date));
				map.put("order_pdate", date);
				Calendar cal = Calendar.getInstance();
				cal.setTime(date);

				map.put("f_year", cal.get(Calendar.YEAR));
				map.put("f_month", cal.get(Calendar.MONTH) + 1);
				map.put("f_day", cal.get(Calendar.DAY_OF_MONTH));
				map.put("f_week", cal.get(Calendar.DAY_OF_WEEK));
				map.put("f_hour", cal.get(Calendar.HOUR_OF_DAY));

			} catch (ParseException e) {
				map.put("_exception", e.getMessage());
			}
		}
		return map;
	}
}
