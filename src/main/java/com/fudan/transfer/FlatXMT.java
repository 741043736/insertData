package com.fudan.transfer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.*;


public class FlatXMT {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\Users\\Meng\\Documents\\eclipse-workspace\\hadoop-common");
		SparkConf conf = new SparkConf().setAppName("task-mix");
		conf.set("es.nodes", "10.141.222.31");
		conf.set("es.port", "9200");
		conf.set("es.scroll.size", "2000");
		// conf.set("es.query",
		// "{\"query\":{\"bool\":{\"must\":[],\"must_not\":[],\"should\":[{\"term\":{\"panelId.keyword\":\"11814201B0C\"}},{\"term\":{\"SN.keyword\":\"MP6HA21D4038281\"}},{\"term\":{\"sn.keyword\":\"MP6HA21D4038281\"}},{\"term\":{\"panelid.keyword\":\"11814201B0C\"}},{\"term\":{\"WO_CODE.keyword\":\"GLCX17101701-CX20"
		// +
		// "\"}}]}},\"from\":0,\"size\":10,\"sort\":[],\"aggs\":{}}");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Map<String, Object>> ft = JavaEsSpark.esRDD(sc, "dataset_test/_doc").values();
		JavaRDD<Map<String, Object>> spi = JavaEsSpark.esRDD(sc, "dataset_spi/_doc").values();
		JavaRDD<Map<String, Object>> aoi = JavaEsSpark.esRDD(sc, "dataset_aoi/_doc").values();
		JavaRDD<Map<String, Object>> rec = JavaEsSpark.esRDD(sc, "dataset_process_rec/_doc").values();
		JavaRDD<Map<String, Object>> fix = JavaEsSpark.esRDD(sc, "dataset_fix/_doc").values();
		JavaRDD<Map<String, Object>> wo = JavaEsSpark.esRDD(sc, "dataset_workorder/_doc").values();

		JavaRDD<Map<String, Object>> aoi_spi = aoi.mapToPair(t -> {
			Object sn = t.get("sn");
			if (sn == null) {
				sn = "null";
			}
			return Tuple2.apply(sn.toString(), t);
		}).filter(t -> !t._1.equals("null")).cogroup(spi.mapToPair(t -> {
			Object sn = t.get("sn");
			if (sn == null) {
				sn = "null";
			}
			return Tuple2.apply(sn.toString(), t);
		}).filter(t -> !t._1.equals("null"))).map(
				new Function<Tuple2<String, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>>, Map<String, Object>>() {
					/**
					 *
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Map<String, Object> call(
							Tuple2<String, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>> l) {
						Map<String, Object> map = new HashMap<>();
						map.put("m_sn", l._1);
						Iterator<Map<String, Object>> aois = l._2._1().iterator();
						Iterator<Map<String, Object>> spis = l._2._2().iterator();
						List<Map<String, Object>> aoiList = new LinkedList<>();
						List<Map<String, Object>> spiList = new LinkedList<>();
						while (aois.hasNext()) {
							Map<String, Object> aoiMap = aois.next();
							aoiList.add(aoiMap);
							if (aoiMap.get("panelId") != null) {
								map.put("m_pid", aoiMap.get("panelId"));
							}
						}
						while (spis.hasNext()) {
							Map<String, Object> spiMap = spis.next();
							spiList.add(spiMap);
							if (map.get("m_pid") == null) {
								if (spiMap.get("panelId") != null) {
									map.put("m_pid", spiMap.get("panelId"));
								}
							}
						}
						map.put("obj_aoi", aoiList);
						map.put("obj_spi", spiList);
						return map;
					}
				});

		JavaRDD<Map<String, Object>> sn_wo = aoi_spi.mapToPair(f -> {
			List aoiList = (List) f.get("obj_aoi");
			List spiList = (List) f.get("obj_spi");
			Object o = "null";
			if (aoiList.size() > 0) {
				o = ((Map) aoiList.get(0)).get("workOrder");
			} else {
				if (spiList.size() > 0) {
					o = ((Map) spiList.get(0)).get("workOrder");
				}
			}
			if (o == null) {
				o = "null";
			}
			return Tuple2.apply(o, f.get("m_sn"));
		}).filter(f -> !f._1.equals("null")).cogroup(wo.mapToPair(w -> {
			Object ww = w.get("WO_CODE");
			if (ww == null) {
				ww = "null";
			}
			return Tuple2.apply(ww, w);
		}).filter(f -> !f._1.equals("null"))).flatMap(
				(FlatMapFunction<Tuple2<Object, Tuple2<Iterable<Object>, Iterable<Map<String, Object>>>>, Map<String, Object>>) t -> {
					List<Map<String, Object>> ret = new ArrayList<>();
					Iterator<Object> sns = t._2._1.iterator();
					Iterator<Map<String, Object>> wos = t._2._2.iterator();
					List<Map<String, Object>> woList = new ArrayList<>();
					while (wos.hasNext()) {
						Map<String, Object> woMap = wos.next();
						woList.add(woMap);
					}
					while (sns.hasNext()) {
						Object sn = sns.next();
						Map<String, Object> map = new HashMap<>();
						map.put("sn", sn);
						map.put("wo", woList);
						ret.add(map);
					}
					return ret.iterator();
				});

		JavaPairRDD<String, Object> sn_sn = aoi_spi.mapToPair(t -> {
			Object pid = t.get("m_pid");
			if (pid == null) {
				pid = "null";
			}
			return Tuple2.apply(pid.toString(), t.get("m_sn"));
		}).filter(t -> !t._1.equals("null")).cogroup(rec.mapToPair(r -> {
			Object pid = r.get("panelid");
			if (pid == null) {
				pid = "null";
			}
			return Tuple2.apply(pid.toString(), r.get("sn"));
		})).flatMap(
				(FlatMapFunction<Tuple2<String, Tuple2<Iterable<Object>, Iterable<Object>>>, Map<String, String>>) t -> {
					List<Map<String, String>> maps = new ArrayList<>();
					Iterator<Object> msns = t._2._1().iterator();
					Iterator<Object> sns = t._2._2().iterator();

					List<String> ms = new ArrayList<>();
					List<String> ss = new ArrayList<>();
					while (msns.hasNext()) {
						ms.add(String.valueOf(msns.next()));
					}
					while (sns.hasNext()) {
						ss.add(String.valueOf(sns.next()));
					}
					Collections.sort(ms);
					Collections.sort(ss);

					for (int i = 0; i < ss.size() && i < ms.size(); i++) {
						Map<String, String> map = new HashMap<>();
						map.put("old", ms.get(i));
						map.put("new", ss.get(i));
						maps.add(map);
					}
					return maps.iterator();
				}).mapToPair(s -> Tuple2.apply(s.get("old"), s.get("new")));

		JavaRDD<Map<String, Object>> aoi_spi_wo = aoi_spi.mapToPair(as -> Tuple2.apply(as.get("m_sn").toString(), as))
				.cogroup(sn_wo.mapToPair(sw -> Tuple2.apply(sw.get("sn").toString(), sw.get("wo"))))
				.map((Function<Tuple2<String, Tuple2<Iterable<Map<String, Object>>, Iterable<Object>>>, Map<String, Object>>) k -> {
					Iterator<Map<String, Object>> orignal = k._2._1().iterator();
					Iterator<Object> wos = k._2._2().iterator();
					Map<String, Object> map = orignal.next();
					if (wos.hasNext()) {
						map.put("wo_obj", wos.next());
					}
					return map;
				});

		JavaRDD<Map<String, Object>> aoi_spi_rec = aoi_spi_wo
				.mapToPair(as -> Tuple2.apply(as.get("m_sn").toString(), as)).cogroup(sn_sn, 64).map(k -> {
					Iterator<Map<String, Object>> orignal = k._2._1().iterator();
					Iterator<Object> newSn = k._2._2().iterator();
					Map<String, Object> map = orignal.next();
					if (newSn.hasNext()) {
						map.put("m_sn", newSn.next());
					}
					return map;
				});

		JavaRDD<Map<String, Object>> aoi_spi_rec_ft = aoi_spi_rec.mapToPair(asr -> Tuple2.apply(asr.get("m_sn"), asr))
				.cogroup(ft.mapToPair(f -> {
					Object sn = f.get("sn");
					if (sn == null) {
						sn = "null";
					}
					return Tuple2.apply(sn, f);
				}).filter(s -> !s._1.equals("null")))
				.map((Function<Tuple2<Object, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>>, Map<String, Object>>) v -> {
					Iterator<Map<String, Object>> orignalList = v._2._1().iterator();
					Map<String, Object> orignal = null;
					if (orignalList.hasNext()) {
						orignal = orignalList.next();
					} else {
						orignal = new HashMap<>();
					}
					List<Map<String, Object>> ftObj = new LinkedList<>();
					Iterator<Map<String, Object>> fts = v._2._2().iterator();
					while (fts.hasNext()) {
						Map<String, Object> ftMap = fts.next();
						ftObj.add(ftMap);
						if (orignal.get("m_sn") == null) {
							if (ftMap.get("sn") != null) {
								orignal.put("m_sn", ftMap.get("sn"));
							}
						}
					}
					orignal.put("obj_ft", ftObj);
					return orignal;
				});

		JavaRDD<Map<String, Object>> spi_aoi_rec_ft_fix = aoi_spi_rec_ft.mapToPair(t -> Tuple2.apply(t.get("m_sn"), t))
				.cogroup(fix.mapToPair(f -> {
					Object sn = f.get("SN");
					if (sn == null) {
						sn = "null";
					}
					return Tuple2.apply(sn, f);
				}).filter(f -> !f._1.equals("null")))
				.map((Function<Tuple2<Object, Tuple2<Iterable<Map<String, Object>>, Iterable<Map<String, Object>>>>, Map<String, Object>>) v -> {
					Iterator<Map<String, Object>> ors = v._2._1().iterator();
					Map<String, Object> orignal = null;
					if (ors.hasNext()) {
						orignal = ors.next();
					} else {
						orignal = new HashMap<>();
					}
					List<Map<String, Object>> fxObj = new LinkedList<>();
					Iterator<Map<String, Object>> fxs = v._2._2().iterator();
					while (fxs.hasNext()) {
						Map<String, Object> ftMap = fxs.next();
						fxObj.add(ftMap);
						if (orignal.get("m_sn") == null) {
							if (ftMap.get("SN") != null) {
								orignal.put("m_sn", ftMap.get("SN"));
							}
						}
					}
					orignal.put("obj_fix", fxObj);
					return orignal;
				});

//		JavaEsSpark.saveToEs(spi_aoi_rec_ft_fix, "d-m2/_doc");
	}
}
