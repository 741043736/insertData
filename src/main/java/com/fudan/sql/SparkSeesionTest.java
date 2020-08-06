package com.fudan.sql;

import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.AnalysisException;

import com.fudan.basic.InitConf;

public class SparkSeesionTest {
	public static class Person implements Serializable {
		  /**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String name;
		  private int age;

		  public String getName() {
		    return name;
		  }

		  public void setName(String name) {
		    this.name = name;
		  }

		  public int getAge() {
		    return age;
		  }

		  public void setAge(int age) {
		    this.age = age;
		  }
	}
	public static void main(String[] args) throws AnalysisException {
		SparkSession spark = SparkSession.builder().config(InitConf.init("test", true)).getOrCreate();
		Dataset<Row> df = spark.read().json("C:\\Users\\Meng\\Documents\\GitHub\\spark\\examples\\src\\main\\resources\\people.json");
		
		Person person = new Person();
		person.setName("Andy");
		person.setAge(32);
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		System.out.println(personEncoder.toString());
		Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
		javaBeanDS.show();
	}
}


