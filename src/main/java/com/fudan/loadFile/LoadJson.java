package com.fudan.loadFile;

import java.util.ArrayList;
import java.util.Iterator;
import com.fudan.basic.Person;

import shaded.parquet.org.codehaus.jackson.map.ObjectMapper;
import org.apache.spark.api.java.function.FlatMapFunction;

class LoadJson implements FlatMapFunction<Iterator<String>,Person> {

	public Iterator<Person> call(Iterator<String> lines) throws Exception {
		ArrayList<Person> people = new ArrayList<Person>();
		ObjectMapper mapper = new ObjectMapper();
		while(lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line,Person.class));
			} catch (Exception e) {
				//
			}
		}
		return (Iterator<Person>) people;
	}
}

class WriteJson implements FlatMapFunction<Iterator<Person>,String>{

	public Iterator<String> call(Iterator<Person> people) throws Exception {
		ArrayList<String> text = new ArrayList<String>();
		ObjectMapper mapper = new ObjectMapper();
		while(people.hasNext())
		{
			Person person = people.next();
			text.add(mapper.writeValueAsString(person));
		}
		return (Iterator<String>) text;
	}
}