package com.RUSpark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.sql.Date;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/* any necessary Java packages here */

public class RedditHourImpact {
	// private static final Pattern SPACE = Pattern.compile(" ");
	public static String getHour(String unixtime) {
		Long ut = Long.parseLong(unixtime)*1000;
		Date d = new Date(ut);
		int hour = d.getHours();
		System.out.println("hour: "+hour);
		return hour+"";
	}
	public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		System.out.println(1);
		String InputPath = args[0];
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditHourImpact")
			      .getOrCreate();
		  System.out.println(2);
	JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
	System.out.println(3);
	JavaPairRDD<String, Integer> impacts = rows.mapToPair(s -> new Tuple2<>((String)getHour((String)s.getString(1)), (Integer.parseInt((String) s.get(4))+Integer.parseInt((String) s.get(5)) +Integer.parseInt((String) s.get(6)))));
	//JavaPairRDD<Integer, Integer> impacts = rows.mapToPair(s -> new Tuple2<>(new Date(s.getLong(1)*1000).getHours(), (s.getInt(4)+s.getInt(5)+s.getInt(6))));
	System.out.println(4);
	 JavaPairRDD<String, Integer> summedImpacts = impacts.reduceByKey((i1, i2) -> i1 + i2);
	 System.out.println(5);
	 List<Tuple2<String, Integer>> output = summedImpacts.collect();
	 System.out.println(6);
	 	for(int i=0; i<23; i++) {
	    for (Tuple2<?,?> tuple : output) {
	    	if(tuple._1().equals(i+""))
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
	 	}
	    spark.stop();
		/* Implement Here */ 
		
	}

}
