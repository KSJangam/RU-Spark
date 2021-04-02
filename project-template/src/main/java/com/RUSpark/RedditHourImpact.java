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
	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		  SparkSession spark = SparkSession
			      .builder()
			      .appName("RedditHourImpact")
			      .getOrCreate();
	
	JavaRDD<Row> rows = spark.read().csv(InputPath).javaRDD();
	
	JavaPairRDD<Integer, Integer> impacts = rows.mapToPair(s -> new Tuple2<>(new Date(Long.parseLong((String)s.get(0))*1000).getHours(), (Integer.parseInt((String) s.get(4))+Integer.parseInt((String) s.get(5)) +Integer.parseInt((String) s.get(6)))));
	//JavaPairRDD<Integer, Integer> impacts = rows.mapToPair(s -> new Tuple2<>(new Date(s.getLong(1)*1000).getHours(), (s.getInt(4)+s.getInt(5)+s.getInt(6))));
	
	 JavaPairRDD<Integer, Integer> summedImpacts = impacts.reduceByKey((i1, i2) -> i1 + i2);
	
	 List<Tuple2<Integer, Integer>> output = summedImpacts.collect();
	 	for(int i=0; i<23; i++) {
	    for (Tuple2<?,?> tuple : output) {
	    	if(tuple._1().equals(i))
	      System.out.println(tuple._1() + " " + tuple._2());
	    }
	 	}
	    spark.stop();
		/* Implement Here */ 
		
	}

}
